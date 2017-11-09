;; The Climate Corporation licenses this file to you under under the Apache
;; License, Version 2.0 (the "License"); you may not use this file except in
;; compliance with the License.  You may obtain a copy of the License at
;;
;;   http://www.apache.org/licenses/LICENSE-2.0
;;
;; See the NOTICE file distributed with this work for additional information
;; regarding copyright ownership.  Unless required by applicable law or agreed
;; to in writing, software distributed under the License is distributed on an
;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
;; or implied.  See the License for the specific language governing permissions
;; and limitations under the License.
(ns com.climate.squeedo.sqs
  (:require
    [clojure.tools.logging :as log]
    [cheshire.core :as json])
  (:import
    (com.amazonaws.services.sqs.model
      Message
      MessageAttributeValue
      QueueDoesNotExistException
      ReceiveMessageRequest
      SendMessageRequest)
    (com.amazonaws.services.sqs
      AmazonSQSClient
      AmazonSQSClientBuilder)))

(def ^:dynamic auto-retry-seconds
  "How long to wait before retrying a non-responding compute request."
  ;; Wait 10 minutes.
  600)

(def ^:dynamic poll-timeout-seconds
  "How long to wait for a response when polling for new messages on the queue."
  10)

(def ^:dynamic maximum-retries
  "Maximum number of retries before sending a message to the dead letter
  queue."
  3)

(def ^:const invalid-queue-message
  (str "Queue names can only include alphanumeric characters "
       "hyphens, or underscores. A FIFO queue must have the"
       ".fifo suffix. Queue name should be less than "
       "80 characters."))

(defn valid-queue-name?
  "Returns true if an SQS queue name is valid, false otherwise"
  [queue-name]
  (and (re-matches #"[A-Za-z0-9_-]+(\.fifo)?" queue-name)
       (< (count queue-name) 80)))

(defn validate-queue-name!
  "Validates input for SQS queue names.

  Returns nil, or throws an IllegalArgumentException on invalid queue name."
  [queue-name]
  (when-not (valid-queue-name? queue-name)
    (throw (IllegalArgumentException. invalid-queue-message))))

(defn- redrive-policy
  "Encode the RedrivePolicy for a dead letter queue.
  http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html"
  [^AmazonSQSClient client ^String dead-letter-url]
  (let [queue-arn (-> client
                      (.getQueueAttributes dead-letter-url ["QueueArn"])
                      (.getAttributes)
                      (get "QueueArn"))]
    {"RedrivePolicy"
     ; yes, this is really necessary.
     (json/generate-string {"maxReceiveCount"     maximum-retries
                            "deadLetterTargetArn" queue-arn})}))

(defn- get-queue
  "Retrieve the queue URL (Amazon Resource Name) from SQS."
  [^AmazonSQSClient client ^String queue-name]
  (try
    (.getQueueUrl (.getQueueUrl client queue-name))
    (catch QueueDoesNotExistException _
      (log/debugf "SQS queue %s does not exist" queue-name)
      nil)))

(defn- set-attributes
  "Set queue attributes for the speecified queue URL.

  Optionally takes a dead letter queue URL (Amazon Resource Name),
  a queue that already exists, that gets associated with the returned queue."
  ([client queue-url queue-attrs]
   (set-attributes client queue-url queue-attrs nil))
  ([^AmazonSQSClient client ^String queue-url queue-attrs dead-letter-url]
   (let [default-attrs {"DelaySeconds"                  0       ; delay after enqueue
                        "MessageRetentionPeriod"        1209600 ; max, 14 days
                        "ReceiveMessageWaitTimeSeconds" poll-timeout-seconds
                        "VisibilityTimeout"             auto-retry-seconds}
         redrive-attrs (when dead-letter-url
                         (redrive-policy client dead-letter-url))]
     (->> (merge default-attrs
                 queue-attrs
                 redrive-attrs)
          (map (fn [[k v]] [(str k) (str v)]))
          (into {})
          (.setQueueAttributes client queue-url)))))

(defn- get-or-create-queue
  "Get or create an SQS queue with the given name and specified queue attributes.

  Optionally takes a dead letter queue URL (Amazon Resource Name),
  a queue that already exists, that gets associated with the returned queue."
  [^AmazonSQSClient client ^String queue-name queue-attrs dead-letter-url]
  (if-let [queue-url (get-queue client queue-name)]
    (do (log/warnf "Found existing queue %s, queue attributes and redrive policy will not be modified." queue-name)
        queue-url)
    (let [queue-url (.getQueueUrl (.createQueue client queue-name))]
      (log/debugf "Created SQS queue %s" queue-name)
      (set-attributes client queue-url queue-attrs dead-letter-url)
      queue-url)))

(defn mk-connection
  "Create an SQS connection to a queue identified by string queue-name. This
  should be done once at startup time.

  Optionally takes the name of a dead-letter queue, where messages that failed
  too many times will go.
  http://aws.typepad.com/aws/2014/01/amazon-sqs-new-dead-letter-queue.html

  Also, it optionally takes attribute maps for the main queue and the dead letter
  queue. These must be maps from string to string, with keys matching standard
  SQS attribute names.

  Note: if the queue already exists, the queue attributes and redrive policy
  will not be modified. Use `set-queue-attributes` to set attributes on an
  existing queue."
  [queue-name & {:keys [dead-letter
                        client
                        queue-attributes
                        dead-letter-queue-attributes]}]
  (validate-queue-name! queue-name)
  (let [client (or client (AmazonSQSClientBuilder/defaultClient))
        dead-letter-connection (when dead-letter
                                 (mk-connection dead-letter
                                                :client client
                                                :queue-attributes dead-letter-queue-attributes))
        queue-url (get-or-create-queue
                    client queue-name queue-attributes (:queue-url dead-letter-connection))]
    (log/infof "Using SQS queue %s at %s" queue-name queue-url)
    (merge
      {:client     client
       :queue-name queue-name
       :queue-url  queue-url}
      (when dead-letter
        {:dead-letter (dissoc dead-letter-connection :client)}))))

(defn set-queue-attributes
  "Update queue attributes of an existing queue & dead letter queue.
  The attribute maps must be from string to string, with keys matching standard
  SQS attribute names.

  Input:
    queue-connection - A connection to a queue made with mk-connection

    Optional arguments:
    :queue-attributes - attribute map for the main queue
    :dead-letter-queue-attributes - attribute map for the dead letter queue"
  [{:keys [client queue-url dead-letter]} & {:keys [queue-attributes
                                                    dead-letter-queue-attributes]}]
  (when (and dead-letter dead-letter-queue-attributes)
    (set-attributes client (:queue-url dead-letter) dead-letter-queue-attributes))
  (set-attributes client queue-url queue-attributes (:queue-url dead-letter)))

(defn- build-msg-attributes
  "A helper function to turn a Clojure keyword map into a Map<String,MessageAttributeValue>"
  [message-attribute-map]
  (when message-attribute-map
    (into {}
          (map (fn [[k v]] [(name k) (-> (MessageAttributeValue.)
                                         (.withStringValue (str v))
                                         (.withDataType "String"))])
               message-attribute-map))))

(defn- send-message
  "Sends a new message with the given string body to the queue specified
  by the string URL.  Returns a map with :id and :body-md5 slots."
  [^AmazonSQSClient client queue-url message & [message-attributes]]
  (let [message-attribute-value-map (build-msg-attributes message-attributes)
        send-message-request (cond-> (SendMessageRequest. queue-url message)
                               message-attribute-value-map (.withMessageAttributes message-attribute-value-map))
        resp (.sendMessage client send-message-request)]
    {:id (.getMessageId resp)
     :body-md5 (.getMD5OfMessageBody resp)}))

(defn enqueue
  "Enqueues a message to a queue.

  Input:
    queue-connection - A connection to a queue made with mk-connection
    message - The message to be placed on the queue

    Optional arguments:
    :message-attributes - A map of SQS Attributes to apply to this message.
    :serialization-fn - A function that serializes the message you want to enqueue to a string
    By default, pr-str will be used"
  [queue-connection message & opts]
  (let [{:keys [client queue-name queue-url]} queue-connection
        {:keys [message-attributes
                serialization-fn]
         :or {serialization-fn pr-str}} opts]
    (log/debugf "Enqueueing %s to %s" message queue-name)
    (send-message client queue-url (serialization-fn message) message-attributes)))

(defn- clojurify-message-attributes
  [^Message msg]
  (let [javafied-message-attributes (.getMessageAttributes msg)]
    (->> javafied-message-attributes
         (map (fn [[k ^MessageAttributeValue mav]]
                [(keyword k) (case (.getDataType mav)
                               "String" (.getStringValue mav)
                               "Number" (.getStringValue mav)
                               "Binary" (.getBinaryValue mav))]))
         (into {}))))

(defn- message-map
  [queue-url ^Message msg]
  {:attributes (.getAttributes msg)
   :message-attributes (clojurify-message-attributes msg)
   :body (.getBody msg)
   :body-md5 (.getMD5OfBody msg)
   :id (.getMessageId msg)
   :receipt-handle (.getReceiptHandle msg)
   :source-queue queue-url})

(defn- receive
  "Receives one or more messages from the queue specified by the given URL.
  Input:
    client - AmazonSQSClient
    queue-url - url to the SQS resource

    Optional keyword arguments:
    :limit - between 1 (default) and 10, the maximum number of messages to receive
    :visibility - seconds the received messages should not be delivered to other
              receivers; defaults to the queue's visibility attribute
    :attributes - a collection of string names of :attributes to include in
              received messages; e.g. #{\"All\"} will include all attributes,
              #{\"SentTimestamp\"} will include only the SentTimestamp attribute, etc.
              Defaults to the empty set (i.e. no attributes will be included in
              received messages).
              See the SQS documentation for all support message attributes.
    :wait-time-seconds - enables long poll support. time is in seconds, bewteen
              0 (default - no long polling) and 20.
              Allows Amazon SQS service to wait until a message is available
              in the queue before sending a response.
              See the SQS documentation at (http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html)

  Returns a seq of maps with these slots:
    :attributes - message attributes
    :body - the string body of the message
    :body-md5 - the MD5 checksum of :body
    :id - the message's ID
    :receipt-handle - the ID used to delete the message from the queue after
              it has been fully processed.
    :source-queue - the URL of the queue from which the message was received"
  [^AmazonSQSClient client queue-url & {:keys [limit
                                               visibility
                                               wait-time-seconds
                                               ^java.util.Collection attributes
                                               ^java.util.Collection message-attribute-names]}]
  (let [req (-> (ReceiveMessageRequest. queue-url)
                (.withMaxNumberOfMessages (-> limit (min 10) (max 1) int Integer/valueOf))
                (.withAttributeNames attributes)
                (.withMessageAttributeNames message-attribute-names))
        req (if wait-time-seconds (.withWaitTimeSeconds req (Integer/valueOf (int wait-time-seconds))) req)
        req (if visibility (.withVisibilityTimeout req (Integer/valueOf (int visibility))) req)]
    (->> (.receiveMessage client req)
         (.getMessages)
         (map (partial message-map queue-url)))))

;; Raw message is being returned, so it's up to the user to determine the proper reader for their needs.
;; NaN,Infinity,-Infinity http://dev.clojure.org/jira/browse/CLJ-1074
(defn dequeue
  "Read messages from a queue. If there is nothing to read before
  poll-timeout-seconds, return [].

  This does *not* remove the messages from the queue! For that, see ack.

  In case of exception, logs the exception and returns []."
  [{:keys [client queue-name queue-url]}
   & {:keys [limit attributes message-attributes]
      :or {limit 10
           attributes #{"All"}
           message-attributes #{"All"}}}]
  ;; Log at debug so we don't spam about polling.
  (log/debugf "Attempting dequeue from %s" queue-name)
  ;; will have a single queue/connection
  (try
    (->> (receive client
                  queue-url
                  :wait-time-seconds poll-timeout-seconds
                  :limit limit
                  :attributes attributes
                  :message-attribute-names message-attributes)
         (map (fn [m]
                (log/debugf "Dequeued from queue %s message %s" queue-name m)
                (assoc m :queue-name queue-name))))
    (catch Exception e
      (log/error e "Encountered exception dequeueing.")
      [])))

(defn ack
  "Remove the message from the queue.

  Input:
    connection - as created by mk-connection
    message - the message to ack"
  [{:keys [^AmazonSQSClient client
           ^String queue-url]}
   {:keys [^String receipt-handle]}]
  (log/debugf "Acking message %s" receipt-handle)
  (.deleteMessage client queue-url receipt-handle))

(defn nack
  "Put the message back on the queue.

  Input:
    connection - as created by mk-connection
    message - the message to nack
    nack-visibility-seconds (optional) - How long to wait before retrying a failed compute request.
                                         Defaults to 0."
  ([connection message]
   (nack connection message 0))
  ([{:keys [^AmazonSQSClient client
            ^String queue-url]}
    {:keys [^String receipt-handle]}
    nack-visibility-seconds]
   (log/debugf "Nacking message %s" receipt-handle)
   (.changeMessageVisibility client queue-url receipt-handle (int nack-visibility-seconds))))
