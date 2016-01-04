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
    [cemerick.bandalore :as sqs]
    [cheshire.core :as json]
    [clojure.tools.logging :as log])
  (:import [com.amazonaws.services.sqs.model MessageAttributeValue SendMessageRequest ReceiveMessageRequest Message]
           [com.amazonaws.services.sqs AmazonSQSClient]))

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
       "hyphens, or underscores. Queue name should be less than "
       "80 characters."))

(defn valid-queue-name?
  "Returns true if an SQS queue name is valid, false otherwise"
  [queue-name]
  (not (or (empty? queue-name)
           (re-find #"[^A-Za-z0-9_-]" queue-name)
           (>= (count queue-name) 80))))

(defn validate-queue-name!
  "Validates input for SQS queue names.

  Returns nil, or throws an IllegalArgumentException on invalid queue name."
  [queue-name]
  (when-not (valid-queue-name? queue-name)
    (throw (IllegalArgumentException. invalid-queue-message))))

(defn- redrive-policy
  "Encode the RedrivePolicy for a dead letter queue.
  http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html"
  [client dead-letter-queue-url]
  {"RedrivePolicy"
    (json/generate-string                                   ; yes, this is really necessary.
      {"maxReceiveCount"     maximum-retries
       "deadLetterTargetArn" ((sqs/queue-attrs client dead-letter-queue-url)
                              "QueueArn")})})

(defn- get-queue
  "Retrieve the queue URL (Amazon Resource Name) from SQS.

  Optionally takes a dead letter queue URL (Amazon Resource Name),
  a queue that already exists, that gets associated with the returned queue."
    [client queue-name queue-atts & [dead-letter-url]]
  (let [default-attrs {"DelaySeconds"                  0    ; delay after enqueue
                       "MessageRetentionPeriod"        1209600 ; max, 14 days
                       "ReceiveMessageWaitTimeSeconds" poll-timeout-seconds
                       "VisibilityTimeout"             auto-retry-seconds}
        ; Tip: don't try sending attrs as create-queue's 'options' param: they aren't the same
        q (sqs/create-queue client queue-name)]
    (sqs/queue-attrs client q
                     (merge default-attrs
                            queue-atts
                            (when dead-letter-url (redrive-policy client dead-letter-url))))
    q))

(defn mk-connection
  "Create an SQS connection to a queue identified by string queue-name. This
  should be done once at startup time.

  Optionally takes the name of a dead-letter queue, where messages that failed
  too many times will go.
  http://aws.typepad.com/aws/2014/01/amazon-sqs-new-dead-letter-queue.html

  Also, it optionally takes attribute maps for the main queue and the dead letter
  queue. These must be maps from string to string, with keys matching standard
  SQS attribute names
  "
  [queue-name & {:keys [dead-letter
                        client
                        queue-attributes
                        dead-letter-queue-attributes]}]
  (validate-queue-name! queue-name)
  (when dead-letter
    (validate-queue-name! dead-letter))
  (let [client (or client (sqs/create-client))
        dead-letter-connection (when dead-letter
                                 (mk-connection dead-letter
                                                :client client
                                                :queue-attributes dead-letter-queue-attributes))
        queue-url (if dead-letter
                    (get-queue client queue-name queue-attributes (:queue-url dead-letter-connection))
                    (get-queue client queue-name queue-attributes))]
    (log/infof "Using SQS queue %s at %s" queue-name queue-url)
    (merge
     {:client     client
      :queue-name queue-name
      :queue-url  queue-url}
     (when dead-letter
       {:dead-letter (dissoc dead-letter-connection :client)}))))

(defn- build-msg-attributes
  "A simple helper function to turn a Clojure keyword map into a Map<String,MessageAttributeValue>"
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
  "Enqueues a message to a queue
  Arguments:
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

(defn- clojurify-message-attributes [^Message msg]
  (let [javafied-message-attributes (.getMessageAttributes msg)]
    (->> javafied-message-attributes
         (map (fn [[k ^MessageAttributeValue mav]] [(keyword k) (.getStringValue mav)]))
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
   Optionally accepts keyword arguments:

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
      .getMessages
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
    (->>
      (receive client
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
  "Remove the message from the queue."
  [{:keys [client]} message]
  (log/debugf "Acking message %s" message)
  (sqs/delete client message))

(defn nack
  "Put the message back on the queue.
   input:
     connection - as created by mk-connection
     message - the message to nack
     nack-visibility-seconds (optional) - How long to wait before retrying a failed compute request.
                                          Defaults to 0."
  [{:keys [client queue-url]} message & [nack-visibility-seconds]]
   (log/debugf "Nacking message %s" message)
   (sqs/change-message-visibility client queue-url message
                                  (int (or nack-visibility-seconds 0))))
