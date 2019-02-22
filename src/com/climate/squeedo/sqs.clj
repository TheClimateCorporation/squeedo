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
      CreateQueueRequest
      Message
      MessageAttributeValue
      QueueDoesNotExistException
      ReceiveMessageRequest
      SendMessageRequest)
    (com.amazonaws.services.sqs
      AmazonSQSClient
      AmazonSQSClientBuilder)))


;;;;;;;;
;; queue

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
       "hyphens, or underscores. A FIFO queue must have the "
       ".fifo suffix. Queue name should be no more than "
       "80 characters."))

(defn parse-queue-name
  [queue-name]
  (let [[_ queue-name fifo?] (re-matches #"([A-Za-z0-9_-]+)(\.fifo)?" queue-name)]
    (when queue-name
      {:name queue-name
       :fifo? (boolean fifo?)})))

(defn valid-queue-name?
  "Returns true if an SQS queue name is valid, false otherwise"
  [queue-name]
  (and (some? (parse-queue-name queue-name))
       (<= (count queue-name) 80)))

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

(defn- set-attributes
  "Set queue attributes for the speecified queue URL.

  Optionally takes a dead letter queue URL (Amazon Resource Name),
  a queue that already exists, that gets associated with the returned queue."
  [^AmazonSQSClient client ^String queue-url queue-attrs dead-letter-url]
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
         (.setQueueAttributes client queue-url))))

(defn- get-or-create-queue
  "Get or create SQS queue for the given queue name. Returns the queue URL
  (Amazon Resource Name)."
  [^AmazonSQSClient client ^String queue-name]
  (try
    (.getQueueUrl (.getQueueUrl client queue-name))
    (catch QueueDoesNotExistException _
      (log/debugf "SQS queue %s does not exist. Creating queue." queue-name)
      (let [fifo? (:fifo? (parse-queue-name queue-name))
            create-queue-request (cond-> (CreateQueueRequest.)
                                   true  (.withQueueName queue-name)
                                   fifo? (.withAttributes {"FifoQueue" "true"}))]
        (.getQueueUrl (.createQueue client create-queue-request))))))

(defn- default-client [] (AmazonSQSClientBuilder/defaultClient))

(defn configure-queue
  "Update an SQS queue with the provided configuration. This should be done
  once at startup time, when you need to (re)configure the queue.

  The queue attribute maps must be from string to string, with keys matching
  standard SQS attribute names.

  Optionally takes the name of a dead-letter queue, where messages that failed
  too many times will go.
  http://aws.typepad.com/aws/2014/01/amazon-sqs-new-dead-letter-queue.html

  The queue will be created if it does not exist. A dead letter queue will
  be created if specified and it does not exist.

  Input:
    queue-name - The name of the queue to be configured.

    Optional arguments:
    :queue-attributes - attribute map for the main queue
    :dead-letter - name of dead letter queue
    :dead-letter-queue-attributes - attribute map for the dead letter queue"
  [queue-name & {:keys [client
                        queue-attributes
                        dead-letter
                        dead-letter-queue-attributes]}]
  (validate-queue-name! queue-name)
  (let [client (or client (default-client))
        dead-letter-url (when dead-letter
                          (configure-queue dead-letter
                                           :client client
                                           :queue-attributes dead-letter-queue-attributes))
        queue-url (get-or-create-queue client queue-name)]
    (set-attributes client queue-url queue-attributes dead-letter-url)
    queue-url))

(defn- dead-letter-deprecation-warning
  [options]
  (when (:dead-letter options)
    (println
      (str "WARNING - :dead-letter option for com.climate.squeedo.sqs/mk-connection has"
           " been removed. Please use com.climate.squeedo.sqs/configure to configure an"
           " SQS dead letter queue."))))

(defn mk-connection
  "Make an SQS connection to a queue identified by string queue-name. This
  should be done once at startup time.

  Will throw QueueDoesNotExistException if the queue does not exist.
  `configure-queue` can be used to create and configure the queue."
  [^String queue-name & {:keys [client] :as options}]
  (dead-letter-deprecation-warning options)
  (validate-queue-name! queue-name)
  (let [^AmazonSQSClient client (or client (default-client))
        queue-url (.getQueueUrl (.getQueueUrl client queue-name))]
    (log/infof "Using SQS queue %s at %s" queue-name queue-url)
    {:client client
     :queue-name queue-name
     :queue-url queue-url}))


;;;;;;;;;;
;; message

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
  [^AmazonSQSClient client queue-url message opts]
  (let [{:keys [message-attributes message-group-id message-deduplication-id]} opts
        message-attribute-value-map (build-msg-attributes message-attributes)
        send-message-request (cond-> (SendMessageRequest. queue-url message)
                               message-attribute-value-map (.withMessageAttributes message-attribute-value-map)
                               message-group-id (.withMessageGroupId message-group-id)
                               message-deduplication-id (.withMessageDeduplicationId message-deduplication-id))
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
    :message-group-id - This parameter applies to FIFO queues only
    :message-deduplication-id - This parameter applies to FIFO queues only

    By default, pr-str will be used"
  [queue-connection message & {:as opts}]
  (let [{:keys [client queue-name queue-url]} queue-connection
        {:keys [serialization-fn]
         :or {serialization-fn pr-str}} opts]
    (log/debugf "Enqueueing %s to %s" message queue-name)
    (send-message client queue-url (serialization-fn message) (dissoc opts :serialization-fn))))

(defn- clojurify-message-attributes
  [^Message msg]
  (let [javafied-message-attributes (.getMessageAttributes msg)]
    (->> javafied-message-attributes
         (map (fn [[k ^MessageAttributeValue mav]]
                [(keyword k) (case (.getDataType mav)
                               "String" (.getStringValue mav)
                               "Number" (.getStringValue mav)
                               "Binary" (.getBinaryValue mav)
                               (.getStringValue mav))]))
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

(defn dequeue*
  "Read messages from a queue. If there is nothing to read before poll-timeout-seconds, return [].

  This does *not* remove the messages from the queue! For that, see ack.

  Does not catch Exceptions that might be thrown while attempting to receive messages from the queue."
  [{:keys [client queue-name queue-url]}
   & {:keys [limit attributes message-attributes]
      :or   {limit              10
             attributes         #{"All"}
             message-attributes #{"All"}}}]
  ;; Log at debug so we don't spam about polling.
  (log/debugf "Attempting dequeue from %s" queue-name)
  ;; will have a single queue/connection
  (->> (receive client
                queue-url
                :wait-time-seconds poll-timeout-seconds
                :limit limit
                :attributes attributes
                :message-attribute-names message-attributes)
       (map (fn [m]
              (log/debugf "Dequeued from queue %s message %s" queue-name m)
              (assoc m :queue-name queue-name)))))

;; Raw message is being returned, so it's up to the user to determine the proper reader for their needs.
;; NaN,Infinity,-Infinity http://dev.clojure.org/jira/browse/CLJ-1074
(defn dequeue
  "Read messages from a queue. If there is nothing to read before
  poll-timeout-seconds, return [].

  This does *not* remove the messages from the queue! For that, see ack.

  In case of exception, logs the exception and returns []."
  [& args]
  (try
    (apply dequeue* args)
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
