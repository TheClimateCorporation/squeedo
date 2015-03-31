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
  (:import
    [com.amazonaws.services.sqs.model ListQueuesRequest]))

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

(defn validate-queue-name!
  "Validates input for SQS queue names.

  Returns nil, or throws an IllegalArgumentException on invalid queue name."
  [queue-name]
  (when (or (empty? queue-name)
          (re-find #"[^A-Za-z0-9_-]" queue-name)
          (>= (count queue-name) 80))
    (throw (IllegalArgumentException.
             (str "Queue names can only include alphanumeric characters"
               " hyphens, or underscores. Queue name should be less than"
               " 80 characters.")))))

(defn- redrive-policy
  "Encode the RedrivePolicy for a dead letter queue.
  http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html"
  [client dead-letter-queue]
  {"RedrivePolicy"
    (json/generate-string                                   ; yes, this is really necessary.
      {"maxReceiveCount"     maximum-retries
       "deadLetterTargetArn" ((sqs/queue-attrs client dead-letter-queue)
                              "QueueArn")})})

(defn- get-queue
  "Retrieve the queue URL (Amazon Resource Name) from SQS.

  Optionally takes a dead letter queue URL (Amazon Resource Name),
  a queue that already exists, that gets associated with the returned queue."
    [client queue-name & [dead-letter-arn]]
  (let [default-attrs {"DelaySeconds"                  0    ; delay after enqueue
                       "MessageRetentionPeriod"        1209600 ; max, 14 days
                       "ReceiveMessageWaitTimeSeconds" poll-timeout-seconds
                       "VisibilityTimeout"             auto-retry-seconds}
        ; Tip: don't try sending attrs as create-queue's 'options' param: they aren't the same
        q (sqs/create-queue client queue-name)]
    (sqs/queue-attrs client q
      (merge default-attrs (when dead-letter-arn (redrive-policy client dead-letter-arn))))
    q))

(defn mk-connection
  "Create an SQS connection to a queue identified by string queue-name. This
  should be done once at startup time.

  Optionally takes the name of a dead-letter queue, where messages that failed
  too many times will go.
  http://aws.typepad.com/aws/2014/01/amazon-sqs-new-dead-letter-queue.html
  "
  [queue-name & {:keys [dead-letter]}]
  (validate-queue-name! queue-name)
  (when dead-letter
    (validate-queue-name! dead-letter))
  (let [client (sqs/create-client)
        queue-url (if dead-letter
                    (get-queue client queue-name (get-queue client dead-letter))
                    (get-queue client queue-name))]
    (log/infof "Using SQS queue %s at %s" queue-name queue-url)
    {:client     client
     :queue-name queue-name
     :queue-url  queue-url}))

(defn enqueue
  [{:keys [client queue-name queue-url] :as connection} message]
  "Add a message to a queue."
  (log/debugf "Enqueueing %s to %s" message queue-name)
  (sqs/send client queue-url (pr-str message)))

;; Raw message is being returned, so it's up to the user to determine the proper reader for their needs.
;; NaN,Infinity,-Infinity http://dev.clojure.org/jira/browse/CLJ-1074
(defn dequeue
  "Read messages from a queue. If there is nothing to read before
  poll-timeout-seconds, return [].

  This does *not* remove the messages from the queue! For that, see ack.

  In case of exception, logs the exception and returns []."
  [{:keys [client queue-name queue-url] :as connection}
   & {:keys [limit]
      :or {limit 10}}]
  ;; Log at debug so we don't spam about polling.
  (log/debugf "Attempting dequeue from %s" queue-name)
  ;; will have a single queue/connection
  (try
    (->>
      (sqs/receive client queue-url :wait-time-seconds poll-timeout-seconds :limit limit)
      (map (fn [m]
             (log/debugf "Dequeued from queue %s message %s" queue-name m)
             (assoc m :queue-name queue-name))))
    (catch Exception e
      (log/error e "Encountered exception dequeueing.")
      [])))

(defn ack
  "Remove the message from the queue."
  [{:keys [client] :as connection} message]
  (log/debugf "Acking message %s" message)
  (sqs/delete client message))

(defn nack
  "Put the message back on the queue.
   input:
     connection - as created by mk-connection
     message - the message to nack
     nack-visibility-seconds (optional) - How long to wait before retrying a failed compute request.
                                          Defaults to 0."
  [{:keys [client queue-url] :as connection} message & [nack-visibility-seconds]]
   (log/debugf "Nacking message %s" message)
   (sqs/change-message-visibility client queue-url message
                                  (int (or nack-visibility-seconds 0))))
