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
(ns com.climate.squeedo.sqs-consumer
  "Functions for using Amazon Simple Queueing Service to request and perform
 computation."
  (:require
    [clojure.core.async :refer [close! go-loop go >! <! <!! >!! chan buffer onto-chan]]
    [com.climate.squeedo.sqs :as gsqs]
    [clojure.tools.logging :as log]))

(defn- create-queue-listener
  " kick off a listener in the background that eagerly grabs messages as quickly
    as possible and fetches them into a buffered channel.  This will park the thread
    if the message channel is full. (ie. don't prefetch too many messages as there is a memory
    impact, and they have to get processed before timing out.)
  "
  [connection num-listeners buffer-size dequeue-limit]
  (let [buf (buffer buffer-size)
        message-channel (chan buf)]
    (dotimes [_ num-listeners]
      (go (while
            (let [messages (gsqs/dequeue connection :limit dequeue-limit)]
              ; block until all messages are put onto message-channel
              (<! (onto-chan message-channel messages false))
              (not (.closed? message-channel))))))
    [message-channel buf]))

(defn- create-workers
  [connection worker-size max-concurrent-work message-channel compute]
  (let [done-channel (chan worker-size)
        ; the work-token-channel ensures we only have a fixed numbers of messages processed at one time
        work-token-channel (chan max-concurrent-work)]
    (dotimes [_ worker-size]
      (go-loop []
        (>! work-token-channel :token)
        (when-let [message (<! message-channel)]
          (try
            (compute message done-channel)
            (catch Throwable t
              (log/error t "Error thrown by compute, saving go block and nacking message")
              (>! done-channel (assoc message :nack true))))
          (recur))))

    (go-loop []
      (when-let [message (<! done-channel)]
        (do
          ; free up the work-token-channel
          (<! work-token-channel)
          ; (n)ack the message asynchronously
          (if (:nack message)
            (go (gsqs/nack connection message))
            (go (gsqs/ack connection message)))
          (recur))))
    done-channel))

(defn start-consumer
  "Creates a consumer that reads messages as quickly as possible into a local buffer up
   to the configured buffer size.

   Work is done asynchronously by workers controlled by the size of the work buffer (currently
   hardcoded to number of cpus minus 1) Bad things happen if you have workers > number of cpus.

   The client has responsibility for calling stop-consumer when you no longer want to process messages.
   Additionally, the client MUST send the message back on the done-channel when processing is complete
   or if an uncaught exception occurs the message will be auto-nack'd in SQS.

   Failed messages will currently not be acked, but rather go back on for redelivery after the timeout.

   This code is atom free :)

   inputs:
    queue-name - the name of an sqs queue (will be created if necessary)

    compute - a compute function that takes two args: a 'message' containing the body of the sqs
              message and a channel on which to ack/nack when done.
    opts -
           :message-channel-size : the number of messages to prefetch from sqs; default 20 * num-listeners
           :num-workers : the number of workers processing messages concurrently
           :num-listeners : the number of listeners polling from sqs. default is (num-workers / 10)
                            since each listener dequeues up to 10 messages at a time
           :dequeue-limit : the number of messages to dequeue at a time; default 10
           :max-concurrent-work : the maximum number of total messages processed.  This is mainly for
                            asynch workflows; default num-workers
           :dl-queue-name : the dead letter queue to which messages that are failed the maximum number of
                            times will go (will be created if necessary). Defaults to (str queue-name \"-failed\")
   outputs:
    a map with keys, :done-channel - the channel to send messages to be acked
                     :message-channel - unused by the client.
  "
  [queue-name compute
   & {:keys [message-channel-size num-workers num-listeners dl-queue-name dequeue-limit max-concurrent-work]
      :or {num-workers (max 1 (- (.. Runtime getRuntime availableProcessors) 1))
           dl-queue-name (str queue-name "-failed")
           dequeue-limit 10}}]
  (let [connection (gsqs/mk-connection queue-name
                     :dead-letter dl-queue-name)
        n-listeners (or num-listeners (max 1 (int (/ num-workers 10))))
        message-channel-size (or message-channel-size (* 20 n-listeners))
        max-concurrent-work (or max-concurrent-work num-workers)
        message-channel (first (create-queue-listener
                                 connection n-listeners message-channel-size dequeue-limit))
        done-channel (create-workers connection num-workers max-concurrent-work message-channel compute)]
    {:done-channel done-channel :message-channel message-channel}))

(defn stop-consumer
  "Takes a consumer created by start-consumer and closes the channels.
  This should be called to stopped consuming messages."
  [{:keys [done-channel message-channel]}]
  (close! message-channel)
  (close! done-channel))
