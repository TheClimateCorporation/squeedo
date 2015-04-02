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
    [clojure.core.async.impl.protocols :refer [closed?]]
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
              (not (closed? message-channel))))))
    [message-channel buf]))

(defn- create-workers
  "Create workers to run the compute function. Workers are expected to be CPU bound or handle all IO in an asynchronous
  manner.  In the future we may add an option to run computes in a thread/pool that isn't part of the core.async's
  threadpool."
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

(defn- ->options-map
  "If options are provided as a map, return it as-is; otherwise, the options are provided as varargs and must be
  converted to a map"
  [options]
  (if (= 1 (count options))
    ; if only 1 option, assume it is a map, otherwise options are provided in varargs pairs
    (first options)
    ; convert the varags list into a map
    (apply hash-map options)))

(defn get-available-processors
  []
  (.availableProcessors (Runtime/getRuntime)))

(defn- get-dead-letter-queue-name
  [queue-name options]
  (or (:dl-queue-name options)
      (str queue-name "-failed")))

(defn- get-worker-count
  [options]
  (or (:num-workers options)
      (max 1 (- (get-available-processors) 1))))

(defn- get-listener-count
  [worker-count options]
  (or (:num-listeners options)
      (max 1 (int (/ worker-count 10)))))

(defn- get-message-channel-size
  [listener-count options]
  (or (:message-channel-size options)
      (* 20 listener-count)))

(defn get-max-concurrent-work
  [worker-count options]
  (or (:max-concurrent-work options)
      worker-count))

(defn get-dequeue-limit
  [options]
  (or (:dequeue-limit options)
      10))

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
                        times will go (will be created if necessary).
                        Defaults to (str queue-name \"-failed\")
   outputs:
    a map with keys, :done-channel - the channel to send messages to be acked
                     :message-channel - unused by the client.
  "
  [queue-name compute & opts]
  (let [options (->options-map opts)
        dead-letter-queue-name (get-dead-letter-queue-name queue-name options)
        connection (gsqs/mk-connection queue-name :dead-letter dead-letter-queue-name)
        worker-count (get-worker-count options)
        listener-count (get-listener-count worker-count options)
        message-channel-size (get-message-channel-size listener-count options)
        max-concurrent-work (get-max-concurrent-work worker-count options)
        dequeue-limit (get-dequeue-limit options)
        message-channel (first (create-queue-listener
                                 connection listener-count message-channel-size dequeue-limit))
        done-channel (create-workers connection worker-count max-concurrent-work message-channel compute)]
    {:done-channel done-channel :message-channel message-channel}))

(defn stop-consumer
  "Takes a consumer created by start-consumer and closes the channels.
  This should be called to stopped consuming messages."
  [{:keys [done-channel message-channel]}]
  (close! message-channel)
  (close! done-channel))
