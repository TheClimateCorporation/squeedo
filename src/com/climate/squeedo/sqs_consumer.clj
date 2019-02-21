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
    [clojure.core.async :refer [close! go-loop go thread >! <! <!! chan buffer onto-chan timeout]]
    [clojure.core.async.impl.protocols :refer [closed?]]
    [clojure.tools.logging :as log]
    [com.climate.squeedo.sqs :as sqs]))

(defn- create-queue-listeners
  "Kick off listeners in the background that eagerly grab messages as quickly as possible and fetch them into a buffered
  channel.

  This will park the thread if the message channel is full. (ie. don't prefetch too many messages as there is
  a memory impact, and they have to get processed before timing out.)

  If there is an Exception while trying to poll for messages, wait exceptional-poll-delay-ms before trying again."
  [connection num-listeners buffer-size dequeue-limit exceptional-poll-delay-ms]
  (let [buf (buffer buffer-size)
        message-chan (chan buf)]
    (dotimes [_ num-listeners]
      (go-loop []
        (try
          (let [messages (sqs/dequeue* connection :limit dequeue-limit)]
            ; park until all messages are put onto message-channel
            (<! (onto-chan message-chan messages false)))
          (catch Throwable t
            (log/errorf t "Encountered exception dequeueing.  Waiting %d ms before trying again." exceptional-poll-delay-ms)
            (<! (timeout exceptional-poll-delay-ms))))
        (when (not (closed? message-chan))
          (recur))))
    [message-chan buf]))

(defn- create-dedicated-queue-listeners
  "Similar to `create-queue-listeners` but listeners are created on dedicated threads and will be blocked when the
  message channel is full.  Potentially useful when consuming from large numbers of SQS queues within a single program.

  If there is an Exception while trying to poll for messages, wait exceptional-poll-delay-ms before trying again."
  [connection num-listeners buffer-size dequeue-limit exceptional-poll-delay-ms]
  (let [buf (buffer buffer-size)
        message-chan (chan buf)]
    (dotimes [_ num-listeners]
      (thread
        (loop []
          (try
            (let [messages (sqs/dequeue* connection :limit dequeue-limit)]
              ; block until all messages are put onto message-channel
              (<!! (onto-chan message-chan messages false)))
            (catch Throwable t
              (log/errorf t "Encountered exception dequeueing.  Waiting %d ms before trying again." exceptional-poll-delay-ms)
              (Thread/sleep exceptional-poll-delay-ms)))
          (when (not (closed? message-chan))
            (recur)))))
    [message-chan buf]))

(defn- worker
  [work-token-chan message-chan compute done-chan]
  (go-loop []
    (>! work-token-chan :token)
    (when-let [message (<! message-chan)]
      (try
        (compute message done-chan)
        (catch Throwable _
          (>! done-chan (assoc message :nack true))))
      (recur))))

(defn- acker
  [connection work-token-chan done-chan]
  (go-loop []
    (when-let [message (<! done-chan)]
      ; free up the work-token-chan
      (<! work-token-chan)
      ; (n)ack the message asynchronously
      (let [nack (:nack message)]
        (go
          (cond
            (integer? nack) (sqs/nack connection message (:nack message))
            nack            (sqs/nack connection message)
            :else           (sqs/ack connection message))))
      (recur))))

(defn- create-workers
  "Create workers to run the compute function. Workers are expected to be CPU bound or handle all IO in an asynchronous
  manner. In the future we may add an option to run computes in a thread/pool that isn't part of the core.async's
  threadpool."
  [connection worker-size max-concurrent-work message-chan compute]
  (let [done-chan (chan worker-size)
        ; the work-token-channel ensures we only have a fixed numbers of messages processed at one time
        work-token-chan (chan max-concurrent-work)]
    (dotimes [_ worker-size]
      (worker work-token-chan message-chan compute done-chan))
    (acker connection work-token-chan done-chan)
    done-chan))

(defn- ->options-map
  "If options are provided as a map, return it as-is; otherwise, the options are provided as varargs and must be
  converted to a map"
  [options]
  (if (= 1 (count options))
    ; if only 1 option, assume it is a map, otherwise options are provided in varargs pairs
    (first options)
    ; convert the varags list into a map
    (apply hash-map options)))

(defn- get-available-processors
  []
  (.availableProcessors (Runtime/getRuntime)))

(defn- get-worker-count
  [options]
  (or (:num-workers options)
      (max 1 (- (get-available-processors) 1))))

(defn- get-listener-count
  [worker-count options]
  (or (:num-listeners options)
      (max 1 (int (/ worker-count 10)))))

(defn- get-listener-threads
  [options]
  (or (:listener-threads? options) false))

(defn- get-message-channel-size
  [listener-count options]
  (or (:message-channel-size options)
      (* 20 listener-count)))

(defn- get-max-concurrent-work
  [worker-count options]
  (or (:max-concurrent-work options)
      worker-count))

(defn- get-dequeue-limit
  [options]
  (or (:dequeue-limit options)
      10))

(defn- get-exceptional-poll-delay-ms
  [options]
  (or (:exceptional-poll-delay-ms options)
      10000))

(defn- dead-letter-deprecation-warning
  [options]
  (when (:dl-queue-name options)
    (println
      (str "WARNING - :dl-queue-name option for com.climate.squeedo.sqs-consumer/start-consumer"
           " has been removed. Please use com.climate.squeedo.sqs/configure to configure an SQS"
           " dead letter queue."))))

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

   Input:
    queue-name - the name of an sqs queue
    compute - a compute function that takes two args: a 'message' containing the body of the sqs
              message and a channel on which to ack/nack when done.

   Optional arguments:
    :message-channel-size      - the number of messages to prefetch from sqs; default 20 * num-listeners
    :num-workers               - the number of workers processing messages concurrently
    :num-listeners             - the number of listeners polling from sqs; default is (num-workers / 10) because each
                                 listener dequeues up to 10 messages at a time
    :listener-threads?         - run listeners in dedicated threads; if true, will create one thread per listener
    :dequeue-limit             - the number of messages to dequeue at a time; default 10
    :max-concurrent-work       - the maximum number of total messages processed.  This is mainly for async workflows;
                                 default num-workers
    :client                    - the SQS client to use (if missing, sqs/mk-connection will create a client)
    :exceptional-poll-delay-ms - when an Exception is received while polling, the number of ms we wait until polling
                                 again.  Default is 10000 (10 seconds).
   Output:
    a map with keys, :done-channel    - the channel to send messages to be acked
                     :message-channel - unused by the client."
  [queue-name compute & opts]
  (let [options (->options-map opts)
        _ (dead-letter-deprecation-warning options)
        connection (sqs/mk-connection queue-name :client (:client options))
        worker-count (get-worker-count options)
        listener-count (get-listener-count worker-count options)
        message-chan-size (get-message-channel-size listener-count options)
        max-concurrent-work (get-max-concurrent-work worker-count options)
        dequeue-limit (get-dequeue-limit options)
        exceptional-poll-delay-ms (get-exceptional-poll-delay-ms options)
        [message-chan _] (if (get-listener-threads options)
                           (create-dedicated-queue-listeners
                             connection listener-count message-chan-size dequeue-limit exceptional-poll-delay-ms)
                           (create-queue-listeners
                             connection listener-count message-chan-size dequeue-limit exceptional-poll-delay-ms))
        done-chan (create-workers
                    connection worker-count max-concurrent-work message-chan compute)]
    {:done-channel done-chan :message-channel message-chan}))

(defn stop-consumer
  "Takes a consumer created by start-consumer and closes the channels.
  This should be called to stopped consuming messages."
  [{:keys [done-channel message-channel]}]
  (close! message-channel)
  (close! done-channel))
