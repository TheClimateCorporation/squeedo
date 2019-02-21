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
(ns com.climate.squeedo.sqs-consumer-test
  (:require
    [clojure.test :refer :all]
    [clojure.core.async :refer [<!! >!! <! put! timeout close! buffer chan go >!]]
    [org.httpkit.client]
    [com.climate.squeedo.sqs :as sqs]
    [com.climate.squeedo.test-utils :refer [with-temporary-queues]]
    [com.climate.squeedo.sqs-consumer :as sqs-server]
    [com.climate.claypoole :as cp])
  (:import
    [java.util.concurrent TimeoutException]))

(defonce tracker (atom 0))

(defn before [f]
  (reset! tracker 0)
  (f))

(use-fixtures :each before)

(defmacro with-timeout
  [msec & body]
  `(let [f# (future (do ~@body))
         v# (gensym)
         result# (deref f# ~msec v#)]
     (if (= v# result#)
       (do
         (future-cancel f#)
         (throw (TimeoutException.)))
       result#)))

(defn async-get [url message channel compute-more-fn]
  (org.httpkit.client/get url (fn [r] (go
                                        ; do some more processing with the response
                                        (when compute-more-fn
                                          (compute-more-fn))
                                        (>! channel message)
                                        (swap! tracker inc)))))

(defn- eat-some-cpu [how-much]
  (reduce + (range 1 how-much)))

(defn- wait-for-messages [num-messages timeout]
  (with-timeout timeout
                (while (not (= num-messages @tracker))
                  (Thread/sleep 100))))

; for testing other types of computes
(defn- simple-compute [message done-channel]
  ;(println message)
  (put! done-channel message)
  (swap! tracker inc))

(defn- compute [message done-channel]
  ; do something expensive
  (eat-some-cpu 100000)
  ; do this if you will have I/O
  (async-get "http://google.com" message done-channel nil))

(defn- slow-compute [message done-channel]
  ; don't ever do this.
  (Thread/sleep 2000)
  (put! done-channel message))

(defn- stub-compute-fn [_ _] nil)

(deftest test-create-queue-listeners
  (testing "Verify messages up to the buffer-size are retrieved"
    (let [test-chan (chan 10)
          _ (doseq [_ (range 4)]
              (>!! test-chan "ignored"))]
      (with-redefs [sqs/dequeue* (fn [& _] [(<!! test-chan)])]
        (let [[message-chan buf] (#'sqs-server/create-queue-listeners nil 1 2 1 0)
              wait-and-check (fn [count]
                               (with-timeout 1000
                                             (while (< (.count buf) count)
                                               (Thread/sleep 100)))
                               (is (true? (.full? buf)))
                               (is (= (.count buf) count))
                               (<!! message-chan))]
          (wait-and-check 2)
          (wait-and-check 2)
          (wait-and-check 2)
          (is (false? (.full? buf)))
          (is (= 1 (.count buf)))
          (close! message-chan)))))
  (testing "Verify retry after specified ms"
    (let [test-chan (chan 10)
          _ (>!! test-chan "ignored")
          first-call (atom true)]
      (with-redefs [sqs/dequeue* (fn [& _]
                                   (when @first-call
                                     (reset! first-call false)
                                     (throw (Exception. "thrown first time only")))
                                   [(<!! test-chan)])]
        (let [retry-ms 123
              [message-chan _] (#'sqs-server/create-queue-listeners nil 1 2 1 retry-ms)
              start-ms (System/currentTimeMillis)]
          (<!! message-chan)
          (is (>= (- (System/currentTimeMillis) start-ms) retry-ms))
          (close! message-chan))))))

(deftest test-create-dedicated-queue-listeners
  (testing "Verify messages up to the buffer-size are retrieved"
    (let [first-call (atom true)]
      (with-redefs [sqs/dequeue* (fn [& _]
                                   (if @first-call
                                     (do (reset! first-call false)
                                         ["msg1" "msg2" "msg3" "msg4"])
                                     []))]
        (let [[message-chan buf] (#'sqs-server/create-dedicated-queue-listeners nil 1 2 1 0)
              wait-and-check (fn [count]
                               (with-timeout 1000
                                             (while (< (.count buf) count)
                                               (Thread/sleep 100)))
                               (is (true? (.full? buf)))
                               (is (= (.count buf) count))
                               (<!! message-chan))]
          (wait-and-check 2)
          (wait-and-check 2)
          (wait-and-check 2)
          (is (false? (.full? buf)))
          (is (= 1 (.count buf)))
          (close! message-chan)))))
  (testing "Verify retry after specified ms"
    (let [call-number (atom 0)]
      (with-redefs [sqs/dequeue* (fn [& _]
                                   (swap! call-number inc)
                                   (case @call-number
                                         1 (throw (Exception. "thrown first time only"))
                                         2 ["msg1" "msg2" "msg3" "msg4"]
                                         []))]
        (let [retry-ms 123
              [message-chan _] (#'sqs-server/create-dedicated-queue-listeners nil 1 2 1 retry-ms)
              start-ms (System/currentTimeMillis)]
          (<!! message-chan)
          (is (>= (- (System/currentTimeMillis) start-ms) retry-ms))
          (close! message-chan))))))

(deftest create-workers
  (testing "Verify workers ack processed messages"
    (with-redefs [sqs/ack (fn [_ _] (swap! tracker inc))]
      (let [num-messages 4
            message-channel (chan (buffer num-messages))
            done-channel (#'sqs-server/create-workers nil 2 2 message-channel slow-compute)]
        (doseq [_ (range num-messages)] (>!! message-channel "ignored"))
        (wait-for-messages num-messages 60000)
        (close! message-channel)
        (close! done-channel)))))

(deftest verify-opts-to-start-consumer
  (with-redefs [sqs/mk-connection (fn [& _] {:client     "client"
                                             :queue-name "q"
                                             :queue-url  "http://"})]
    (testing "message-channel-size defaults to 20 and
             worker-size defaults to number of cpus - 1 and
             num-listeners is 1
             dequeue-limit is 10"
      (with-redefs [sqs-server/create-queue-listeners (fn [_ num-listeners message-channel-size dequeue-limit exceptional-poll-delay-ms]
                                                        (is (= 20 message-channel-size))
                                                        (is (= (-> (#'sqs-server/get-available-processors)
                                                                   (- 1)
                                                                   (/ 10)
                                                                   int
                                                                   (max 1))
                                                               num-listeners))
                                                        (is (= 10 dequeue-limit))
                                                        (is (= 10000 exceptional-poll-delay-ms))
                                                        [1 nil])
                    sqs-server/create-workers (fn [_ num-workers _ _ _]
                                                (is (= num-workers
                                                       (- (#'sqs-server/get-available-processors)
                                                          1))))]
        (is (sqs-server/start-consumer "q" (fn [_ _] println)))))
    (testing "message-channel-size can be configured"
      (with-redefs [sqs-server/create-queue-listeners (fn [_ _ message-channel-size _ _]
                                                        (is (= 10 message-channel-size))
                                                        [1 nil])
                    sqs-server/create-workers (fn [_ _ _ _ _] nil)]
        (is (sqs-server/start-consumer "q" stub-compute-fn :message-channel-size 10))))
    (testing "worker-size can be configured"
      (with-redefs [sqs-server/create-queue-listeners (constantly [1 nil])
                    sqs-server/create-workers (fn [_ num-workers _ _ _] (is (= 100 num-workers)))]
        (is (sqs-server/start-consumer "q" stub-compute-fn :num-workers 100))))
    (testing "num-listeners can be configured"
      (with-redefs [sqs-server/create-queue-listeners (fn [_ num-listeners _ _ _]
                                                        (is (= 10 num-listeners))
                                                        [1 nil])
                    sqs-server/create-workers (fn [_ _ _ _ _] nil)]
        (sqs-server/start-consumer "q" stub-compute-fn :num-listeners 10))))
  (testing "mk-connection"
    (with-redefs [sqs/mk-connection (fn [q-name & _]
                                      (is (= ::name q-name))
                                      {:client "client"
                                       :queue-name q-name
                                       :queue-url "http://"})
                  sqs-server/create-queue-listeners (constantly [1 nil])
                  sqs-server/create-workers (fn [_ _ _ _ _] nil)]
      (is (sqs-server/start-consumer ::name stub-compute-fn))))
  (testing "options can be configured via map"
    (let [queue-name ::queue-name
          worker-count ::worker-count
          listener-count ::listener-count
          message-channel-size ::message-channel-size
          max-concurrent-work ::max-concurrent-work
          dequeue-limit ::dequeue-limit
          client ::client
          exceptional-poll-delay-ms ::exceptional-poll-delay-ms
          options-map {:num-workers               worker-count
                       :num-listeners             listener-count
                       :message-channel-size      message-channel-size
                       :max-concurrent-work       max-concurrent-work
                       :dequeue-limit             dequeue-limit
                       :exceptional-poll-delay-ms exceptional-poll-delay-ms
                       :client                    client}
          stub-connection {:client     client
                           :queue-name queue-name
                           :queue-url  ::queue-url}
          stub-message-channel 1]
      (with-redefs [sqs/mk-connection (fn [input-queue-name & {input-client :client}]
                                        (is (= queue-name input-queue-name))
                                        (is (= client input-client))
                                        stub-connection)
                    sqs-server/create-queue-listeners (fn [input-connection input-listener-count
                                                           input-message-channel-size input-dequeue-limit
                                                           input-exceptional-poll-delay]
                                                        (is (= stub-connection input-connection))
                                                        (is (= message-channel-size input-message-channel-size))
                                                        (is (= listener-count input-listener-count))
                                                        (is (= dequeue-limit input-dequeue-limit))
                                                        (is (= exceptional-poll-delay-ms input-exceptional-poll-delay))
                                                        [stub-message-channel nil])
                    sqs-server/create-workers (fn [input-connection input-worker-count input-max-concurrent-work
                                                   input-message-channel input-compute-fn]
                                                (is (= stub-connection input-connection))
                                                (is (= worker-count input-worker-count))
                                                (is (= max-concurrent-work input-max-concurrent-work))
                                                (is (= stub-message-channel input-message-channel))
                                                (is (= stub-compute-fn input-compute-fn)))]
        (sqs-server/start-consumer queue-name stub-compute-fn options-map)))))

(deftest test-message-processing-concurrency
  (testing "Verify the maximum number of messages processed concurrently doesn't exceed the number
           of workers"
    (with-redefs [sqs/ack stub-compute-fn
                  sqs/dequeue* (fn [_ _ _]
                                 {:id 1 :body "message"})
                  sqs/mk-connection (fn [& _] {})]
      (let [num-workers 4
            consumer (sqs-server/start-consumer "queue-name"
                                                (fn [_ _]
                                                  ; an intentionally bad consumer,
                                                  ; that forgets to ack back
                                                  (swap! tracker inc))
                                                :num-workers num-workers)]
        (wait-for-messages num-workers 1000)
        ; wait a bit to make sure nothing else gets grabbed
        (Thread/sleep 200)
        (is (= @tracker num-workers))
        (sqs-server/stop-consumer consumer)))))

(deftest ^:integration test-create-queue-listeners-integration
  (testing "Verify messages up to the buffer-size are retrieved"
    (with-temporary-queues
      [queue-name]
      (sqs/configure-queue queue-name)
      (let [connection (sqs/mk-connection queue-name)
            [message-chan buf] (#'sqs-server/create-queue-listeners connection 1 2 1 0)
            _ (doseq [i (range 4)] (sqs/enqueue connection i))
            wait-and-check (fn [count]
                             (with-timeout 10000
                                           (while (< (.count buf) count)
                                             (Thread/sleep 100)))
                             (is (true? (.full? buf)))
                             (is (= (.count buf) count))
                             (<!! message-chan))]
        (wait-and-check 2)
        (wait-and-check 2)
        (wait-and-check 2)
        (is (false? (.full? buf)))
        (is (= (.count buf) 1))
        (close! message-chan)))))

(deftest ^:integration consumer-happy-path
  (testing "Verify it consumes all messages properly"
    (with-temporary-queues
      [queue-name]
      (sqs/configure-queue queue-name)
      (let [connection (sqs/mk-connection queue-name)
            num-messages 10
            _ (doseq [i (range num-messages)] (sqs/enqueue connection i))
            start (System/currentTimeMillis)
            consumer (sqs-server/start-consumer queue-name compute)]
        (wait-for-messages num-messages 100000)
        (println "total: " (- (System/currentTimeMillis) start))
        (Thread/sleep 100)
        (is (= num-messages @tracker))
        (sqs-server/stop-consumer consumer)))))

(deftest ^:integration consumer-continues-processing
  (testing "Verify it consumes messages after queue empty"
    (binding [sqs/poll-timeout-seconds 0]
      (with-temporary-queues
        [queue-name]
        (sqs/configure-queue queue-name)
        (let [num-messages 5
              connection (sqs/mk-connection queue-name)
              _ (doseq [i (range num-messages)] (sqs/enqueue connection i))
              consumer (sqs-server/start-consumer queue-name compute)]
          (wait-for-messages num-messages 10000)
          (is (= num-messages @tracker))
          ; wait for a bit to simulate no messages on the queue for a while
          (Thread/sleep 2000)
          (doseq [i (range num-messages)] (sqs/enqueue connection i))
          (wait-for-messages (* num-messages 2) 10000)
          (is (= (* num-messages 2) @tracker))
          (sqs-server/stop-consumer consumer))))))

(deftest ^:integration stop-consumer
  (testing "Verify stop-consumer closes channels"
    (with-temporary-queues
      [queue-name]
      (sqs/configure-queue queue-name)
      ;; Work with a test queue.
      (let [consumer (sqs-server/start-consumer queue-name compute)]
        (is (false? (.closed? (:message-channel consumer))))
        (is (false? (.closed? (:done-channel consumer))))
        (sqs-server/stop-consumer consumer)
        (is (true? (.closed? (:message-channel consumer))))
        (is (true? (.closed? (:done-channel consumer))))))))

(deftest ^:integration nacking-works
  (testing "Verify we can nack a message and retry"
    (with-temporary-queues
      [queue-name]
      (sqs/configure-queue queue-name)
      (let [connection (sqs/mk-connection queue-name)
            _ (sqs/enqueue connection "hello")
            consumer (sqs-server/start-consumer
                       queue-name
                       (fn [message done-channel]
                         (swap! tracker
                                (fn [t]
                                  ; nack the first time, ack after
                                  (put! done-channel
                                        (assoc message :nack (= t 0)))
                                  (inc t)))))]
        (wait-for-messages 2 10000)
        (Thread/sleep 100)
        (is (= 2 @tracker))
        (sqs-server/stop-consumer consumer)))))

(deftest ^:integration nacking-timeout-works
  (testing "Verify we can nack a message with a visibility timeout"
    (with-temporary-queues
      [queue-name]
      (sqs/configure-queue queue-name)
      (let [connection (sqs/mk-connection queue-name)
            _ (sqs/enqueue connection "hello")
            consumer (sqs-server/start-consumer
                       queue-name
                       (fn [message done-channel]
                         (swap! tracker
                                (fn [t]
                                  ; nack the first time, ack after
                                  (put! done-channel
                                        (assoc message :nack (if (= t 0) 5 false)))
                                  (inc t)))))]
        (is (thrown? TimeoutException (wait-for-messages 2 4000)))
        (wait-for-messages 2 10000)
        (Thread/sleep 100)
        (is (= 2 @tracker))
        (sqs-server/stop-consumer consumer)))))

(defn- time-consumer
  [& {:keys [n num-workers num-listeners dequeue-limit] :as args}]
  (with-temporary-queues
    [queue-name]
    (sqs/configure-queue queue-name)
    (let [connection (sqs/mk-connection queue-name)
          _ (cp/upmap 100 (partial sqs/enqueue connection) (range n))
          start (System/currentTimeMillis)
          consumer (apply sqs-server/start-consumer
                          (concat [queue-name simple-compute]
                                  (reduce-kv conj [] args)))]

      (wait-for-messages n 1000000)
      (println (format "n %d, num-workers %d, num-listeners %d, dequeue-limit %d, time (ms): %d"
                       n num-workers num-listeners dequeue-limit
                       (- (System/currentTimeMillis) start)))
      (Thread/sleep 3000)
      (is (= n @tracker))
      ;; NB These tests sometimes end in AWS NonExistentQueue exception if not all
      ;; messages have been ack'd when the queue is deleted
      (sqs-server/stop-consumer consumer)
      (Thread/sleep 2000))))

(deftest ^:benchmark benchmark-consumer
  ;timings based on ec2 c3.xlarge
  (testing "Time consuming many messages"
    (time-consumer :n 1000 :num-workers 10 :num-listeners 1 :dequeue-limit 1) ; time (ms): 15333
    (reset! tracker 0)
    (time-consumer :n 1000 :num-workers 100 :num-listeners 1 :dequeue-limit 1) ; time (ms): 13118
    (reset! tracker 0)
    (time-consumer :n 1000 :num-workers 10 :num-listeners 1 :dequeue-limit 10) ; time (ms):  2128
    (reset! tracker 0)
    (time-consumer :n 1000 :num-workers 100 :num-listeners 1 :dequeue-limit 10) ; time (ms):  2299
    (reset! tracker 0)
    (time-consumer :n 1000 :num-workers 100 :num-listeners 10 :dequeue-limit 10) ; time (ms):   778
    (reset! tracker 0)
    (time-consumer :n 1000 :num-listeners 10 :dequeue-limit 10))) ; time (ms):   748

;; run this to see how good squeedo works with cpu and async non-blocking IO
;; on my 8 core machine i can drive cpu usage to 750%
(deftest ^:example-cpu example-awesome-cpu-usage
  (with-temporary-queues
    [queue-name]
    (sqs/configure-queue queue-name)
    (let [connection (sqs/mk-connection queue-name)
          intense-cpu-fn #(eat-some-cpu 1000000)
          intense-compute-fn (fn  [message done-channel]
                               (intense-cpu-fn)
                               (async-get "http://google.com" message done-channel intense-cpu-fn))
          num-messages 3000
          _ (cp/upmap 100 (partial sqs/enqueue connection) (range num-messages))
          start (System/currentTimeMillis)
          consumer (sqs-server/start-consumer queue-name
                                              intense-compute-fn
                                              :num-listeners 10
                                              :max-concurrent-work 50)]

      (wait-for-messages num-messages 1000000)
      (println "total: " (- (System/currentTimeMillis) start))
      (Thread/sleep 100)
      (is (= num-messages @tracker))
      (sqs-server/stop-consumer consumer))))

(defmacro with-temporary-queue-collection
  "Create num-queues number of SQS queues and bind a vector to sym containing the
  queues, presumably for testing large numbers of queues."
  [num-queues sym & body]
  (let [syms (vec (repeatedly num-queues gensym))]
    `(with-temporary-queues ~syms
       (let [~sym ~syms]
         ~@body))))

;; run this to see how squeedo can be configured to work with large number
;; of sqs queues (using listener threads).
;; on my machine this takes about 20 seconds, compared to about 120 seconds
;; without dedicated listener threads.
(deftest ^:example-listener-threads example-large-number-of-queues
  (let [compute-fn (fn [msg done-chan]
                     (put! done-chan msg)
                     (swap! tracker inc))]

    (with-temporary-queue-collection 60 queues

      ;; setup
      (is (= 60 (count queues)))
      (run! sqs/configure-queue queues)
      (cp/upmap 20 #(sqs/enqueue (sqs/mk-connection %) "hello world!") queues)

      ;; start the consumers
      (let [start (System/currentTimeMillis)
            consumers (doall
                        (for [queue queues]
                          (sqs-server/start-consumer queue compute-fn
                                                     :num-listeners 1
                                                     :num-workers 1
                                                     :listener-threads? true)))]
        (wait-for-messages (count queues) 100000)
        (is (= (count queues) @tracker)) ;; got all messages?
        (println "total for" (count queues) "queues: " (- (System/currentTimeMillis) start))

        ;; cleanup (physical SQS queues are deleted via `with-temporary-queues`)
        (doseq [consumer consumers]
          (sqs-server/stop-consumer consumer))))))
