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
(ns com.climate.squeedo.test.sqs-consumer
  (:require
    [cemerick.bandalore :as bandalore]
    [clojure.core.async :refer [<!! >!! <! timeout close! buffer chan go >!]]
    [clojure.test :refer :all]
    [clojure.core.async :refer [<!! >!! <! put! timeout close! buffer chan go >!]]
    [org.httpkit.client]
    [clojure.tools.logging :as log]
    [com.climate.squeedo.sqs :as sqs]
    [com.climate.squeedo.test.sqs :as sqs-test]
    [cemerick.bandalore :as sqs-client]
    [com.climate.squeedo.sqs-consumer :as sqs-server]
    [com.climate.claypoole :as cp])
  (:import
    [java.util.concurrent TimeUnit]
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

(defn async-get [url result message]
  (org.httpkit.client/get url (fn [r] (go
                                        (>! result message)
                                        (swap! tracker inc)))))
; for testing other types of computes
(defn simple-compute [message done-channel]
  ;(println message)
  (put! done-channel message)
  (swap! tracker inc))

(defn compute [message done-channel]
  ; do something expensive
  (reduce + (range 1 100000))
  ; do this if you will have I/O
  (async-get "http://google.com" done-channel message))

(defn slow-compute [message done-channel]
  ; don't ever do this.
  (Thread/sleep 2000)
  (put! done-channel message))


(deftest test-create-queue-listener
  (testing "Verify messages up to the buffer-size are retrieved"
    (let [test-chan (chan 10)
          _ (doseq [i (range 4)]
              (>!! test-chan "ignored"))]
      (with-redefs [sqs/dequeue (fn [& _] [(<!! test-chan)])]
        (let [listener (#'sqs-server/create-queue-listener nil 1 2 1)
              message-channel (first listener)
              buf (second listener)
              wait-and-check (fn [count]
                               (with-timeout 1000
                                 (while (< (.count buf) count)
                                   (Thread/sleep 100)))
                               (is (true? (.full? buf)))
                               (is (= (.count buf) count))
                               (<!! message-channel))]
          (wait-and-check 2)
          (wait-and-check 2)
          (wait-and-check 2)
          (is (false? (.full? buf)))
          (is (= 1 (.count buf)))
          (close! message-channel))))))

(deftest create-workers
  (testing "Verify workers ack processed messages"
    (with-redefs [sqs/ack (fn [_ _] (swap! tracker inc))]
      (let [message-channel (chan (buffer 4))
            done-channel (#'sqs-server/create-workers nil 2 2 message-channel slow-compute)]
        (doseq [_ (range 4)] (>!! message-channel "ignored"))
        (with-timeout 1000000
          (while (not (= 4 @tracker))
            (Thread/sleep 100)))
        (close! message-channel)
        (close! done-channel)))))

(deftest verify-opts-to-start-consumer
  (with-redefs [sqs/mk-connection (fn [_ _ _] {:client "client"
                                               :queue-name "q"
                                               :queue-url "http://"})]
    (testing "message-channel-size defaults to 20 and
            worker-size defaults to number of cpus - 1 and
            num-listeners is 1
            dequeue-limit is 10"
      (with-redefs [sqs-server/create-queue-listener (fn [_ num-listeners message-channel-size dequeue-limit]
                                                       (is (= message-channel-size 20))
                                                       (is (= num-listeners
                                                             (-> Runtime
                                                               (.. getRuntime availableProcessors)
                                                               (- 1)
                                                               (/ 10)
                                                               int
                                                               (max 1))))
                                                       (is (= dequeue-limit 10))
                                                       [1])
                    sqs-server/create-workers (fn [_ num-workers _ _ _]
                                                (is (= num-workers
                                                      (- (.. Runtime getRuntime availableProcessors)
                                                        1))))]
        (sqs-server/start-consumer "q" (fn [_ _] println) :dl-queue-name "q-dl")))
    (testing "message-channel-size can be configured"
      (with-redefs [sqs-server/create-queue-listener (fn [_ _ message-channel-size _]
                                                       (is (= message-channel-size 10))
                                                       [1])
                    sqs-server/create-workers (fn [_ _ _ _ _] nil)]
        (sqs-server/start-consumer "q" (fn [_ _] nil) :message-channel-size 10 :dl-queue-name "q-dl")))
    (testing "worker-size can be configured"
      (with-redefs [sqs-server/create-queue-listener (fn [_ _ _ _]
                                                       [1])
                    sqs-server/create-workers (fn [_ num-workers _ _ _]
                                                (is (= num-workers 100)))]
        (sqs-server/start-consumer "q" (fn [_ _] nil) :num-workers 100 :dl-queue-name "q-dl")))
    (testing "num-listeners can be configured"
      (with-redefs [sqs-server/create-queue-listener (fn [_ num-listeners _ _]
                                                       (is (= num-listeners 10))
                                                       [1])
                    sqs-server/create-workers (fn [_ _ _ _ _] nil)]
        (sqs-server/start-consumer "q" (fn [_ _] nil) :num-listeners 10 :dl-queue-name "q-dl"))))
  (testing "dl-queue-name defaults to queue-name-failed"
    (with-redefs [sqs/mk-connection (fn [q-name _ dl-queue-name]
                                      (is (= "q-failed" dl-queue-name))
                                      (is (= "q" q-name))
                                      {:client "client"
                                       :queue-name q-name
                                       :queue-url "http://"})
                  sqs-server/create-queue-listener (fn [_ _ _ _] [1])
                  sqs-server/create-workers (fn [_ _ _ _ _] nil)]
      (sqs-server/start-consumer "q" (fn [_ _] nil)))))


(deftest ^:integration test-create-queue-listener-integration
  (testing "Verify messages up to the buffer-size are retrieved"
    (sqs-test/with-temporary-queue
      [queue-name dlq-name]
      (let [connection (sqs/mk-connection queue-name :dead-letter dlq-name)
            listener (#'sqs-server/create-queue-listener connection 1 2 1)
            message-channel (first listener)
            buf (second listener)
            _ (doseq [i (range 4)] (sqs/enqueue connection i))
            wait-and-check (fn [count]
                             (with-timeout 10000
                               (while (< (.count buf) count)
                                 (Thread/sleep 100)))
                             (is (true? (.full? buf)))
                             (is (= (.count buf) count))
                             (<!! message-channel))]
        (wait-and-check 2)
        (wait-and-check 2)
        (wait-and-check 2)
        (is (false? (.full? buf)))
        (is (= (.count buf) 1))
        (close! message-channel)))))


(deftest test-message-processing-concurrency
  (testing "Verify the maximum number of messages processed concurrently doesn't exceed the number
   of workers"
    (with-redefs [sqs/ack (fn [_ _] nil)
                  sqs/dequeue (fn [_ _ _]
                                {:id 1 :body "message"})
                  sqs/mk-connection (fn [_ _ _] {})]
      (let [num-workers 4
            consumer (sqs-server/start-consumer "queue-name"
                       (fn [_ _]
                         ; an intentionally bad consumer,
                         ; that forgets to ack back
                         (swap! tracker inc))
                       :num-workers num-workers)]
        (with-timeout 1000
          (while (not (= num-workers @tracker))
            (Thread/sleep 100)))
        ; wait a bit to make sure nothing else gets grabbed
        (Thread/sleep 200)
        (is (= @tracker num-workers))
        (sqs-server/stop-consumer consumer)))))


(deftest ^:integration consumer-happy-path
  (testing "Verify it consumes all messages properly"
    (sqs-test/with-temporary-queue
      [queue-name dlq-name]
      (let [connection (sqs/mk-connection queue-name :dead-letter dlq-name)
            _ (doseq [i (range 10)] (sqs/enqueue connection i))
            start (System/currentTimeMillis)
            consumer (sqs-server/start-consumer queue-name compute :dl-queue-name dlq-name)]

        (with-timeout 1000000
          (while (< @tracker 10)
            (Thread/sleep 100)))
        (println "total: " (- (System/currentTimeMillis) start))
        (Thread/sleep 100)
        (is (= 10 @tracker))
        (sqs-server/stop-consumer consumer)))))

(deftest ^:integration consumer-continues-processing
  (testing "Verify it consumes messages after queue empty"
    (binding [sqs/poll-timeout-seconds 0]
      (sqs-test/with-temporary-queue
        [queue-name dlq-name]
        (let [
               connection (sqs/mk-connection queue-name :dead-letter dlq-name)
               _ (doseq [i (range 5)] (sqs/enqueue connection i))
               consumer (sqs-server/start-consumer queue-name compute :dl-queue-name dlq-name)]

          (with-timeout 10000
            (while (< @tracker 5)
              (Thread/sleep 100)))
          (is (= 5 @tracker))
          ; wait for a bit to simulate no messages on the queue for a while
          (Thread/sleep 2000)
          (doseq [i (range 5)] (sqs/enqueue connection i))
          (with-timeout 10000
            (while (< @tracker 10)
              (Thread/sleep 100)))
          (is (= 10 @tracker))
          (sqs-server/stop-consumer consumer))))))

(deftest ^:integration stop-consumer
  (testing "Verify stop-consumer closes channels"
    (sqs-test/with-temporary-queue
      [queue-name dlq-name]
      ;; Work with a test queue.
      (let [connection (sqs/mk-connection queue-name :dead-letter dlq-name)
            consumer (sqs-server/start-consumer queue-name compute :dl-queue-name dlq-name)]
        (is (false? (.closed? (:message-channel consumer))))
        (is (false? (.closed? (:done-channel consumer))))
        (sqs-server/stop-consumer consumer)
        (is (true? (.closed? (:message-channel consumer))))
        (is (true? (.closed? (:done-channel consumer))))))))

(deftest ^:integration nacking-works
  (testing "Verify we can nack a message and retry"
    (sqs-test/with-temporary-queue
      [queue-name dlq-name]
      (let [connection (sqs/mk-connection queue-name :dead-letter dlq-name)
            _ (sqs/enqueue connection "hello")
            start (System/currentTimeMillis)
            consumer (sqs-server/start-consumer
                       queue-name
                       (fn [message done-channel]
                         (swap! tracker
                                (fn [t]
                                  ; nack the first time, ack after
                                  (put! done-channel
                                        (assoc message :nack (= t 0)))
                                  (inc t))))
                       :dl-queue-name dlq-name)]
        (with-timeout 1000000
          (while (< @tracker 2)
            (Thread/sleep 100)))
        (println "total: " (- (System/currentTimeMillis) start))
        (Thread/sleep 100)
        (is (= 2 @tracker))
        (sqs-server/stop-consumer consumer)))))

(defn- time-consumer
  [& {:keys [n num-workers num-listeners dequeue-limit] :as args}]
  (sqs-test/with-temporary-queue
    [queue-name dlq-name]
    (let [connection (sqs/mk-connection queue-name :dead-letter dlq-name)
          _ (cp/upmap 100 (partial sqs/enqueue connection) (range n))
          start (System/currentTimeMillis)
          consumer (apply sqs-server/start-consumer
                     (concat [queue-name simple-compute :dl-queue-name dlq-name]
                       (reduce-kv conj [] args)))]

      (with-timeout 1000000
        (while (< @tracker n)
          (Thread/sleep 100)))
      (println (format "n %d, num-workers %d, num-listeners %d, dequeue-limit %d, time (ms): %d"
                 n num-workers num-listeners dequeue-limit
                 (- (System/currentTimeMillis) start)))
      (Thread/sleep 10000)
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
