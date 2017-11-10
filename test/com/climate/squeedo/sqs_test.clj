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
(ns com.climate.squeedo.sqs-test
  (:require
    [clojure.test :refer :all]
    [clojure.tools.logging :as log]
    [com.climate.squeedo.sqs :as sqs]
    [com.climate.squeedo.test-utils :refer [with-temporary-queue]]
    [clojure.tools.reader.edn :as edn]
    [cheshire.core :as json]
    [cemerick.bandalore :as band]))

(deftest valid-queue-name
  (testing "Queue name validity with special characters"
    (is (thrown? IllegalArgumentException
          (sqs/validate-queue-name!
            "a-queue-name-with.in-it"))))
  (testing "Queue name length check"
    (is (thrown? IllegalArgumentException
          (sqs/validate-queue-name!
            "areally-really-really-really-really-really-really-really-looooooooooooooooonnnnnnggg-queue-name")))
    (let [eighty-char-str (->> (repeat "L")
                               (take 80)
                               (reduce str))]
          (is (nil? (sqs/validate-queue-name!
                      eighty-char-str)))))
  (testing "Empty queue name"
    (is (thrown? IllegalArgumentException
          (sqs/validate-queue-name! ""))))
  (testing "Queue with .fifo suffix"
    (is (nil? (sqs/validate-queue-name! "hello-world.fifo")))))

(defn dequeue-1
  "Convenience function for some of these tests"
  [connection]
  (first (sqs/dequeue connection :limit 1)))

(deftest ^:integration test-queue-creation
  (with-temporary-queue
    [queue dl-queue]
    (testing "non-existent queue"
      (let [{:keys [client queue-url dead-letter]}
            (sqs/mk-connection queue
                               :dead-letter dl-queue
                               :queue-attributes {"VisibilityTimeout" "1"}
                               :dead-letter-queue-attributes {"VisibilityTimeout" "2"})]
        (is (= "1" (get (band/queue-attrs client queue-url) "VisibilityTimeout")))
        (is (= "2" (get (band/queue-attrs client (:queue-url dead-letter)) "VisibilityTimeout")))))

    (testing "pre-existing queue, does not change attributes"
      (let [{:keys [client queue-url dead-letter]}
            (sqs/mk-connection queue
                               :dead-letter dl-queue
                               :queue-attributes {"VisibilityTimeout" "3"}
                               :dead-letter-queue-attributes {"VisibilityTimeout" "4"})]
        (is (= "1" (get (band/queue-attrs client queue-url) "VisibilityTimeout")))
        (is (= "2" (get (band/queue-attrs client (:queue-url dead-letter)) "VisibilityTimeout")))))))

(deftest ^:integration test-set-queue-attributes
  (with-temporary-queue
    [queue]
    (testing "set attributes on queue"
      (let [{:keys [client queue-url] :as conn}
            (sqs/mk-connection queue
                               :queue-attributes {"VisibilityTimeout" "9"})]
        (is (= "9" (get (band/queue-attrs client queue-url) "VisibilityTimeout")))

        (sqs/set-queue-attributes conn :queue-attributes {"VisibilityTimeout" "10"})
        (is (= "10" (get (band/queue-attrs client queue-url) "VisibilityTimeout"))))))

  (with-temporary-queue
    [queue dl-queue]
    (testing "set attributes on queue and dead-letter queue"
      (let [{:keys [client queue-url] :as conn}
            (sqs/mk-connection queue
                               :dead-letter dl-queue
                               :queue-attributes {"VisibilityTimeout" "42"}
                               :dead-letter-queue-attributes {"VisibilityTimeout" "43"})]
        (is (= "42" (get (band/queue-attrs client queue-url) "VisibilityTimeout")))
        (is (= "43" (get (band/queue-attrs client dl-queue) "VisibilityTimeout")))
        (sqs/set-queue-attributes conn
                                  :queue-attributes {"VisibilityTimeout" "44"}
                                  :dead-letter-queue-attributes {"VisibilityTimeout" "45"})
        (is (= "44" (get (band/queue-attrs client queue-url) "VisibilityTimeout")))
        (is (= "45" (get (band/queue-attrs client dl-queue) "VisibilityTimeout"))))))

  (with-temporary-queue
    [queue dl-queue]
    (testing "set attributes on queue and dead-letter queue adds a redrive policy
             if it did not exist"
      (let [;; create each queue separately
            _ (sqs/mk-connection dl-queue)
            _ (sqs/mk-connection queue)

            ;; establish a connection with dl-queue where both queues exist prior
            {:keys [client queue-url] :as conn}
            (sqs/mk-connection queue :dead-letter dl-queue)]
        (is (nil? (get (band/queue-attrs client queue) "RedrivePolicy")))
        (sqs/set-queue-attributes conn)
        (is (= {"maxReceiveCount"     3
                "deadLetterTargetArn" (get (band/queue-attrs client dl-queue) "QueueArn")}
               (json/decode
                 (get (band/queue-attrs client queue) "RedrivePolicy"))))))))

(deftest ^:integration test-multiple-formats
  (with-temporary-queue
    [queue-name]
    (let [connection-1 (sqs/mk-connection queue-name)
          test-object {:entry1 "this is entry 1"
                       :entry2 ["this" "is" "entry" "2"]}]
      (testing "Write and read json"
        (let [json-object (json/generate-string test-object)]
          (sqs/enqueue connection-1 json-object)
          (let [msg (dequeue-1 connection-1)
                body (:body msg)
                parsed-body (json/parse-string body)]
            (is (= json-object parsed-body)))))
      (testing "Write and read plain text"
        (let [test-string "this is a test string"]
          (sqs/enqueue connection-1 test-string)
          (let [msg (dequeue-1 connection-1)
                body (read-string (:body msg))]
            (is (= test-string body))))))))

(deftest ^:integration test-queue-operations
  (with-temporary-queue
    [queue-name]
    (binding [sqs/auto-retry-seconds 5]
      (let [connection-1 (sqs/mk-connection queue-name)
            connection-2 (sqs/mk-connection queue-name)
            messages (atom #{:what-a-message! {:this "is" 1 {"demo" ["map", "eh"]}}})]
        (testing "Write to the queue"
          ; smoketest, no actual assertions
          (doseq [msg @messages]
            (sqs/enqueue connection-1 msg)))
        (testing "Read from the queue and acknowledge it."
          (let [msg (dequeue-1 connection-2)]
            (is (contains? @messages (edn/read-string (:body msg))))
            ;; Acknowledge it.
            (sqs/ack connection-2 msg)
            ;; Remove it from messages atom, for use in future assertions.
            (swap! messages disj (edn/read-string (:body msg)))))
        (let [other-message (first @messages)]
          (let [msg (dequeue-1 connection-2)]
            (testing "Read other message from the queue."
              (is (= other-message (edn/read-string (:body msg)))))
            ;; Put it back.
            (sqs/nack connection-2 msg))
          (let [msg (dequeue-1 connection-2)]
            (testing "Read same message a second time, after nack-ing."
              (is (= other-message (edn/read-string (:body msg)))))
            ;; Don't put it back, just sleep so that it times out.
            (Thread/sleep 7000)) ; 7s > auto-retry-seconds
          (let [msg (dequeue-1 connection-2)]
            (testing "If we don't ack a message (it times out), we get it again!"
              (is (= other-message (edn/read-string (:body msg)))))
            (sqs/ack connection-2 msg)))
        ;; Queue is now empty
        (testing "Reading an empty queue times out and returns empty []."
          (is (empty? (sqs/dequeue connection-2))))
        (testing " Just once more, write and read with message attributes and non-default serialization."
          (let [connection-3 (sqs/mk-connection queue-name)
                input-message  {:final-msg {:ayy "lmao"}}
                _ (sqs/enqueue connection-1 input-message
                               :message-attributes {:some-attribute "some-value"}
                               :serialization-fn json/generate-string)
                msg (dequeue-1 connection-3)]
            (is (= (json/generate-string input-message) (:body msg)))
            (is (= "some-value" (:some-attribute (:message-attributes msg))))
            (sqs/ack connection-3 msg)))))))

(deftest ^:integration test-dead-letter-redrive
  (with-temporary-queue
    [queue-name dlq-name]
    (binding [sqs/maximum-retries 3]
      (let [q-connection (sqs/mk-connection queue-name :dead-letter dlq-name)
            dlq-connection (sqs/mk-connection dlq-name)]
        ;; exhaust maximum receives
        (sqs/enqueue q-connection :potato)
        (sqs/nack q-connection (dequeue-1 q-connection)) ; pop and push 1
        (sqs/nack q-connection (dequeue-1 q-connection)) ; pop and push 2
        (testing "DLQ is empty before a message has exhausted maximum receives."
          (is (empty? (sqs/dequeue dlq-connection))))
        (sqs/nack q-connection (dequeue-1 q-connection)) ; pop and push 3
        (testing "After maximum receives, the message no longer appears on the main queue"
          (is (empty? (sqs/dequeue q-connection))))
        (testing "After maximum receives, the message appears on the dead-letter queue"
          (let [msg (dequeue-1 dlq-connection)]
            (is (= :potato (edn/read-string (:body msg))))
            (when msg (sqs/ack dlq-connection msg))))))))

(deftest ^:integration test-client-supplied-mk-connection
  (with-temporary-queue
    [queue-name]
    (testing "supplying the client to use to make the connection utilizes my supplied client"
      (let [my-client (band/create-client)
            result-connection (sqs/mk-connection queue-name :client my-client)]
        (is (= my-client (:client result-connection)))))
    (testing "not supplying the client to use to make the connection creates a new one"
          ;; Really just validating that AmazonSQSClient's equality is identity based.
          (let [my-client (band/create-client)
                result-connection (sqs/mk-connection queue-name)]
            (is (not= my-client (:client result-connection)))))))

(deftest ^:integration test-multi-dequeue
  (with-temporary-queue
    [queue-name]
    (let [connection-1 (sqs/mk-connection queue-name)
          ;; SQS messages are randomly partitioned across brokers behind the API, so..
          ;; 1. We need quite a few messages for some partitions to get at least 10.
          ;; 2. Each dequeue only hits 1 partition, so may not always get 10
          ;; messages, even if there are > 10 in the total queue depth.
          messages (atom (set (range 100)))]
      (testing "Write to the queue"
        ; smoketest, no actual assertions
        (doseq [msg @messages]
          (sqs/enqueue connection-1 msg)))
      (testing "Read from the queue in no more than 20 batches"
        (loop [i 20]
          (let [msgs (sqs/dequeue connection-1)]
            (log/infof "Got %d messages containing: %s" (count msgs) (mapv :body msgs))
            (doseq [msg msgs]
              (is (contains? @messages (edn/read-string (:body msg))))
              ;; Acknowledge it.
              (sqs/ack connection-1 msg)
              ;; Remove it from messages atom, for use in future assertions.
              (swap! messages disj (edn/read-string (:body msg)))))
          ;; Loop until we run out of attempts or we've gotten all expected messages
          (when (and (> i 0) (not-empty @messages))
            (recur (dec i)))))
      ;; Queue is now empty
      (testing "The queue is now empty and we received all messages; the atom is empty."
        (is (empty? @messages))
        (is (empty? (sqs/dequeue connection-1)))))))
