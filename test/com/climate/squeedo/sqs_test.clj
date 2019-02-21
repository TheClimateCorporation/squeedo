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
    [com.climate.squeedo.test-utils :refer [generate-queue-name with-temporary-queues]]
    [clojure.tools.reader.edn :as edn]
    [cheshire.core :as json]
    [cemerick.bandalore :as band])
  (:import
    (com.amazonaws.services.sqs.model
      QueueDoesNotExistException)))

(deftest test-parse-queue-name
  (testing "Standard queue name"
    (is (= {:name "foo" :fifo? false}
           (sqs/parse-queue-name "foo"))))
  (testing "Fifo queue name"
    (is (= {:name "foo" :fifo? true}
           (sqs/parse-queue-name "foo.fifo"))))
  (testing "Invalid queue name"
    (is (nil? (sqs/parse-queue-name "fifo.it")))))

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

(deftest test-dequeue
  (let [client {:dummy :client}
        messages [{:message "message1"} {:message "message2"}]
        expected [{:message "message1", :queue-name "queue-name"} {:message "message2", :queue-name "queue-name"}]]
    (testing "params passed through correctly"
      (with-redefs [sqs/receive (fn [sqs-client queue-url & {:keys [limit _ _ attributes message-attribute-names]}]
                                  (are [a b] (= a b)
                                             client sqs-client
                                             "URL" queue-url
                                             2 limit
                                             #{"Attribute"} attributes
                                             #{"AttributeName"} message-attribute-names)
                                  messages)]
        (is (= expected
               (sqs/dequeue {:client     {:dummy :client}
                             :queue-name "queue-name"
                             :queue-url  "URL"}
                            :limit 2
                            :attributes #{"Attribute"}
                            :message-attributes #{"AttributeName"})))))
    (with-redefs [sqs/receive (fn [& _]
                                (throw (Exception. "SomeException")))]
      (testing "dequeue swallows exceptions"
        (is (= []
               (sqs/dequeue {:queue-name "queue-name"}))))
      (testing "dequeue* does not swallow exceptions"
        (is (thrown-with-msg? Exception #"SomeException" (sqs/dequeue* {:queue-name "queue-name"})))))))

(defn dequeue-1
  "Convenience function for some of these tests"
  [connection]
  (first (sqs/dequeue connection :limit 1)))

(deftest ^:integration test-mk-connection
  (with-temporary-queues
    [queue-name]
    (testing "non-existent queue"
      (is (thrown? QueueDoesNotExistException (sqs/mk-connection queue-name))))

    (testing "pre-existing queue - makes a connection"
      (sqs/configure-queue queue-name)
      (let [{:keys [client]} (sqs/mk-connection queue-name)]
        ;; test that we can access the queue's attributes
        (is (-> (band/queue-attrs client queue-name)
                (get "QueueArn")
                (not-empty)))))))

(deftest ^:integration test-configure-queue
  (with-temporary-queues
    [queue-name]
    (testing "set attributes on queue"
      (sqs/configure-queue queue-name :queue-attributes {"VisibilityTimeout" "9"})
      (let [{:keys [client] :as conn} (sqs/mk-connection queue-name)]
        (is (= "9" (get (band/queue-attrs client queue-name) "VisibilityTimeout"))))

      ;; now change the attributes...
      (sqs/configure-queue queue-name :queue-attributes {"VisibilityTimeout" "42"})
      (let [{:keys [client] :as conn} (sqs/mk-connection queue-name)]
        (is (= "42" (get (band/queue-attrs client queue-name) "VisibilityTimeout"))))))

  (testing "configure queue with dead letter queue"
    (with-temporary-queues
      [queue-name dl-queue-name]
      (sqs/configure-queue queue-name :dead-letter dl-queue-name)
      ;; ensure dl-queue is created
      (is (sqs/mk-connection dl-queue-name))
      (let [{:keys [client queue-url] :as conn} (sqs/mk-connection queue-name)]
        (is (= {"maxReceiveCount"     3
                "deadLetterTargetArn" (get (band/queue-attrs client dl-queue-name) "QueueArn")}
               (json/decode
                 (get (band/queue-attrs client queue-name) "RedrivePolicy")))))

      (testing "configure dead letter queue attributes"
        ;; ensure neither queue has value we will assert
        (let [{:keys [client] :as conn} (sqs/mk-connection queue-name)]
          (is (not= "80" (get (band/queue-attrs client queue-name) "VisibilityTimeout"))))
        (let [{:keys [client] :as conn} (sqs/mk-connection dl-queue-name)]
          (is (not= "80" (get (band/queue-attrs client dl-queue-name) "VisibilityTimeout"))))

        (sqs/configure-queue queue-name
                             :dead-letter dl-queue-name
                             :dead-letter-queue-attributes {"VisibilityTimeout" "80"})

        (let [{:keys [client] :as conn} (sqs/mk-connection queue-name)]
          (is (not= "80" (get (band/queue-attrs client queue-name) "VisibilityTimeout"))))
        (let [{:keys [client] :as conn} (sqs/mk-connection dl-queue-name)]
          (is (= "80" (get (band/queue-attrs client dl-queue-name) "VisibilityTimeout"))))))

    (testing "where dead letter queue is an existing queue"
      (with-temporary-queues
        [queue-name dl-queue-name]
        (sqs/configure-queue dl-queue-name) ;; create dl-queue
        (is (sqs/mk-connection dl-queue-name))

        (sqs/configure-queue queue-name :dead-letter dl-queue-name)
        (let [{:keys [client queue-url] :as conn} (sqs/mk-connection queue-name)]
          (is (= {"maxReceiveCount"     3
                  "deadLetterTargetArn" (get (band/queue-attrs client dl-queue-name) "QueueArn")}
                 (json/decode
                   (get (band/queue-attrs client queue-name) "RedrivePolicy")))))))))

(deftest ^:integration test-multiple-formats
  (with-temporary-queues
    [queue-name]
    (sqs/configure-queue queue-name)
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
  (with-temporary-queues
    [queue-name]
    (binding [sqs/auto-retry-seconds 5]
      (sqs/configure-queue queue-name)
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

(deftest ^:integration test-enqueue-fifo
  (testing "fifo queue attributes"
    (let [queue-name (str (generate-queue-name) ".fifo")]
      (try
        (sqs/configure-queue queue-name)
        (let [connection (sqs/mk-connection queue-name)
              _ (sqs/enqueue connection "foo"
                             :message-group-id "1234"
                             :message-deduplication-id "5678")
              msg (dequeue-1 connection)]
          (is (= "1234" (.get (:attributes msg) "MessageGroupId")))
          (is (= "5678" (.get (:attributes msg) "MessageDeduplicationId"))))
        (finally
          (.deleteQueue (:client (sqs/mk-connection queue-name)) queue-name)
          (log/infof "Deleted testing queue %s" queue-name))))))

(deftest ^:integration test-dead-letter-redrive
  (with-temporary-queues
    [queue-name dlq-name]
    (binding [sqs/maximum-retries 3]
      (sqs/configure-queue queue-name :dead-letter dlq-name)
      (let [q-connection (sqs/mk-connection queue-name)
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
  (with-temporary-queues
    [queue-name]
    (sqs/configure-queue queue-name)
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
  (with-temporary-queues
    [queue-name]
    (sqs/configure-queue queue-name)
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
