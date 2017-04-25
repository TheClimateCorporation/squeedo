(ns com.climate.squeedo.sqs-consumer.middleware-test
  (:require
    [clojure.test :refer :all]
    [clojure.tools.logging :as log]
    [com.climate.squeedo.sqs-consumer.middleware :as mw]))

(defn test-consumer
  [x chan]
  (is (= ::chan chan))
  x)

(deftest deserialization-fn
  (testing "applies f to the message body"
    (is (= {:body 2 :other :stuff}
           ((mw/wrap-deserialization-fn test-consumer inc)
            {:body 1 :other :stuff} ::chan)))))

(deftest deserialize-json
  (testing "decodes the message body"
    (is (= {:body {:hello "world"} :other :stuff}
           ((mw/wrap-deserialize-json test-consumer)
            {:body "{\"hello\": \"world\"}" :other :stuff} ::chan)))))

(deftest wrap-time-logger
  (testing "logs the consumer execution time"
    (let [log (atom nil)]
      (with-redefs [log/log* (fn [_ _ _ s] (reset! log s))]
        (is (= ::msg ((mw/wrap-time-logger test-consumer) ::msg ::chan)))
        (is (re-matches #"Compute took \d+ milliseconds" @log))

        (reset! log nil)
        (is (thrown?
              Exception
              ((mw/wrap-time-logger (fn [& _] (throw (Exception.))))
               ::msg ::chan)))
        (is (re-matches #"Compute took \d+ milliseconds" @log))))))
