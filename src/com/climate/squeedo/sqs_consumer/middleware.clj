(ns com.climate.squeedo.sqs-consumer.middleware
  (:require
    [clojure.tools.logging :as log]
    [cheshire.core :as json]))

(defn wrap-deserialization-fn
  "Deserialize a message body using f"
  [handler f]
  (fn [msg done-chan]
    (handler (update msg :body f) done-chan)))

(defn wrap-deserialize-json
  "Deserialize a JSON encoded message body"
  [handler]
  (wrap-deserialization-fn handler #(json/decode % true)))

(defn wrap-uncaught-exception-logger
  "Log uncaught exceptions that occur during the consumer handler.
  Exceptions are logged and then rethrown."
  ([handler]
   (wrap-uncaught-exception-logger handler "Error thrown by consumer handler"))
  ([handler log-msg]
   (fn [msg done-chan]
     (try
       (handler msg done-chan)
       (catch Throwable t
         (log/error t log-msg)
         (throw t))))))
