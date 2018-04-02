(ns com.climate.squeedo.test-utils
  (:require
    [clojure.tools.logging :as log]
    [cemerick.bandalore :as bandalore]))

(defn generate-queue-name
  []
  (format "test_squeedo_%s_%s"
          (System/currentTimeMillis)
          (rand-int Integer/MAX_VALUE)))

(defn initialize-queue
  [queue-sym]
  (let [queue-name (generate-queue-name)]
    (log/infof "Using testing queue %s for %s" queue-name queue-sym)
    queue-name))

(defn destroy-queue
  [client queue-name]
  (try
    (let [url (bandalore/create-queue client queue-name)]
      (bandalore/delete-queue client url)
      (log/infof "Deleted testing queue %s" queue-name))
    (catch Exception e
      (log/warnf e "Failed to delete testing queue %s" queue-name))))

(defmacro with-temporary-queues
  [queue-syms & body]
  `(let ~(->> (map (fn [q] `(initialize-queue (quote ~q))) queue-syms)
              (map vector queue-syms)
              (apply concat)
              (vec))
     (try
       ~@body
       (finally
         (let [client# (bandalore/create-client)]
           (dorun (map (partial destroy-queue client#) ~queue-syms)))))))
