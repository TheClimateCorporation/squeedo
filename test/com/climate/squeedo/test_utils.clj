(ns com.climate.squeedo.test-utils
  (:require [clojure.tools.logging :as log]
            [cemerick.bandalore :as bandalore]))

(defmacro with-temporary-queue
  [[queue-name & [dead-letter-queue-name]] & body]
  (let [dead-letter-queue-name (or dead-letter-queue-name
                                   (gensym "dead-letter-queue-name"))]
    `(let [r# (rand-int Integer/MAX_VALUE)
           ~queue-name (format "test_squeedo_%s" r#)
           ~dead-letter-queue-name (format "test_squeedo_dead-letter_%s" r#)]
       (log/infof "Using testing queue %s" ~queue-name)
       (when ~dead-letter-queue-name
         (log/infof "Using testing dead-letter queue %s" ~dead-letter-queue-name))
       (try
         ~@body
         (finally
           ;; Definitely clean up after ourselves!
           (let [client# (bandalore/create-client)
                 queue-url# (bandalore/create-queue client# ~queue-name)]
             (bandalore/delete-queue client# queue-url#)
             (log/infof "Deleted testing queue %s" ~queue-name)
             (when ~dead-letter-queue-name
               (let [dlq-url# (bandalore/create-queue client#
                                                      ~dead-letter-queue-name)]
                 (bandalore/delete-queue client# dlq-url#))
               (log/infof "Deleted testing dead letter queue %s"
                          ~dead-letter-queue-name))))))))
