## 0.3.0-SNAPSHOT (unreleased)

* **BREAKING**
  * queue attributes are only set when `com.climate.squeedo.sqs/mk-connection` creates a new sqs queue
    * if an existing queue is found, queue attributes are not applied (this includes dead-letter/redrive configuration)
    * squeedo will emit a `WARN` log when this occurs
    * allows reading from queues that already exist for consumers without create permissions (#34)
    * new api function, `com.climate.squeedo.sqs/set-queue-attributes`, which allows ad-hoc calls to set attributes
    for those who need it
  * support binary message attributes (#30)
  * remove bandalore as a source dependency

## 0.2.3 (unreleased)

* fix default dead letter queue in SQS consumer when queue is a FIFO queue
* support creating a FIFO queue when connecting to one that does not exist

## 0.2.2 (November 9, 2017)

* support and validate fifo queue names (thanks @lainiewright!)

## 0.2.1 (June 26, 2017)

* consumer middleware for deserialization and exception logging

## 0.2.0 (June 26, 2017)

* adjustable visibility timeout in consumer api via `:nack timeout`
* updated deps: `[org.clojure/core.async "0.3.442"]`
