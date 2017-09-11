## 1.0.0-SNAPSHOT (unreleased)

* support binary message attributes (#30)
* remove bandalore as a source dependency
* **BREAKING**
  * queue attributes are only set via `com.climate.squeedo.sqs/configure-queue`
    * allows reading from queues that already exist for consumers without create
      permissions (#34).
    * `com.climate.squeedo.sqs/configure-queue` will create the specified
      queue (and dead letter queue) if it does not exist.
    * removed attribute options from `com.climate.squeedo.sqs/mk-connection`.
      this function now only makes a connection and returns a reusable connection object.
      a `QueueDoesNotExistException` exception will be thrown if the queue does not exist
      (use `com.climate.squeedo.sqs/configure-queue` to create the queue first).
    * removed dead letter queue option in `com.climate.squeedo.sqs-consumer/start-consumer`.
      use `com.climate.squeedo.sqs/configure-queue` to set up dead letter queue.
    * removed the default behavior of creating a dead letter queue when starting a
      consumer.

## 0.2.1 (June 26, 2017)

* consumer middleware for deserialization and exception logging

## 0.2.0 (June 26, 2017)

* adjustable visibility timeout in consumer api via `:nack timeout`
* updated deps: `[org.clojure/core.async "0.3.442"]`
