# Squeedo

Squeedo: The sexiest message consumer ever (™)

This library allows you to quickly spin up a message consumer and worker pool using core.async. The message is returned
in its raw format, so it's up to the caller to determine the proper reader for their purposes.

## Latest version

[![Clojars Project](http://clojars.org/com.climate/squeedo/latest-version.svg)](http://clojars.org/com.climate/squeedo)

## Changes

Version `1.0.0` features breaking changes to queue connection and configuration.
See [CHANGELOG](https://github.com/TheClimateCorporation/squeedo/blob/master/CHANGELOG.md) for release notes.

## Inspiration

Squeedo's inspiration came from our continual need to quickly process lots of messages from SQS. We found that the
code to support these processes was quite similar in that it often involved a lot of plumbing of listening to SQS,
pulling messages, processing them with some kind of threadpool and then acking them back. The goal was to make this
easier by somehow simply passing a compute function that would handle all of the plumbing and allow us to focus on
the compute logic of what to do with the message.

After several iterations of basing this plumbing code on threadpools, we quickly found that we couldn't get the kind of
throughput we wanted simply by tuning only the number of threads. We needed something more dynamic, that would adapt
better to the number of cores it ran on and would squeeze every last bit of CPU from our EC2 instances. After reading
this blog post http://martintrojer.github.io/clojure/2013/07/07/coreasync-and-blocking-io we were inspired to use
core.async and Squeedo was born.

## Why use Squeedo?

Where Squeedo shines is in its ability to push CPU utilization to the max without managing a threadpool. It is
especially good when combined with an non-blocking I/O web client library like http-kit as mentioned in the blog above.

## Simple quick start

In its simplest form, Squeedo is composed of only 2 parts, a compute function and a consumer.

```clojure
(require '[com.climate.squeedo.sqs-consumer :refer [start-consumer stop-consumer]]
         '[clojure.core.async :refer [put!]])

;;the compute function that takes a message and a channel to ack or nack on when done with the message
(defn compute
  [message done-channel]
  (println message)
  ;; never or limit the use of blocking IO calls here, use http-kit for these calls
  (put! done-channel message))

(def consumer (start-consumer "my-sqs-queue" compute))

;;when done listening
(stop-consumer consumer)
```

The compute function must post to the done-channel even for exceptions. This will ack/nack each message. Squeedo
listens to the done-channel to know when work is complete so it can pass your compute function another message to
process.

To nack a message (i.e., send update the visibility timeout for the message in SQS, so that it will be
reporcessed again later, up until the configured MaxReceives for your queue), put the message back on the done-channel
with a :nack key. You can specify a value of true to use the default visibility timeout of 0 (retry this message as
soon as possible) or you can give an integer value in seconds.

```clojure
;; retry as soon as possible
(put! done-channel (assoc message :nack true))
;; retry in about 10 seconds
(put! done-channel (assoc message :nack 10))
```

## http-kit example with non-blocking IO

``` clojure
(require '[com.climate.squeedo.sqs-consumer :refer [start-consumer stop-consumer]]
         '[org.httpkit.client]
         '[clojure.core.async :refer [go >!]])

(defn- eat-some-cpu
  [how-much]
  (reduce + (range 1 how-much)))

(defn- async-get
  [url message channel]
  (org.httpkit.client/get url (fn [r] (go
                                        ; do some more processing with the response
                                        (eat-some-cpu 1000000)
                                        (>! channel message)))))

(defn compute
  [message done-channel]
  ; do something expensive
  (eat-some-cpu 1000000)
  ; do this if you will have I/O
  (async-get "http://google.com" message done-channel))

(def consumer (start-consumer "my-sqs-queue" compute :num-listeners 10 :max-concurrent-work 50))

;;when done listening
;; (stop-consumer consumer)
```

## Usage in Jetty based Ring app

Typical project.clj configuration to setup the servlet context listener hooks:

``` clojure
(defproject com.awesome/microservice "0.0.1"

  :ring
    {:init initialize!
     :destroy destroy!}
```

``` clojure
(defonce consumer (atom {}))

(defn- compute-fn
  [message done-channel]
  ;; do something
  (put! done-channel message))

(defn initialize!
  "Call `initialize!` once to initialize global state before serving. This fn is
   invoked on servlet initialization with zero arguments"
  []
  (swap! consumer
         merge
         (start-consumer queue-name compute-fn :max-concurrent-work 10)))

(defn destroy!
  "Destroy the Jetty context and stop the SQS consumer"
  []
  (stop-consumer @consumer)
  (reset! consumer {}))
```

## Advanced configurations options

One of the great things about Squeedo is the advanced configuration options that can be used to tune the consumer to
your workflow beyond what the very reasonable defaults do out of the box.

* **:message-channel-size** - the number of messages to prefetch from SQS; default 20 * num-listeners. Prefetching messages allow us to keep the compute function continuously busy without having to wait for more to be first pulled from the remote SQS queue. Make sure to set the timeout appropriately when you create the queue.
* **:num-workers** - the number of workers processing messages concurrently. This controls how many workers actually process messages at any one time. (defaults to number of CPU's - 1 or 1 if single core). Squeedo works best with 2 or more CPU's. This is not the amount of work that can be outstanding at any one time, that is controlled below with :max-concurrent-work.
* **:num-listeners** - the number of listeners polling from SQS. default is (num-workers /dequeue-limit) since each listener dequeues up to dequeue-limit messages at a time. If you have a really fast process, you can actually starve the compute function of messages and thus need more listeners pulling from SQS.
* **:dequeue-limit** - the number of messages to dequeue at a time; default 10
* **:max-concurrent-work** - the maximum number of total messages processed concurrently. This is mainly for async workflows where you can have work started and are waiting for parked IO threads to complete; default num-workers. This allows you to always keep the CPU's busy by having data returned by IO ready to be processed. Its really a memory game at this point -- how much data you can buffer that's ready to be processed by your asynchronous http clients.
* **:client** - the SQS client used to start the consumer. By default an SQS client is created internally using instance credentials, but a client can be passed in to be used. This allows you to use a client you may have already created with some specific properties (e.g. manually overridden AWS credentials).
* **:exceptional-poll-delay-ms** - the length of delay when a listener gets an exception while polling SQS (default is 10 seconds).

## Additional goodies

Checkout the `com.climate.squeedo.sqs` namespace for extra goodies like configuring and connecting to queues,
enqueuing and dequeuing messages, and acking and nacking.

Example of setting up a queue with custom attributes and a dead letter queue:

```clojure
(require '[com.climate.squeedo.sqs :as sqs])

(defn initialize-my-queue
  []
  (sqs/configure-queue "my-queue"
                       :queue-attributes {"VisibilityTimeout" "60"}
                       :dead-letter "my-queue-failed"
                       :dead-letter-queue-attributes {"VisibilityTimeout" "120"})
  (sqs/mk-connection "my-queue"))

(def my-queue (initialize-my-queue))
```

## Acknowledgments

* Shoutouts to [Jeff Melching](https://github.com/jmelching), [Tim Chagnon](https://github.com/tchagnon), and
[Robert Grailer](https://github.com/RobertGrailer) for being major contributors to this project.
* Bandalore, which Squeedo originally leveraged as its internal SQS client (https://github.com/cemerick/bandalore).

## License

Copyright (C) 2018 The Climate Corporation. Distributed under the Apache
License, Version 2.0.  You may not use this library except in compliance with
the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  Unless required by applicable law or agreed
to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied.  See the License for the specific language governing permissions
and limitations under the License.
