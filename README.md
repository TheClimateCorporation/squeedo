# Squeedo

Squeedo: The sexiest message consumer ever (™)

This library allows you to quickly spin up a message consumer and worker pool using core.async. The message is returned
in its raw format, so it's up to the caller to determine the proper reader for their purposes.

## Inspiration

Squeedo's inspiration came from our continual need to quickly process lots of messages from SQS. We found that the
code to support these processes was quite similar in that it often involved a lot of plumbing of listening to SQS,
pulling messages,  processing them with some kind of threadpool and then acking them back.  The goal was to make this
easier by somehow simply passing a compute function that would handle all of the plumbing and allow us to focus on
the compute logic of what to do with the message.

After several iterations of basing this plumbing code on threadpools, we quickly found that we couldn't get the kind of
throughput we wanted simply by tuning only the number of threads.  We needed something more dynamic, that would adapt
better to the number of cores it ran on and would squeeze every last bit of CPU from our EC2 instances. After reading
this blog post http://martintrojer.github.io/clojure/2013/07/07/coreasync-and-blocking-io/ we were inspired to use
core.async and Squeedo was born.

## Why use Squeedo?

Where Squeedo shines is in its ability to push CPU utilization to the max without managing a threadpool.  It is
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
  (put! done-channel message)))
  
(def consumer (start-consumer "my-sqs-queue" compute))

;;when done listening
(stop-consumer consumer)  
```

The compute function must ack/nack each message so Squeedo knows when you are done working so it can pass your compute
function another message to process.

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

What of the great things about Squeedo is the advanced configuration options that can be used to tune the consumer to
your workflow beyond what the very reasonable defaults do out of the box.

* **:message-channel-size** - the number of messages to prefetch from SQS; default 20 * num-listeners. Prefetching messages
allow us to keep the compute function continuously busy without having to wait for more to be first pulled from the
remote SQS queue. Make sure to set the timeout appropriately when you create the queue.
* **:num-workers** - the number of workers processing messages concurrently.  This controls how many workers actually
process messages at any one time. (defaults to number of CPU's - 1 or 1 if single core). Squeedo works best with 2 or
more CPU's so don't change this unless you feel adventurous.  This is not the amount of work that can be outstanding at
any one time, that is controlled below with :max-concurrent-work.
* **:num-listeners** - the number of listeners polling from SQS. default is (num-workers / 10) since each listener dequeues
up to 10 messages at a time.  If you have a really fast process, you can actually starve the compute function of
messages and thus need more listeners pulling from SQS.
* **:dequeue-limit** - the number of messages to dequeue at a time; default 10
* **:max-concurrent-work** - the maximum number of total messages processed.  This is mainly for async workflows where you
can have work started and waiting for parked IO threads to complete; default num-workers.  This allows you to always
keep the CPU's busy by having data returned by IO ready to be processed.  Its really a memory game at this point.
* **:dl-queue-name** - the dead letter queue to which messages that are failed the maximum number of times will go (will be
created if necessary). Defaults to (str queue-name \"-failed\")

## Additional goodies

Checkout the `com.climate.squeedo.sqs` namespace for extra goodies like connecting to queues, enqueuing and dequeing
messages, and acking and nacking.  Mostly addons to https://github.com/cemerick/bandalore

## Acknowledgments

Shoutouts to [Jeff Melching](https://github.com/jmelching), [Tim Chagnon](https://github.com/tchagnon), and
[Robert Grailer](https://github.com/RobertGrailer) for being major contributors to this project.

## License

Copyright (C) 2015 The Climate Corporation. Distributed under the Apache
License, Version 2.0.  You may not use this library except in compliance with
the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  Unless required by applicable law or agreed
to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied.  See the License for the specific language governing permissions
and limitations under the License.
