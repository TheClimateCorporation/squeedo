# squeedo

Squeedo: The sexiest message consumer ever (™)

This library allows you to quickly spin up a message consumer and worker
pool using core.async.  It currently doesn't support changing the buffer sizes 
for workers and messages, but its coming.  The message is returned in its raw format,
so it's up to the user to determine the proper reader for their purposes.

## Usage

    (require '[com.climate.squeedo.sqs-consumer :refer [start-consumer stop-consumer]])

    (def consumer (start-consumer "mosaic-layer-generation-request"
                                  (fn [message done-channel] 
                                    (println message)
                                    (put! done-channel message))))
    ;when done listening
    (stop-consumer consumer)                              
