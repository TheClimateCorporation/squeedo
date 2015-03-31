# squeedo

Squeedo: The sexiest message consumer ever (™)

This library allows you to quickly spin up a message consumer and worker
pool using core.async. The message is returned in its raw format,
so it's up to the caller to determine the proper reader for their purposes.

## Usage

    (require '[com.climate.squeedo.sqs-consumer :refer [start-consumer stop-consumer]])

    (def consumer (start-consumer "mosaic-layer-generation-request"
                                  (fn [message done-channel] 
                                    (println message)
                                    (put! done-channel message))))
    ;when done listening
    (stop-consumer consumer)  

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
