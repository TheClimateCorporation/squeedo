;; The Climate Corporation licenses this file to you under under the Apache
;; License, Version 2.0 (the "License"); you may not use this file except in
;; compliance with the License.  You may obtain a copy of the License at
;;
;;   http://www.apache.org/licenses/LICENSE-2.0
;;
;; See the NOTICE file distributed with this work for additional information
;; regarding copyright ownership.  Unless required by applicable law or agreed
;; to in writing, software distributed under the License is distributed on an
;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
;; or implied.  See the License for the specific language governing permissions
;; and limitations under the License.
(defproject com.climate/squeedo "0.2.2-SNAPSHOT"
  :description "Squeedo: The sexiest message consumer ever (™)"
  :url "http://github.com/TheClimateCorporation/squeedo/"
  :min-lein-version "2.0.0"
  :license {:name "Apache License Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.3.442"]
                 [cheshire "5.7.0"]
                 [com.cemerick/bandalore "0.0.6"
                  :exclusions [com.amazonaws/aws-java-sdk]]
                 [com.amazonaws/aws-java-sdk-sqs "1.11.98"]]

  :profiles {:dev {:dependencies
                   [[http-kit "2.2.0"]
                    [com.climate/claypoole "1.1.4"]]}}

  :plugins [[lein-ancient "0.5.5"]]

  :deploy-repositories [["releases" :clojars]]

  :test-selectors {:default #(not-any? % #{:integration :benchmark :manual})
                   :integration :integration
                   :benchmark :benchmark
                   :manual :manual
                   :all (fn [_] true)})
