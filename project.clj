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
(defproject com.climate/squeedo "1.1.3-SNAPSHOT"
  :description "Squeedo: The sexiest message consumer ever (™)"
  :url "http://github.com/TheClimateCorporation/squeedo/"
  :min-lein-version "2.0.0"
  :license {:name "Apache License Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.10.3" :scope "provided"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/core.async "1.5.648"]
                 [cheshire "5.10.1"]
                 [com.amazonaws/aws-java-sdk-sqs "1.12.137"]]

  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :dependencies [[http-kit "2.5.3"]
                                  [com.cemerick/bandalore "0.0.6"
                                   :exclusions [com.amazonaws/aws-java-sdk]]
                                  [com.climate/claypoole "1.1.4"]]}}

  :plugins [[lein-ancient "0.7.0"]]

  :deploy-repositories [["releases" :clojars]]

  :test-selectors {:default #(not-any? % #{:integration :benchmark
                                           :example-cpu :example-listener-threads})
                   :integration :integration
                   :benchmark :benchmark
                   :example-cpu :example-cpu
                   :example-listener-threads :example-listener-threads
                   :all (fn [_] true)})
