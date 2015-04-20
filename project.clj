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
(defproject com.climate/squeedo "0.1.0"
  :description "Squeedo: The sexiest message consumer ever (™)"
  :url "http://github.com/TheClimateCorporation/squeedo/"
  :min-lein-version "2.0.0"
  :license {:name "Apache License Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.reader "0.8.3"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.3.0"]
                 [cheshire/cheshire "5.3.1"]
                 [com.cemerick/bandalore "0.0.6"]
                 [org.clojure/core.async "0.1.338.0-5c5012-alpha"]
                 [com.amazonaws/aws-java-sdk "1.9.31"]]

  :profiles {:dev {:dependencies
                   [[lein-marginalia "0.7.1"]
                    [http-kit "2.1.16"]
                    [com.climate/claypoole "0.4.0"]]}}
  :plugins [[lein-marginalia "0.7.1"]]

  :test-selectors {:default #(not-any? % #{:integration :benchmark :manual})
                   :integration :integration
                   :benchmark :benchmark
                   :all (fn [_] true)})
