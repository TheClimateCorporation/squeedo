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
(defproject com.climate/squeedo "0.1.2"
  :description "Squeedo: The sexiest message consumer ever (™)"
  :url "http://github.com/TheClimateCorporation/squeedo/"
  :min-lein-version "2.0.0"
  :license {:name "Apache License Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :dependencies [[joda-time "2.9.1"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.reader "0.10.0"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [cheshire/cheshire "5.5.0"]
                 [com.cemerick/bandalore "0.0.6" :exclusions [joda-time]]
                 [org.clojure/core.async "0.2.374"]
                 [com.amazonaws/aws-java-sdk "1.10.49" :exclusions [joda-time]]]

  :profiles {:dev {:dependencies
                   [[lein-marginalia "0.8.0"]
                    [http-kit "2.1.19"]
                    [com.climate/claypoole "1.1.1"]]}}
  :plugins [[lein-marginalia "0.8.0"]]

  :test-selectors {:default #(not-any? % #{:integration :benchmark :manual})
                   :integration :integration
                   :benchmark :benchmark
                   :all (fn [_] true)})
