(defproject metabase/drill-driver "1.0.0"
  :min-lein-version "2.5.0"

  :dependencies
  [
   ;; Exclusions below are all either things that are already part of metabase-core, or provide conflicting
   ;; implementations of things like log4j <-> slf4j, or are part of both hadoop-common and hive-jdbc;
   [org.apache.drill.exec/drill-jdbc-all "1.16.0"       ; Drill JDBC driver
    :exclusions 
    [org.slf4j/log4j-over-slf4j
     org.slf4j/jcl-over-slf4j
     org.slf4j/slf4j-api
    log4j]]]

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "drill.metabase-driver.jar"}})
