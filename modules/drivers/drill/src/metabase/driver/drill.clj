(ns metabase.driver.drill
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [honeysql
             [core :as hsql]
             [helpers :as h]]
            [metabase.driver :as driver]
            [metabase.driver.hive-like :as hive-like]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.mbql.util :as mbql.u]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor
             [store :as qp.store]
             [util :as qputil]]
            [metabase.util.honeysql-extensions :as hx]))

(driver/register! :drill, :parent :hive-like)

;;; ------------------------------------------ Custom HoneySQL Clause Impls ------------------------------------------

(def ^:private source-table-alias
  "Default alias for all source tables. (Not for source queries; those still use the default SQL QP alias of `source`.)"
  "t1")

;; use `source-table-alias` for the source Table, e.g. `t1.field` instead of the normal `schema.table.field`
(defmethod sql.qp/->honeysql [:drill (class Field)]
  [driver field]
  (binding [sql.qp/*table-alias* (or sql.qp/*table-alias* source-table-alias)]
    ((get-method sql.qp/->honeysql [:hive-like (class Field)]) driver field)))

(defmethod sql.qp/apply-top-level-clause [:drill :page] [_ _ honeysql-form {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can simply use limit
      (h/limit honeysql-form items)
      ;; if we need to do an offset we have to do nesting to generate a row number and where on that
      (let [over-clause (format "row_number() OVER (%s)"
                                (first (hsql/format (select-keys honeysql-form [:order-by])
                                                    :allow-dashed-names? true
                                                    :quoting :mysql)))]
        (-> (apply h/select (map last (:select honeysql-form)))
            (h/from (h/merge-select honeysql-form [(hsql/raw over-clause) :__rownum__]))
            (h/where [:> :__rownum__ offset])
            (h/limit items))))))

(defmethod sql.qp/apply-top-level-clause [:drill :source-table]
  [driver _ honeysql-form {source-table-id :source-table}]
  (let [{table-name :name, schema :schema} (qp.store/table source-table-id)]
    (h/from honeysql-form [(sql.qp/->honeysql driver (hx/identifier :table schema table-name))
                           (sql.qp/->honeysql driver (hx/identifier :table-alias source-table-alias))])))


;;; ------------------------------------------- Other Driver Method Impls --------------------------------------------

(defn- drill
  "Create a database specification for a Drill cluster. Opts should include
  :drill-connect."
  [{:keys [drill-connect]
    :or {drill-connect "drillbit=localhost"}
    :as opts}]
  (merge {:classname "org.apache.drill.jdbc.Driver" ; must be in classpath
          :subprotocol "drill"
          :subname drill-connect}
         (dissoc opts :drill-connect)))

(defmethod sql-jdbc.conn/connection-details->spec :drill
  [_ details]
  (-> details
      drill
      (sql-jdbc.common/handle-additional-options details)))

(defn- dash-to-underscore [s]
  (when s
    (str/replace s #"-" "_")))

;; we need this because transactions are not supported in Hive 1.2.1
;; bound variables are not supported in Spark SQL (maybe not Hive either, haven't checked)
(defmethod driver/execute-query :drill
  [driver {:keys [database settings], query :native, :as outer-query}]
  (let [query (-> (assoc query
                    :remark (qputil/query->remark outer-query)
                    :query  (if (seq (:params query))
                              (unprepare/unprepare driver (cons (:query query) (:params query)))
                              (:query query))
                    :max-rows (mbql.u/query->max-rows-limit outer-query))
                  (dissoc :params))]
    (sql-jdbc.execute/do-with-try-catch
      (fn []
        (let [db-connection (sql-jdbc.conn/db->pooled-connection-spec database)]
          (hive-like/run-query-without-timezone driver settings db-connection query))))))

(defmethod driver/supports? [:drill :basic-aggregations]              [_ _] true)
(defmethod driver/supports? [:drill :binning]                         [_ _] true)
(defmethod driver/supports? [:drill :expression-aggregations]         [_ _] true)
(defmethod driver/supports? [:drill :expressions]                     [_ _] true)
(defmethod driver/supports? [:drill :native-parameters]               [_ _] true)
(defmethod driver/supports? [:drill :nested-queries]                  [_ _] true)
(defmethod driver/supports? [:drill :standard-deviation-aggregations] [_ _] true)

;; only define an implementation for `:foreign-keys` if none exists already. In test extensions we define an alternate
;; implementation, and we don't want to stomp over that if it was loaded already
(when-not (get (methods driver/supports?) [:drill :foreign-keys])
  (defmethod driver/supports? [:drill :foreign-keys] [_ _] true))

(defmethod sql.qp/quote-style :drill [_] :mysql)
