(ns metabase.engine.core-test
  "End-to-end through the shared library: these need `engine/target/debug/libmetabase_engine.*`
  (run `cargo build` in `engine/`) or a bundled library."
  (:require
   [clojure.test :refer :all]
   [metabase.engine.core :as engine]
   [metabase.engine.ffi :as engine.ffi]
   [metabase.test :as mt]))

(def ^:private snapshot
  {:database_id     42
   :catalog_version "v1"
   :connector       "postgres"
   :tables          [{:schema  "public"
                      :name    "Orders"
                      :columns [{:name "ID" :type "Int64" :nullable false}
                                {:name "Total" :type "Float64"}
                                {:name "Created At" :type "Timestamp(Microsecond, Some(\"UTC\"))"}]}]})

(defmacro ^:private with-engine
  "Run `body` against a freshly started in-process engine, stopping it afterwards."
  [& body]
  `(mt/with-temporary-setting-values [~'engine-transport :ffi]
     (try
       ~@body
       (finally
         (engine/stop!)))))

(deftest ^:mb/engine-tests library-is-found-test
  (is (some? (engine.ffi/resolve-library-path nil))
      "Build the engine first: cd engine && cargo build"))

(deftest ^:mb/engine-tests version-test
  (with-engine
    (is (re-matches #"\d+\.\d+\.\d+" (engine/version)))
    (is (engine/healthy?))))

(deftest ^:mb/engine-tests plan-and-unparse-test
  (with-engine
    (engine/register-catalog! snapshot)
    (engine/with-session [session {:database_id 42 :remark "Metabase:: userID: 1"}]
      (testing "engine SQL comes back as Postgres SQL with placeholders and a typed schema"
        (let [{:keys [warehouse_sql warehouse schema]}
              (engine/plan-and-unparse session "SELECT \"ID\", \"Total\" FROM \"public\".\"Orders\" WHERE \"Total\" > $1 ORDER BY \"ID\" LIMIT 10")]
          (is (= "postgres" warehouse))
          (is (= (str "-- Metabase:: userID: 1\n"
                      "SELECT \"Orders\".\"ID\", \"Orders\".\"Total\" FROM \"public\".\"Orders\" "
                      "WHERE (\"Orders\".\"Total\" > $1) ORDER BY \"Orders\".\"ID\" ASC NULLS LAST LIMIT 10")
                 warehouse_sql))
          (is (= [{:name "ID" :type "Int64" :nullable false}
                  {:name "Total" :type "Float64" :nullable true}]
                 schema))))
      (testing "unsupported constructs throw with the QP error type and the construct named"
        (let [e (is (thrown? clojure.lang.ExceptionInfo
                             (engine/plan-and-unparse session "SELECT arrow_cast(\"Total\", 'Utf8') FROM \"public\".\"Orders\"")))]
          (is (= :unsupported-feature (:type (ex-data e))))
          (is (= "function arrow_cast" (:construct (ex-data e))))
          (is (= "postgres" (:warehouse (ex-data e))))))
      (testing "a syntax error is an invalid query"
        (let [e (is (thrown? clojure.lang.ExceptionInfo (engine/plan-and-unparse session "SELEC 1")))]
          (is (= :invalid-query (:type (ex-data e)))))))))

(deftest ^:mb/engine-tests missing-catalog-test
  (with-engine
    (let [e (is (thrown? clojure.lang.ExceptionInfo (engine/create-session! {:database_id 12345})))]
      (is (= :bad-configuration (:type (ex-data e)))))))

(deftest ^:mb/engine-tests restart-test
  (with-engine
    (is (engine/healthy?))
    (engine/stop!)
    (testing "the engine starts again on the next call"
      (is (engine/healthy?)))))
