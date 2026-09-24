(ns metabase.engine.routing-test
  (:require
   [clojure.test :refer :all]
   [metabase.engine.routing :as engine.routing]
   [metabase.lib.test-metadata :as meta]
   [metabase.lib.test-util :as lib.tu]))

(deftest ^:parallel native-query-detection-test
  (let [native? #(#'engine.routing/native-query? % nil)]
    (testing "MBQL queries with no native stage are routable"
      (is (false? (native? {:type :query :query {:source-table 1}})))
      (is (false? (native? {:lib/type :mbql/query :stages [{:lib/type :mbql.stage/mbql :source-table 1}]}))))
    (testing "native at the top level, in a nested source query, in a join, or as an MBQL 5 stage keeps the query on the driver"
      (is (true? (native? {:type :native :native {:query "SELECT 1"}})))
      (is (true? (native? {:type :query :query {:source-query {:native "SELECT 1"} :aggregation [[:count]]}})))
      (is (true? (native? {:type :query :query {:source-table 1 :joins [{:source-query {:native "SELECT 1"} :alias "j"}]}})))
      (is (true? (native? {:lib/type :mbql/query :stages [{:lib/type :mbql.stage/native :native "SELECT 1"}]}))))))

(deftest ^:parallel details-supported-test
  (is (true? (engine.routing/details-supported? {:host "localhost" :ssl true :ssl-mode "require"})))
  (is (true? (engine.routing/details-supported? {:host "localhost" :additional-options ""})))
  (testing "tunnels, custom certificates, and JDBC-only options keep a database on JDBC"
    (is (false? (engine.routing/details-supported? {:host "localhost" :tunnel-enabled true})))
    (is (false? (engine.routing/details-supported? {:host "localhost" :ssl-root-cert {:source "uploaded" :value "..."}})))
    (is (false? (engine.routing/details-supported? {:host "localhost" :ssl-use-client-auth true})))
    (is (false? (engine.routing/details-supported? {:host "localhost" :additional-options "prepareThreshold=0"})))
    (is (false? (engine.routing/details-supported? {:host "localhost" :ssl true :ssl-cert "-----BEGIN CERTIFICATE-----"})))
    (is (false? (engine.routing/details-supported? {:host "localhost" :use-auth-provider true :auth-provider "aws-iam"})))))

(deftest ^:parallel nested-field-column-detection-test
  (let [mp (lib.tu/mock-metadata-provider
            meta/metadata-provider
            {:fields [{:id            1000
                       :table-id      (meta/id :venues)
                       :name          "data → count"
                       :display-name  "data → count"
                       :base-type     :type/Integer
                       :database-type "jsonb"
                       :nfc-path      ["data" "count"]}]})
        nfc? #(#'engine.routing/nested-field-column-query? % mp)]
    (testing "a query touching a JSON path column stays on JDBC"
      (is (true? (nfc? {:type :query :query {:source-table (meta/id :checkins) :fields [[:field 1000 nil]]}})))
      (is (true? (nfc? {:lib/type :mbql/query :stages [{:lib/type :mbql.stage/mbql :source-table (meta/id :checkins)
                                                        :filters [[:= {} [:field {} 1000] 5]]}]}))))
    (testing "so does a query reading a table that has one whole (every field gets added later)"
      (is (true? (nfc? {:type :query :query {:source-table (meta/id :venues)}}))))
    (testing "tables and fields without JSON paths are fine"
      (is (false? (nfc? {:type :query :query {:source-table (meta/id :checkins) :fields [[:field (meta/id :checkins :date) nil]]}}))))))

(deftest ^:parallel standalone-compile-stays-on-the-warehouse-test
  (let [db    {:id 1 :engine :postgres :details {:host "localhost"} :settings {:query-engine "datafusion"}}
        mp    meta/metadata-provider
        query {:type :query :database 1 :query {:source-table (meta/id :checkins)}}]
    (is (= :datafusion (engine.routing/effective-driver query mp db :postgres :postgres)))
    (binding [engine.routing/*standalone-compile* true]
      (is (= :postgres (engine.routing/effective-driver query mp db :postgres :postgres))))))
