(ns metabase.engine.routing-test
  (:require
   [clojure.test :refer :all]
   [metabase.engine.routing :as engine.routing]))

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
    (is (false? (engine.routing/details-supported? {:host "localhost" :additional-options "prepareThreshold=0"})))))
