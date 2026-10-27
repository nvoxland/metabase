(ns metabase-enterprise.sandbox.enforcement-test.queries-test
  "Pure tests of the query builders: the mutation catalogue rewrites refs as documented, and every grid position uses the ref."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.queries.generated :as generated]
   [metabase-enterprise.sandbox.enforcement-test.queries.grid :as grid]))

(set! *warn-on-reflection* true)

(deftest ^:parallel mutation-kinds-rewrite-refs-test
  (let [f  (partial dataset/fid generated/fake-ids)
        q  {:database 1
            :type     :query
            :query    {:source-table (dataset/tid generated/fake-ids :orders)
                       :joins        [{:source-table (dataset/tid generated/fake-ids :shops) :alias "S"
                                       :condition    [:= [:field (f :orders :shop_id) nil] [:field (f :shops :id) {:join-alias "S"}]]
                                       :fields       :none}]
                       :fields       [[:field (f :orders :id) nil]
                                      [:field (f :shops :price) {:source-field (f :orders :shop_id)}]
                                      [:field (f :shops :secret) {:join-alias "S"}]]}}
        at (fn [kind k] (generated/apply-mutations generated/fake-ids q [[kind k]]))]
    (testing "SHOPS refs come first in walk order: the join condition's SHOPS.ID, then PRICE, then SECRET"
      (is (= [:field "ID" {:base-type :type/Integer}]
             (get-in (at :id->name 0) [:query :joins 0 :condition 2])))
      (is (= [:field "PRICE" {:base-type :type/Integer}]
             (get-in (at :id->name 1) [:query :fields 1])))
      (is (= [:field "SECRET" {:base-type :type/Text}]
             (get-in (at :id->name 2) [:query :fields 2])))
      (is (= [:field "SECRET" {}]
             (get-in (at :id->name-no-base-type 2) [:query :fields 2]))))
    (testing "source-field variants"
      (is (= [:field (f :shops :price) {:source-field (f :orders :shop_id) :source-field-name "SHOP_ID"}]
             (get-in (at :source-field-name 1) [:query :fields 1])))
      (is (= [:field (f :shops :price) {:source-field (f :orders :shop_id) :source-field-name "NOPE"}]
             (get-in (at :source-field-name-bogus 1) [:query :fields 1])))
      (is (= [:fk-> [:field-id (f :orders :shop_id)] [:field-id (f :shops :price)]]
             (get-in (at :fk-> 1) [:query :fields 1])))
      (is (= [:field (f :shops :price) nil]
             (get-in (at :drop-source-field 1) [:query :fields 1])))
      (testing "do not apply to a ref without :source-field"
        (is (= q (at :fk-> 2)))))
    (testing "join-alias variants"
      (is (= [:joined-field "S" [:field-id (f :shops :secret)]]
             (get-in (at :joined-field 2) [:query :fields 2])))
      (is (= [:field (f :shops :secret) nil]
             (get-in (at :drop-join-alias 2) [:query :fields 2])))
      (is (= [:field (f :shops :secret) {:join-alias "Nope"}]
             (get-in (at :bogus-join-alias 2) [:query :fields 2]))))
    (testing "wrapping and parameters"
      (is (= [:field-literal "PRICE" :type/Integer]
             (get-in (at :field-literal 1) [:query :fields 1])))
      (is (= [:not-null [:field (f :shops :price) {:source-field (f :orders :shop_id)}]]
             (get-in (at :duplicate-into-filter 1) [:query :filter])))
      (is (= {:source-query (:query q) :fields [[:field (f :shops :price) {:source-field (f :orders :shop_id)}]]}
             (:query (at :outer-stage 1))))
      (is (= [[:field "PRICE" {:base-type :type/Integer}]]
             (get-in (at :outer-stage-name-ref 1) [:query :fields])))
      (is (= [{:type "number/!=" :value [-424242] :target [:dimension [:field (f :shops :price) {:source-field (f :orders :shop_id)}]]}]
             (:parameters (at :parameter 1))))
      (is (= "string/!=" (-> (at :parameter 2) :parameters first :type))))
    (testing "k wraps around the five candidate refs"
      (is (= (at :id->name 1) (at :id->name 6))))))

(deftest ^:parallel grid-positions-use-the-ref-test
  (testing "every grid position puts the ref somewhere, so changing the ref changes the query"
    (doseq [[edge-k e] grid/edges
            position   grid/position-names
            :let       [r1 ((grid/ref-shapes :canonical) generated/fake-ids e :price)
                        r2 ((grid/ref-shapes :bare-id) generated/fake-ids e :price)]]
      (testing (format "%s at %s" edge-k position)
        (is (not= (get (grid/positions generated/fake-ids e :price r1) position)
                  (get (grid/positions generated/fake-ids e :price r2) position)))))))
