(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.policies.generated-test
  "The derivation of a generated policy's specification is itself tested, so a red generated policy can be blamed on
  the sandbox and not on the derivation:

  * the filter predicates agree with the query processor on the base dataset, NULLs included;
  * every hand-written policy that carries a `:spec` derives exactly its hand-written restricted variant, and the
    varied derivation reproduces the hand-written varied datasets;
  * sampled specs are well formed, stably named, and the run's selection is deterministic."
  (:require
   [clojure.set :as set]
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.policies.generated :as generated]
   [metabase.test :as mt]
   [metabase.test.data.interface :as tx]))

(set! *warn-on-reflection* true)

(defn- rows-by-table
  "Table name -> rows of the dataset `d`, a var or a definition."
  [d]
  (into {} (map (juxt :table-name :rows)) (:table-definitions (tx/get-dataset-definition (dataset/definition d)))))

(defn- literal-filters
  "Every single literal filter the generator can produce on `table`'s filter columns: each operator with up to two
  sample values and the absent value."
  [table]
  (for [column (generated/filter-columns table)
        op     (generated/operators-for column)
        value  (generated/sample-values table column op)]
    (cond-> {:column column :op op :via :literal}
      (some? value) (assoc :value value))))

(deftest ^:synchronized filter-semantics-match-metabase-test
  (testing "The rows a derived restricted dataset keeps are the rows the sandbox card's own query returns to an admin"
    (mt/test-drivers (harness/drivers)
      (mt/dataset dataset/shops
        (dataset/ensure-fk-metadata!)
        (doseq [[kind table] [[:card :shops] [:card :regions] [:card-join :regions]]
                f            (literal-filters table)
                :let         [sandboxed (if (= kind :card-join) :shops table)
                              spec      {sandboxed {:kind kind :filters [f] :hidden #{}}}
                              {:keys [rows error]} (harness/execute-as :crowberto (generated/card-query sandboxed (spec sandboxed)))]]
          (testing (pr-str spec)
            (is (nil? error))
            (is (= (sort (map :id (generated/restricted-rows spec sandboxed)))
                   (sort (map first rows))))))))))

(deftest ^:parallel hand-written-variants-are-derivable-test
  (testing "A hand-written policy's spec derives exactly the restricted variant written for it by hand"
    (doseq [[policy-k {:keys [spec restricted]}] (merge policies/all
                                                        (into {} (map (fn [k] [k (policies/policy k)])) (keys policies/attribute-variants)))
            :when spec]
      (testing policy-k
        (is (= (rows-by-table restricted) (rows-by-table (generated/restricted spec)))))))
  (testing "The varied derivation reproduces the hand-written varied datasets"
    (is (= (rows-by-table #'dataset/shops-tier2-varied)
           (rows-by-table (generated/varied (:spec (policies/policy :table-tier))))))
    (is (= (rows-by-table #'dataset/shops-hidden-cols-varied)
           (rows-by-table (generated/varied (:spec (policies/policy :card-hide-price))))))))

(defn- well-formed-problems
  "What is wrong with `spec`, or nil."
  [spec]
  (not-empty
   (for [[table {:keys [kind filters hidden]}] spec
         :let [filter-table (if (= kind :card-join) :regions table)
               attr-columns (into #{} (comp (filter #(= :attribute (:via %))) (map :column)) filters)]
         problem [(when-not (every? (set (generated/filter-columns filter-table)) (map :column filters)) :filter-column)
                  (when (and (#{:table :native} kind) (not-every? #(= [:attribute :=] ((juxt :via :op) %)) filters)) :attribute-only)
                  (when (and (= kind :table) (seq hidden)) :hidden-on-table)
                  (when-not (every? (set (generated/hideable-columns table)) hidden) :not-hideable)
                  (when (contains? hidden :id) :hidden-id)
                  (when (and (= kind :card) (seq (set/intersection hidden attr-columns))) :attribute-column-hidden)
                  (when (not= (count filters) (count (distinct (map :column filters)))) :column-twice)
                  (when (and (empty? filters) (empty? hidden)) :empty)]
         :when problem]
     [table problem])))

(deftest ^:parallel generated-specs-are-well-formed-test
  (doseq [kind generated/kinds
          spec (generated/sample kind 50 7)]
    (testing (pr-str spec)
      (is (nil? (well-formed-problems spec)))
      (is (= (generated/spec-name spec) (generated/spec-name (read-string (pr-str spec)))))))
  (testing "REGIONS columns are hidden too, and only those whose names no other table uses"
    (let [regions-hidden (into #{} (mapcat #(get-in % [:regions :hidden])) (generated/sample :two-tables 200 7))
          other-names    (into #{} (mapcat dataset/columns) [:shops :orders :items])]
      (is (seq regions-hidden))
      (is (empty? (set/intersection regions-hidden other-names)))))
  (testing "different specs, different names"
    (let [specs (mapcat #(generated/sample % 50 7) generated/kinds)]
      (is (= (count (distinct specs)) (count (distinct (map generated/spec-name specs)))))))
  (testing "the run's selection is deterministic and covers every kind"
    (is (= (generated/in-scope) (generated/in-scope)))
    (is (= (set generated/kinds) (set (map generated/spec-kind (generated/in-scope)))))))
