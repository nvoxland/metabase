(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.invariants-test
  "Tests of the invariants themselves, so a red probe can be blamed on the sandbox and not on the harness: verdict unit
  tests, the cross-check that no probe makes two invariants contradict each other, and the mutation score, which breaks
  the sandbox rewrite in known ways and requires an invariant to notice."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.api.util :as sandbox.api.util]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.queries.grid :as grid]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase-enterprise.sandbox.query-processor.middleware.sandboxing :as sandboxing]
   [metabase.query-processor.middleware.enterprise :as qp.middleware.enterprise]
   [metabase.test :as mt]))

(set! *warn-on-reflection* true)

;;; The detectors have their own tests, so a red probe can be blamed on the sandbox and not on the harness.

(deftest ^:parallel differential-verdict-test
  (let [ctx      {:expect :restricted :hidden-cols #{"PRICE"}}
        expected {:cols ["ID" "NAME" "PRICE"] :rows [[1 "Alpha" nil]]}
        base     {:cols ["ID" "NAME" "PRICE"] :rows [[1 "Alpha" 10] [2 "Beta" 20]]}
        verdict  (fn [user] (invariants/differential ctx user expected base))]
    (testing "a hidden column may be left out or returned empty"
      (is (= :consistent (verdict {:cols ["ID" "NAME"] :rows [[1 "Alpha"]]})))
      (is (= :consistent (verdict {:cols ["ID" "NAME" "PRICE"] :rows [[1 "Alpha" nil]]}))))
    (testing "a hidden column must not carry a value"
      (is (= :hidden-column-values (verdict {:cols ["ID" "NAME" "PRICE"] :rows [[1 "Alpha" 10]]}))))
    (testing "an allowed column that disappears"
      (is (= :missing-columns (verdict {:cols ["ID"] :rows [[1]]}))))
    (testing "a column nobody asked for, or an allowed column twice"
      (is (= :extra-columns (verdict {:cols ["ID" "NAME" "hidden_price"] :rows [[1 "Alpha" 123456]]})))
      (is (= :extra-columns (verdict {:cols ["ID" "NAME" "NAME"] :rows [[1 "Alpha" "Alpha"]]}))))
    (testing "an allowed column under another name"
      (is (= :extra-columns (verdict {:cols ["ID" "NAME_2"] :rows [[1 "Alpha"]]}))))
    (testing "rows"
      (is (= :extra-rows (verdict {:cols ["ID" "NAME"] :rows [[1 "Alpha"] [2 "Beta"]]})))
      (is (= :missing-rows (verdict {:cols ["ID" "NAME"] :rows []}))))
    (testing "a failure of the user's query alone is never a pass by itself"
      (is (= :user-error (verdict {:error "500 boom" :status 500})))
      (is (= :user-error (verdict {:error "400 nope" :status 400})))
      (testing "unless the config declares that rejection"
        (is (= :known-rejection (invariants/differential (assoc ctx :accepted-rejections [#"missing join alias"])
                                                         {:error "500 Column with source :source/implicitly-joinable is missing join alias" :status 500}
                                                         expected base)))
        (is (= :user-error (invariants/differential (assoc ctx :accepted-rejections [#"missing join alias"])
                                                    {:error "500 boom" :status 500} expected base)))))
    (testing "the probe itself is invalid when the admin's run fails too"
      (is (= :rejected (invariants/differential ctx {:error "500 x" :status 500} {:error "500 x" :status 500} base)))
      (is (= :expected-error (invariants/differential ctx {:cols [] :rows []} {:error "500 x" :status 500} base))))
    (testing "vacuous: the probe cannot tell restricted data from the rest"
      (is (= :vacuous (invariants/differential ctx {:cols ["ID"] :rows [[1]]} {:cols ["ID"] :rows [[1]]} {:cols ["ID"] :rows [[1]]})))
      (testing "unless the unrestricted data had values in a hidden column that the user did not get"
        (is (= :consistent (invariants/differential ctx {:cols ["ID"] :rows [[1]]} {:cols ["ID" "PRICE"] :rows [[1 nil]]}
                                                    {:cols ["ID" "PRICE"] :rows [[1 10]]}))))))
  (testing "a cancelled sandbox: the user gets exactly what an admin gets"
    (let [ctx  {:expect :unrestricted :hidden-cols #{}}
          base {:cols ["ID" "NAME"] :rows [[1 "A"] [2 "B"]]}]
      (is (= :consistent (invariants/differential ctx base base base)))
      (is (= :over-restricted (invariants/differential ctx {:cols ["ID" "NAME"] :rows [[1 "A"]]} base base)))
      (is (= :missing-columns (invariants/differential ctx {:cols ["ID"] :rows [[1] [2]]} base base)))
      (is (= :extra-columns (invariants/differential ctx {:cols ["ID" "NAME" "X"] :rows [[1 "A" 1] [2 "B" 2]]} base base)))
      (is (= :over-restricted (invariants/differential ctx {:error "500 boom" :status 500} base base)))))
  (testing "a config whose queries must fail"
    (let [ctx      {:expect :error :hidden-cols #{}}
          expected {:cols ["ID"] :rows [[1]]}
          base     {:cols ["ID"] :rows [[1] [2]]}]
      (is (= :failed-as-expected (invariants/differential ctx {:error "400 x" :status 400} expected base)))
      (is (= :missing-error (invariants/differential ctx expected expected base)))
      (is (= :extra-rows (invariants/differential ctx base expected base))))))

(deftest ^:parallel canary-verdict-test
  (let [ctx      {:allowed #{[:shops 1] [:shops 3]}}
        c        (partial dataset/canary-num :shops)
        response (fn [& rows] {:body {:data {:rows (vec rows)}}})]
    (testing "a hidden row's canary in the user's response is a leak"
      (is (= {:verdict :leak :leaked (sorted-set [:shops 2])}
             (invariants/canary ctx (response [(c 2)]) (response))))
      (is (= {:verdict :leak :leaked (sorted-set [:shops 4])}
             (invariants/canary ctx (response [(dataset/canary-text :shops 4)]) (response)))))
    (testing "an aggregate of allowed canaries can equal a hidden one; it is no leak when the expected result has it too"
      (is (= (c 2) (/ (+ (c 1) (c 1) (c 3)) 3)))
      (is (= {:verdict :clean} (invariants/canary ctx (response [(c 2)]) (response [(c 2)]))))
      (testing "but a hidden row's text canary is a leak even then"
        (is (= :leak (:verdict (invariants/canary ctx (response [(dataset/canary-text :shops 2)]) (response [(c 2)])))))))))

(deftest ^:parallel http-equivalence-test
  (let [executed {:rows [[1 "Alpha" 900000015838]] :body {:data {:cols [{:name "ID" :base_type "type/Integer"} {:name "NAME" :base_type "type/Text"} {:name "SECRET_NUM" :base_type "type/BigInteger"}]}}}]
    (testing "no HTTP transport"
      (is (= {:verdict :n/a} (invariants/http-equivalence nil executed))))
    (testing "the same rows and columns, numbers as JSON gives them"
      (is (= {:verdict :same}
             (invariants/http-equivalence {:rows [[1 "Alpha" 900000015838N]] :body (:body executed)} executed))))
    (testing "other rows, or an error on one side only"
      (is (= :differs (:verdict (invariants/http-equivalence {:rows [[1 "Alpha" 1]] :body (:body executed)} executed))))
      (is (= :differs (:verdict (invariants/http-equivalence {:error "500 x" :status 500} executed))))
      (is (= :same (:verdict (invariants/http-equivalence {:error "500 x" :status 500} {:error "400 y" :status 400})))))))

(deftest ^:parallel meaningful-run-test
  (testing "a run that never compared rows that could differ proves nothing"
    (is (not (invariants/meaningful? (repeat 40 {:outcome :vacuous}))))
    (is (not (invariants/meaningful? (repeat 40 {:outcome :rejected}))))
    (is (not (invariants/meaningful? [])))
    (is (invariants/meaningful? (concat (repeat 12 {:outcome :consistent}) (repeat 28 {:outcome :vacuous}))))))

(deftest ^:synchronized detectors-agree-with-each-other-test
  (testing "No probe makes two detectors contradict each other; a disagreement is a detector bug"
    (mt/test-drivers (harness/drivers)
      (is (empty? (invariants/disagreements (mapcat harness/run-hand-written! [:table-tier :card-hide-price :two-tables])))))))

;;; ------------------------------------------- Mutation score of enforcement ---------------------------------------
;;;
;;; Each mutant breaks the sandbox rewrite in one plausible way. Every mutant must be caught by at least one detector
;;; on at least one probe; a surviving mutant is a blind spot in the detectors.

(def ^:private real-apply-sandboxing
  "The OSS entry point of the rewrite, captured before any mutant replaces it. The preprocess pipeline calls this var,
  not the EE one, so mutants must replace it here."
  qp.middleware.enterprise/apply-sandboxing)

(defn- sandbox-only-stages-without-joins
  "A broken rewrite that skips any query with joins, as if the second pass never ran."
  [query]
  (if (some :joins (:stages query))
    query
    (real-apply-sandboxing query)))

(def ^:private mutants
  "Mutant name -> var -> replacement, applied with `with-redefs-fn`. Each is a way the rewrite could fail that would
  let hidden rows through. Left out on purpose: column projection and the wrapper `:fields` filter, whose breakage
  fails closed with a database error that no detector counts as a catch; and `fix-bad-field-id-refs`, which field
  resolution makes equivalent for every probe here."
  (array-map
   :no-sandboxing          {#'qp.middleware.enterprise/apply-sandboxing identity}
   :nothing-enforced       {#'sandbox.api.util/enforced-sandboxes-for-tables (constantly nil)}
   :attribute-filter-lost  {#'sandboxing/sandbox->parameters (constantly [])}
   :joins-never-sandboxed  {#'qp.middleware.enterprise/apply-sandboxing sandbox-only-stages-without-joins}))

(def ^:private mutant-probes
  "Probe name -> probe fn. A small set that between them exercise every part the mutants break: hand-written probes,
  plus a bare off-table ref that only `fix-bad-field-id-refs` rescues through the explicit join."
  (delay
    (merge (select-keys hand-written/all [:shops-source :orders-implicit-price :orders-explicit-join :items-two-hops
                                          :nested-shops :card-shops])
           {[:orders/shop_id :bare-id :explicit-join-present :price]
            (grid/probe-fn :orders/shop_id :bare-id :explicit-join-present :price)})))

(def ^:private mutant-policies [:table-tier :card-hide-price :card-join])

(defn- invariants-fired [{:keys [differential canary structural sql hygiene]}]
  (cond-> #{}
    (#{:extra-rows :missing-rows :extra-columns :missing-columns :hidden-column-values :over-restricted :user-error} differential)
    (conj :differential)
    (= :leak (:verdict canary))                                  (conj :canary)
    (= :raw-ref (:verdict structural))                           (conj :structural)
    (= :raw-table-in-sql (:verdict sql))                         (conj :sql)
    (= :dirty (:verdict hygiene))                                (conj :hygiene)))

(defn mutation-score!
  "Run the mutant probes under each mutant and config. Returns mutant -> set of detectors that caught it, over all
  configs and probes, plus `:probes` with the per-probe detail."
  []
  (into (sorted-map)
        (for [[mutant-k redefs] mutants]
          [mutant-k
           (let [results (with-redefs-fn redefs
                           (fn []
                             (doall
                              (for [policy-k mutant-policies
                                    r        (harness/do-with-policy!
                                              policy-k
                                              (fn [ctx]
                                                (doall (for [[probe-name probe-fn] @mutant-probes]
                                                         (harness/run-probe ctx probe-name probe-fn)))))]
                                r))))]
             {:caught-by (reduce into #{} (map invariants-fired results))
              :probes    (mapv (fn [r] [(:policy r) (:probe r) (:outcome r)]) results)})])))

(deftest ^:synchronized every-mutant-of-the-rewrite-is-caught-test
  (testing "Each plausible way of breaking the sandbox rewrite is caught by at least one detector"
    (mt/test-drivers #{:h2}
      (doseq [[mutant-k {:keys [caught-by probes]}] (mutation-score!)]
        (testing (name mutant-k)
          (is (seq caught-by) (pr-str probes)))))))
