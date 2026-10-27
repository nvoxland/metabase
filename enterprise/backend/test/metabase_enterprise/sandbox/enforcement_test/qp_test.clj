(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.qp-test
  "Sandbox enforcement in the query processor: every hand-written, parameter, authored, grid and generated probe, under
  every policy, with schema checks on and off, judged by every invariant. Tests that expose filed findings stay red on
  purpose until the fixes land."
  (:require
   [clojure.data :as data]
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.queries.formats :as formats]
   [metabase-enterprise.sandbox.enforcement-test.queries.generated :as generated]
   [metabase-enterprise.sandbox.enforcement-test.queries.grid :as grid]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.test :as mt]
   [metabase.util :as u]))

(set! *warn-on-reflection* true)

;;; --------------------------------------------------- Tests ------------------------------------------------------

(deftest ^:synchronized canonical-probes-are-enforced-under-every-sandbox-test
  (mt/test-drivers (harness/drivers)
    (doseq [[mode run-in-mode] harness/modes
            policy-k           (keys policies/all)]
      (when (mt/dataset dataset/shops (policies/supported? policy-k))
        (doseq [{:keys [probe outcome] :as result} (run-in-mode #(harness/run-hand-written! policy-k))]
          (testing (format "%s under %s in %s" probe policy-k mode)
            (is (not (invariants/unexpected-outcome? probe outcome))
                (pr-str (select-keys result [:outcome :differential :canary :structural :hygiene :metadata-diff
                                             :user :expected :query])))))))))

(deftest ^:synchronized parameters-that-add-joins-are-sandboxed-test
  (testing "A parameter whose target reaches SHOPS through an FK is filtered by the sandbox like the rest of the query"
    (mt/test-drivers (harness/drivers)
      (doseq [[mode run-in-mode] harness/modes
              policy-k           (keys policies/all)
              :when              (mt/dataset dataset/shops (policies/supported? policy-k))
              {:keys [probe outcome] :as result} (run-in-mode #(harness/run-parameters! policy-k))]
        (testing (format "%s via %s under %s in %s" probe (:via result) policy-k mode)
          (is (not (invariants/unexpected-outcome? probe outcome))
              (pr-str (select-keys result [:outcome :differential :canary :structural :hygiene :metadata-diff
                                           :user :expected :query]))))))))

(deftest ^:synchronized authored-content-is-sandboxed-test
  (testing "Models with metadata the sandboxed user wrote, their own metrics, and admin segments are sandboxed"
    (mt/test-drivers (harness/drivers)
      (doseq [[mode run-in-mode] harness/modes
              policy-k           (keys policies/all)
              :when              (mt/dataset dataset/shops (policies/supported? policy-k))
              {:keys [probe outcome] :as result} (run-in-mode #(harness/run-authored! policy-k))]
        (testing (format "%s under %s in %s" probe policy-k mode)
          (is (not (invariants/unexpected-outcome? probe outcome))
              (pr-str (select-keys result [:outcome :differential :canary :structural :hygiene :metadata-diff
                                           :user :expected :query]))))))))

(deftest ^:synchronized attribute-values-are-enforced-or-rejected-test
  (testing "Every login attribute value either filters the sandboxed table or fails the query, with and without schema checks"
    (mt/test-drivers (harness/drivers)
      (doseq [[mode run-in-mode] harness/modes
              policy-k           (keys policies/attribute-variants)
              :when              (mt/dataset dataset/shops (policies/supported? policy-k))
              ;; the bogus-join-alias probes are covered by the canonical test
              {:keys [probe outcome] :as result} (run-in-mode
                                                  #(harness/run-hand-written! policy-k (remove #{:model-bogus-join-alias :card-bogus-join-alias}
                                                                                               (keys hand-written/all))))]
        (testing (format "%s under %s in %s" probe policy-k mode)
          (is (not (invariants/unexpected-outcome? probe outcome))
              (pr-str (select-keys result [:outcome :differential :canary :user :expected]))))))))

(deftest ^:synchronized saved-sql-results-are-not-sandboxed-as-documented-test
  (testing "A sandboxed user who can see a saved SQL question gets its unsandboxed results, as the docs say"
    (mt/test-drivers (harness/drivers)
      (harness/do-with-policy!
       :table-tier
       (fn [ctx]
         (doseq [[probe-name probe-fn] hand-written/saved-sql
                 :let [{:keys [differential]} (harness/run-probe (assoc ctx :expect :unrestricted) probe-name probe-fn)]]
           (testing probe-name
             (is (= :consistent differential)))))))))

(deftest ^:synchronized sandboxed-user-responses-are-hygienic-test
  (testing "Nothing in a sandboxed user's response reveals the sandbox or the raw table: no fingerprints, no native or preprocessed forms, no SQL text"
    (mt/test-drivers (harness/drivers)
      (doseq [{:keys [probe config hygiene]} (mapcat harness/run-hand-written! [:table-tier :card-hide-price])]
        (testing (format "%s under %s" probe config)
          (is (= :clean (:verdict hygiene)) (pr-str hygiene)))))))

(deftest ^:synchronized forged-internal-stage-keys-do-not-skip-sandbox-test
  (testing "Client-supplied internal stage keys, in legacy and MBQL 5 form, do not make the rewrite skip a stage"
    (mt/test-drivers (harness/drivers)
      (harness/do-with-policy!
       :table-tier
       (fn [{:keys [ids allowed]}]
         (doseq [[k extra-fn] hand-written/forged-stage-keys
                 :let         [extra (extra-fn (dataset/tid ids :shops))
                               field ["field" (dataset/fid ids :shops :secret) nil]]
                 [fmt query]  {:legacy {:database (:db ids)
                                        :type     "query"
                                        :query    (merge {:source-table (dataset/tid ids :shops) :fields [field]} extra)}
                               :mbql5  {:database (:db ids)
                                        :lib/type "mbql/query"
                                        :stages   [(merge {:lib/type     "mbql.stage/mbql"
                                                           :source-table (dataset/tid ids :shops)
                                                           :fields       [["field" {:lib/uuid (str (random-uuid))} (dataset/fid ids :shops :secret)]]}
                                                          extra)]}}]
           (testing (format "%s as %s" k fmt)
             (let [{:keys [body]} (mt/user-http-request-full-response :rasta :post "dataset" query)]
               (is (= "completed" (:status body)) (:error body))
               (is (empty? (remove allowed (dataset/canary-ids body)))
                   "the sandboxed user saw a hidden row's canary")))))))))

(deftest ^:synchronized field-ref-grid-slice-cannot-escape-sandbox-test
  (mt/test-drivers (harness/drivers)
    (doseq [[mode run-in-mode]                  harness/modes
            policy-k                            [:table-tier :card-hide-price]
            {:keys [probe outcome] :as result} (run-in-mode #(harness/run-grid! policy-k (grid/probe-names grid/ci-selection)))]
      (testing (format "%s under %s in %s" (pr-str probe) policy-k mode)
        ;; the grid's shapes are malformed on purpose, so a rejection of both users' queries is in order
        (is (not (invariants/failing-outcomes outcome))
            (pr-str (select-keys result [:outcome :differential :canary :structural :hygiene :user :query])))))))

(deftest ^:synchronized query-formats-are-sandboxed-alike-test
  (testing "A probe gets the same outcome sent as legacy MBQL, as MBQL 5, and as MBQL 5 with legacy-ordered refs"
    (mt/test-drivers (harness/drivers)
      (doseq [[mode run-in-mode] harness/modes
              policy-k           [:table-tier :card-hide-price]
              [probe results]    (group-by :probe
                                           (run-in-mode
                                            #(concat (harness/run-hand-written-formats! policy-k (keys formats/query-formats))
                                                     (when (= policy-k :table-tier)
                                                       (harness/run-grid-formats! policy-k (keys formats/query-formats)
                                                                                  (grid/probe-names grid/ci-selection))))))]
        (testing (format "%s under %s in %s" (pr-str probe) policy-k mode)
          (is (= 1 (count (into #{} (map :outcome) results)))
              (pr-str (map (juxt :format :outcome) results))))))))

(deftest ^:synchronized hidden-data-does-not-change-sandboxed-responses-test
  (testing "Changing only the rows or columns a sandbox hides leaves every sandboxed response the same"
    (mt/test-drivers (harness/drivers)
      (doseq [[mode run-in-mode] harness/modes
              [policy-k config]  policies/all
              :when              (and (policies/varied-datasets (:restricted config))
                                      (not (#{:error :unrestricted} (:expect config)))
                                      (mt/dataset dataset/shops (policies/supported? policy-k)))
              {:keys [probe varied base varied-response]} (run-in-mode #(harness/run-noninterference! policy-k))]
        (testing (format "%s under %s on %s in %s" probe policy-k (:name (meta varied)) mode)
          (is (= base varied-response)
              (u/truncate (pr-str (take 2 (data/diff base varied-response))) 2000)))))))

(deftest ^:synchronized generated-queries-cannot-escape-sandbox-test
  (mt/test-drivers (harness/drivers)
    (doseq [[mode run-in-mode] harness/modes
            policy-k           [:table-tier :card-hide-price :two-tables]]
      (testing (format "%s in %s" policy-k mode)
        (let [{:keys [result outcomes]} (run-in-mode #(harness/run-generated! policy-k (generated/cases-per-policy) (generated/seed)))]
          (is (true? (:pass? result)) (harness/repro-report result outcomes))
          (is (invariants/meaningful? outcomes) (pr-str (frequencies (map :outcome outcomes)))))))))
