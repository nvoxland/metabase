(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.qp-test
  "Sandbox enforcement in the query processor, the theorem: for every input state handed to the processor as the
  sandboxed user, every invariant holds. An input state is a policy (with its login attributes), a query from one of
  the three sources, a query format and a transport; the processor runs in-process and, with the default transport,
  the same state also goes through `POST /api/dataset`, whose response must equal the in-process result.

  Coverage, as the tests below run it:

  | probes                      | policies                      | modes        | formats | transports            | CI    |
  |-----------------------------|-------------------------------|--------------|---------|-----------------------|-------|
  | hand-written (all)          | every policy                  | production   | legacy  | in-process + dataset  | yes   |
  | hand-written (all)          | table-tier, card-hide-price   | test         | legacy  | in-process + dataset  | yes   |
  | parameters                  | every policy                  | production   | legacy  | + dataset, + dashcard | yes   |
  | hand-written (all)          | every attribute variant       | production   | legacy  | in-process + dataset  | yes   |
  | authored                    | every policy                  | production   | legacy  | in-process            | yes   |
  | forged model metadata       | row and column sandboxes      | production   | legacy  | in-process + dataset  | yes   |
  | hand-written, forged card   | the four saved-question ones  | production   | legacy  | in-process            | yes   |
  | grid, CI slice              | table-tier, card-hide-price   | production   | legacy  | in-process + dataset  | yes   |
  | generated, 40 per policy    | table-tier, card-hide-price, two-tables | production | any | in-process + dataset | yes |
  | hand-written + grid slice   | table-tier, card-hide-price   | production   | all 3   | in-process            | yes   |
  | hand-written + parameters   | policies with a varied dataset| production   | legacy  | in-process            | yes   |

  `MB_SANDBOX_FULL=true` runs both modes on every row. The drivers are [[harness/drivers]]; the grid beyond its CI
  slice and longer generated runs are REPL work (`MB_SANDBOX_GEN_CASES`, `MB_SANDBOX_GEN_SEED`). Tests that expose
  filed findings stay red on purpose until the fixes land."
  (:require
   [clojure.data :as data]
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.ledger :as ledger]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.queries.formats :as formats]
   [metabase-enterprise.sandbox.enforcement-test.queries.generated :as generated]
   [metabase-enterprise.sandbox.enforcement-test.queries.grid :as grid]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.test :as mt]
   [metabase.util :as u]))

(set! *warn-on-reflection* true)

;;; ------------------------------------------------- The test loop ------------------------------------------------

(defn- run-states!
  "Lazy seq of `[mode result]` for every state in the scope: each policy of `policies` (skipping those the driver
  cannot run), in each of `modes`, with every probe of `probes` (name -> fn) in every format and through every
  transport. `run`, when given, replaces the probe loop with `(run ctx)` for runners with their own loop."
  [{:keys [modes policies probes formats transports run]
    :or   {formats [:legacy] transports [[:http :dataset]]}}]
  (for [mode     modes
        policy-k policies
        :when    (mt/dataset dataset/shops (policies/supported? policy-k))
        result   (harness/in-mode
                  mode
                  #(harness/do-with-policy!
                    policy-k
                    (or run
                        (fn [ctx]
                          (doall (for [[probe-name probe-fn] probes, fmt formats, transport transports]
                                   (harness/run-probe (assoc ctx :format fmt :transport transport) probe-name probe-fn)))))))]
    [mode result]))

(defn- state
  "Where a result was judged: the ledger's key for it."
  [mode result]
  (cond-> (select-keys result [:policy :probe :transport :format])
    mode (assoc :mode mode)))

(defn- holds!
  "Report one result to the ledger under `test-k`. It fails the test when it is not in order and the ledger does not
  list it: not failing, and for a hand-written probe not rejected unless invalid on purpose (`rejected-ok?` widens that
  to every probe, for the grid's malformed shapes)."
  [test-k [mode {:keys [probe outcome] :as result}] & {:keys [rejected-ok?]}]
  (ledger/check! test-k (state mode result) outcome
                 (if rejected-ok?
                   (invariants/failing-outcomes outcome)
                   (invariants/unexpected-outcome? probe outcome))))

(defn- grid-slice
  "The CI slice of the grid as probe name -> fn."
  []
  (into {} (for [[edge-k shape-k position-k target :as probe-name] (grid/probe-names grid/ci-selection)
                 :let [f (grid/probe-fn edge-k shape-k position-k target)]
                 :when f]
             [probe-name f])))

;;; --------------------------------------------------- Tests ------------------------------------------------------

(deftest ^:synchronized hand-written-probes-hold-test
  (mt/test-drivers (harness/drivers)
    (ledger/with-ledger :hand-written
      (doseq [state (run-states! (assoc (policies/scope :ci) :probes hand-written/all))]
        (holds! :hand-written state)))))

(deftest ^:synchronized representative-policies-hold-with-schema-checks-test
  (testing "With schema checks on, as in a dev or test JVM, the same states hold under one row and one column sandbox"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :schema-checks
        (doseq [state (run-states! (assoc (policies/scope :test-mode) :probes hand-written/all))]
          (holds! :schema-checks state))))))

(deftest ^:synchronized parameters-hold-test
  (testing "A parameter whose target reaches SHOPS through an FK is filtered by the sandbox, in-process, through the dataset API and through a dashcard"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :parameters
        (doseq [state (run-states! (assoc (policies/scope :ci)
                                          :probes     hand-written/parameters
                                          :transports [[:http :dataset] [:http :dashcard]]))]
          (holds! :parameters state))))))

(deftest ^:synchronized attribute-variants-hold-test
  (testing "Every login attribute value either filters the sandboxed table or fails the query"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :attributes
        (doseq [state (run-states! (assoc (policies/scope :attributes)
                                          ;; the bogus-join-alias probes are covered by the hand-written test
                                          :probes (apply dissoc hand-written/all (keys hand-written/bogus-join-aliases))))]
          (holds! :attributes state))))))

(deftest ^:synchronized authored-content-holds-test
  (testing "Models with metadata the sandboxed user wrote, their own metrics, and admin segments are sandboxed"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :authored
        (doseq [[mode policy-k] (for [mode (:modes (policies/scope :ci)), policy-k (:policies (policies/scope :ci))] [mode policy-k])
                :when (mt/dataset dataset/shops (policies/supported? policy-k))
                result (harness/in-mode mode #(harness/run-authored! policy-k))]
          (holds! :authored [mode result]))))))

(deftest ^:synchronized forged-model-metadata-holds-test
  (testing "Models whose result metadata the sandboxed user forged, with name keys that disagree with each other and with the query, reach nothing the sandbox hides"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :forged-metadata
        (doseq [mode     (:modes (policies/scope :authored))
                policy-k (:policies (policies/scope :authored))
                :when    (mt/dataset dataset/shops (policies/supported? policy-k))
                result   (harness/in-mode mode #(harness/run-forged-metadata! policy-k))]
          (holds! :forged-metadata [mode result]))))))

(deftest ^:synchronized sandbox-card-metadata-is-not-trusted-test
  (testing "Rewriting a sandbox card's stored metadata, as if the guard on non-admin edits were bypassed, never widens the sandbox"
    (mt/test-drivers (harness/drivers)
      (doseq [mode      (:modes (policies/scope :card-policies))
              policy-k  (:policies (policies/scope :card-policies))
              forgery-k (keys harness/sandbox-card-forgeries)
              :when     (mt/dataset dataset/shops (policies/supported? policy-k))
              {:keys [probe outcome] :as result} (harness/in-mode mode #(harness/run-with-forged-sandbox-card! policy-k forgery-k))]
        (testing (format "%s under %s with its card's metadata forged by %s, %s mode" (pr-str probe) policy-k forgery-k mode)
          ;; a broken policy may fail the query; it must not leak
          (is (not (invariants/leak-outcomes outcome))
              (pr-str (select-keys result [:outcome :differential :canary :structural :sql :user :expected :compiled]))))))))

(deftest ^:synchronized grid-slice-holds-test
  (testing "Every ref shape in the positions that take different code paths cannot escape the sandbox"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :grid
        (doseq [state (run-states! (assoc (policies/scope :test-mode) :modes (:modes (policies/scope :ci)) :probes (grid-slice)))]
          ;; the grid's shapes are malformed on purpose, so a rejection of both users' queries is in order
          (holds! :grid state :rejected-ok? true))))))

(deftest ^:synchronized generated-queries-hold-test
  (mt/test-drivers (harness/drivers)
    (ledger/with-ledger :generated
      (doseq [mode     (:modes (policies/scope :ci))
              policy-k [:table-tier :card-hide-price :two-tables]]
        (testing (format "%s in %s mode" policy-k mode)
          (let [{:keys [outcomes]} (harness/in-mode mode #(harness/run-generated! policy-k (generated/cases-per-policy)
                                                                                  (generated/seed) [:http :dataset]))]
            ;; every case the property ran, shrinking included: with a fixed seed the walk is deterministic
            (doseq [{:keys [outcome case]} outcomes]
              (ledger/check! :generated {:policy policy-k :mode mode :probe case} outcome
                             (invariants/failing-outcomes outcome)))
            (ledger/check! :generated {:policy policy-k :mode mode :probe :run}
                           (if (invariants/meaningful? outcomes) :meaningful :not-meaningful)
                           (not (invariants/meaningful? outcomes)))))))))

(deftest ^:synchronized query-formats-hold-alike-test
  (testing "A probe gets the same outcome sent as legacy MBQL, as MBQL 5, and as MBQL 5 with legacy-ordered refs"
    (mt/test-drivers (harness/drivers)
      (doseq [[probe results] (->> (run-states! (assoc (policies/scope :test-mode)
                                                       :modes      (:modes (policies/scope :ci))
                                                       :probes     (merge hand-written/all (grid-slice))
                                                       :formats    (keys formats/query-formats)
                                                       :transports [:in-process]))
                                   (map second)
                                   (group-by (juxt :policy :probe)))]
        (testing (pr-str probe)
          (is (= 1 (count (into #{} (map :outcome) results)))
              (pr-str (map (juxt :format :outcome) results))))))))

(deftest ^:synchronized hidden-data-does-not-change-sandboxed-results-test
  (testing "Changing only the rows or columns a sandbox hides leaves every sandboxed result the same"
    (mt/test-drivers (harness/drivers)
      (doseq [mode               (:modes (policies/scope :ci))
              [policy-k policy]  policies/all
              :when              (and (policies/varied-datasets (:restricted policy))
                                      (not (#{:error :unrestricted} (:expect policy)))
                                      (mt/dataset dataset/shops (policies/supported? policy-k)))
              {:keys [probe varied base varied-response]} (harness/in-mode mode #(harness/run-noninterference! policy-k))]
        (testing (format "%s under %s on %s in %s mode" probe policy-k (:name (meta varied)) mode)
          (is (= base varied-response)
              (u/truncate (pr-str (take 2 (data/diff base varied-response))) 2000)))))))

(deftest ^:synchronized responses-are-hygienic-test
  (testing "Nothing in a sandboxed user's API response reveals the sandbox or the raw table: no fingerprints, no native or preprocessed forms, no SQL text"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :hygiene
        (doseq [[mode {:keys [probe policy hygiene]}] (run-states! (assoc (policies/scope :test-mode) :modes [:production] :probes hand-written/all))]
          (ledger/check! :hygiene {:policy policy :mode mode :probe probe} (:verdict hygiene) (not= :clean (:verdict hygiene))))))))

(deftest ^:synchronized forged-internal-stage-keys-do-not-skip-sandbox-test
  (testing "Client-supplied internal stage keys, in legacy and MBQL 5 form, do not make the rewrite skip a stage, in-process or through the API"
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
                                                          extra)]}}
                 [how result] {:in-process (harness/execute-as :rasta query)
                               :http       (harness/run-as :rasta ids {:outer query})}]
           (testing (format "%s as %s, %s" k fmt how)
             (is (nil? (:error result)) (:error result))
             (is (empty? (remove allowed (dataset/canary-ids (:body result))))
                 "the sandboxed user saw a hidden row's canary"))))))))

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
