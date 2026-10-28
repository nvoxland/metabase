(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.multi-user-test
  "Sandbox enforcement with several sandboxed users at once. Two users with different login attributes, each in their
  own group and sandbox on the same copy of the database (see [[harness/do-with-two-users!]]), take turns: `:rasta`,
  the second user, then each again. Every response is judged against the responding user's own restricted variant,
  so state one user leaves behind that reaches the other shows as a canary, differential or equivalence failure:

  * the processor in-process and through the dataset endpoint, round by round;
  * a saved question both users can run, with query caching on, through the card query endpoint;
  * the scanned entry points (field values, parameter values, chain filters, metadata), which cache per sandbox.

  The policies and second users are [[policies/second-users]]. Results go to the ledger like every other test."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.entry-points :as ep]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.ledger :as ledger]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.queries.formats :as formats]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

(def ^:private rounds
  "How many times each user takes a turn; the second round of each runs after the other user's first."
  2)

(defn- turns
  "`[round ctx]` for each turn: user one, user two, user one, user two, ..."
  [ctxs]
  (for [round (range rounds), ctx ctxs] [round ctx]))

(defn- check!
  [test-k mode round {:keys [user]} {:keys [policy probe outcome] :as result}]
  (ledger/check! test-k
                 (cond-> {:policy policy :mode mode :user user :round round :probe probe}
                   (:transport result) (assoc :transport (:transport result)))
                 outcome
                 (invariants/unexpected-outcome? probe outcome)))

(deftest ^:synchronized two-users-take-turns-test
  (testing "Two sandboxed users with different attributes, alternating, each get their own sandbox's answer"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :two-users
        (doseq [mode     [:production]
                policy-k (keys policies/second-users)
                :when    (mt/dataset dataset/shops (policies/supported? policy-k))]
          (harness/in-mode
           mode
           #(harness/do-with-two-users!
             policy-k
             (fn [ctxs]
               (doseq [[probe-name probe-fn] hand-written/all
                       [round ctx]           (turns ctxs)]
                 (check! :two-users mode round ctx
                         (harness/run-probe (assoc ctx :transport [:http :dataset]) probe-name probe-fn)))))))))))

(defn- do-with-query-caching!
  "Call `thunk` with a root cache config that caches every saved question's results, replacing any root config."
  [thunk]
  (let [existing (t2/select-one :model/CacheConfig :model_id 0 :model "root")]
    (try
      (t2/delete! :model/CacheConfig :model_id 0 :model "root")
      (t2/insert! :model/CacheConfig {:model_id 0 :model "root" :strategy "ttl"
                                      :config   {:multiplier 99999 :min_duration_ms 1}})
      (thunk)
      (finally
        (t2/delete! :model/CacheConfig :model_id 0 :model "root")
        (when existing (t2/insert! :model/CacheConfig (dissoc existing :id)))))))

(deftest ^:synchronized shared-cached-question-test
  (testing "A saved question both sandboxed users run, with its results cached, gives each user their own answer"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :shared-card
        (doseq [mode     [:production]
                policy-k (keys policies/second-users)
                :when    (mt/dataset dataset/shops (policies/supported? policy-k))]
          (harness/in-mode
           mode
           #(harness/do-with-two-users!
             policy-k
             (fn [[{:keys [ids]} :as ctxs]]
               (do-with-query-caching!
                (fn []
                  (let [cached? (atom false)]
                    (doseq [[probe-name probe-fn] hand-written/roles]
                      (mt/with-temp [:model/Card {card-id :id} {:dataset_query (formats/legacy-outer ids (probe-fn ids))}]
                        (doseq [[round ctx] (turns ctxs)
                                :let [result (harness/run-probe (assoc ctx
                                                                       :transport [:http :shared-card]
                                                                       :shared-card-id card-id)
                                                                probe-name probe-fn)]]
                          (when (:cached? result) (reset! cached? true))
                          (check! :shared-card mode round ctx result))))
                    ;; without a single cache hit the test proves nothing about one user's results reaching another
                    (ledger/check! :shared-card {:policy policy-k :mode mode :probe :cache-engaged}
                                   (if @cached? :cache-hit :no-cache-hit) (not @cached?)))))))))))))

(deftest ^:synchronized scanned-entry-points-per-user-test
  (testing "Field values, parameter values, chain filters and metadata, which may be cached per sandbox, give each user only their own sandbox's values"
    (mt/test-drivers #{:h2}
      (ledger/with-ledger :two-users-scanned
        (doseq [policy-k (keys policies/second-users)
                :when    (mt/dataset dataset/shops (policies/supported? policy-k))]
          (harness/in-mode
           :production
           #(harness/do-with-two-users!
             policy-k
             (fn [[{:keys [ids]} :as ctxs]]
               (ep/do-with-dashboard!
                ids
                (fn [dashboard]
                  (doseq [[round {:keys [user] :as ctx}] (turns ctxs)
                          [k {:keys [leaked]}]          (ep/scan! ctx dashboard)]
                    (ledger/check! :two-users-scanned {:policy policy-k :user user :round round :entry-point k}
                                   (if leaked :leak :clean) (boolean leaked)))))))))))))
