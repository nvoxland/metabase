(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.entrypoints-test
  "Sandbox enforcement at the entry points, the hypothesis of the processor theorem: every door the sandboxed user can
  take either leads to the processor with the right context, or is checked on its own. [[ep/entry-points]] classifies
  each one:

  * `:routed` -- hands the processor a context it builds (a card, a dashcard, a download, a pivot). Tested by sending
    a probe through it and requiring the response to equal the in-process result ([[invariants/http-equivalence]]),
    or, where the response is not a result set, by scanning it for canaries and hygiene.
  * `:bypassing` -- reads data or metadata without the processor (field values and search, table and database
    metadata, parameter values, x-rays). Tested by scanning its response for canaries of hidden rows and for hygiene.
  * `:setup` -- produces the enforced sandboxes the processor sees (group combinations, login attribute coercion).
    Tested by the processor tests under those policies; the test here asserts they are in scope.

  [[ep/untested-entry-points]] names the doors this suite does not open, and why. A test keeps the two lists complete
  and disjoint. The base database gets one full sync first, so fingerprints and field values exist as they would in a
  real instance."
  (:require
   [clojure.set :as set]
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.entry-points :as ep]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.ledger :as ledger]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.permissions.core :as perms]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; --------------------------------------------------- Tests ------------------------------------------------------

(deftest ^:parallel every-entry-point-is-classified-or-excused-test
  (testing "the tested and untested lists are disjoint, every tested entry point has a kind and a way to be tested"
    (is (empty? (set/intersection (set (keys ep/entry-points)) (set (keys ep/untested-entry-points)))))
    (doseq [[k {:keys [kind transport request policies]}] ep/entry-points]
      (testing k
        (is (#{:routed :bypassing :setup} kind))
        (is (or transport request policies))))))

(deftest ^:parallel canary-outcome-test
  (let [allowed #{[:shops 1]}
        hidden  (dataset/canary-num :shops 2)]
    (testing "a hidden row's canary as a value"
      (is (= :leak (ep/canary-outcome allowed {:values [[hidden]] :fingerprint {:type {:type/Number {:max hidden}}}}))))
    (testing "only in a fingerprint's numbers"
      (is (= :fingerprint-leak (ep/canary-outcome allowed {:fields [{:fingerprint {:type {:type/Number {:max hidden}}}}]}))))
    (testing "an allowed canary"
      (is (= :clean (ep/canary-outcome allowed {:values [[(dataset/canary-num :shops 1)]]}))))))

(deftest ^:synchronized routed-entry-points-equal-the-processor-test
  (testing "A result-set entry point returns what the processor returns in-process, and passes every invariant"
    (mt/test-drivers (harness/drivers)
      (ledger/with-ledger :routed-entry-points
        (doseq [policy-k ep/scanned-policies
                [k {:keys [transport]}] ep/entry-points
                :when transport
                :let [probes (cond-> hand-written/roles (= k :dashcard-query) (merge hand-written/parameters))]
                {:keys [probe outcome]} (harness/do-with-policy!
                                         policy-k
                                         (fn [ctx]
                                           (doall (for [[probe-name probe-fn] probes]
                                                    (harness/run-probe (assoc ctx :transport transport) probe-name probe-fn)))))]
          (ledger/check! :routed-entry-points {:policy policy-k :entry-point k :probe probe} outcome
                         (invariants/unexpected-outcome? probe outcome)))))))

(deftest ^:synchronized scanned-entry-points-reveal-nothing-outside-the-sandbox-test
  (mt/test-drivers #{:h2}
    (ledger/with-ledger :scanned-entry-points
      (doseq [policy-k ep/scanned-policies
              [k {:keys [canary hygiene]}] (ep/scan-entry-points! policy-k)]
        (ledger/check! :scanned-entry-points {:policy policy-k :entry-point k :check :canary}
                       canary (not= :clean canary))
        (ledger/check! :scanned-entry-points {:policy policy-k :entry-point k :check :hygiene}
                       (:verdict hygiene) (not= :clean (:verdict hygiene)))))))

(deftest ^:parallel setup-entry-points-are-covered-by-the-processor-tests-test
  (testing "every policy a ep/setup entry point names is in the processor tests' CI scope"
    (let [in-scope (set (concat (:policies (policies/scope :ci)) (:policies (policies/scope :attributes))))]
      (doseq [[k {:keys [policies]}] ep/setup]
        (testing k
          (is (every? in-scope policies)))))))

(defn mapped-model-filter-values!
  "Under `policy-k`, the canaries the sandboxed user gets from a dashboard filter on their model column that is mapped
  to SHOPS.SECRET (see `:model-mapped-to-shops`), by filter target and endpoint. Returns `[target endpoint leaked]`,
  where `leaked` is the canary ids outside the sandbox."
  [policy-k]
  (harness/do-with-policy!
   policy-k
   (fn [{:keys [ids allowed]}]
     (let [{:keys [card] :as created} ((:author (hand-written/authored :model-mapped-to-shops)) ids :rasta)]
       (try
         (doall
          (for [[target-k target] {:name-ref ["field" (dataset/fname ids :orders :name) {:base-type "type/Text"}]
                                   :id-ref   ["field" (dataset/fid ids :shops :secret) nil]}
                [endpoint leaked] (mt/with-temp [:model/Dashboard {dash :id} {:parameters [{:id "p" :slug "p" :name "p" :type "string/="}]}
                                                 :model/DashboardCard _ {:dashboard_id       dash
                                                                         :card_id            card
                                                                         :parameter_mappings [{:parameter_id "p" :card_id card
                                                                                               :target       ["dimension" target]}]}]
                                    (doall (for [[endpoint path] {:values (format "dashboard/%d/params/p/values" dash)
                                                                  :search (format "dashboard/%d/params/p/search/CANARY" dash)}]
                                             [endpoint (not-empty (into (sorted-set) (remove allowed)
                                                                        (dataset/canary-ids (mt/user-http-request :rasta :get 200 path))))])))]
            [target-k endpoint leaked]))
         (finally
           (hand-written/delete-authored! created)))))))

(deftest ^:synchronized mapped-model-column-filter-values-are-sandboxed-test
  (testing "A dashboard filter on a model column mapped to a sandboxed column offers only values the sandbox allows"
    (mt/test-drivers #{:h2}
      (doseq [policy-k                  [:table-tier :card-hide-price]
              [target endpoint leaked] (mapped-model-filter-values! policy-k)]
        (testing (format "%s %s under %s" target endpoint policy-k)
          (is (nil? leaked)))))))

(deftest ^:synchronized sandboxed-users-cannot-create-segments-test
  (testing "Segments are admin content, so a sandboxed user cannot define one over the sandboxed table"
    (mt/test-drivers #{:h2}
      (harness/do-with-policy!
       :table-tier
       (fn [{:keys [ids]}]
         (is (= "You don't have permissions to do that."
                (mt/user-http-request :rasta :post 403 "segment"
                                      {:name       "tier 2"
                                       :table_id   (dataset/tid ids :shops)
                                       :definition (assoc-in (hand-written/table-query ids :shops) [:query :filter]
                                                             ["=" ["field" (dataset/fid ids :shops :tier) nil] 2])}))))))))

;;; ------------------------------------------- Edits to a sandbox card -------------------------------------------

(def ^:private sandbox-card-edits
  "Edit name -> `[expected-status body-fn]`, `body-fn` a fn of an ids map returning the `PUT /api/card/:id` body. The
  guard refuses a non-admin's change to what a sandbox reads (`:dataset_query`, `:result_metadata`, `:archived`) on
  any card in the sandbox dependency set. The rest is accepted, `:type` included; the probes then show whether any
  accepted edit changes what the sandbox lets through."
  (let [shops-md #(hand-written/model-metadata (hand-written/table-query % :shops))]
    (array-map
     :result-metadata     [403 (fn [ids] {:result_metadata (shops-md ids)})]
     :dataset-query       [403 (fn [ids] {:dataset_query (hand-written/table-query ids :shops)})]
     :archive             [403 (fn [_] {:archived true})]
     :to-model-with-metadata [403 (fn [ids] {:type "model" :result_metadata (shops-md ids)})]
     :rename              [200 (fn [_] {:name "renamed by a curator"})]
     :move                [200 (fn [_] {:collection_id nil})]
     :to-model            [#{200 400} (fn [_] {:type "model"})]
     :to-metric           [200 (fn [_] {:type "metric"})])))

(defn- do-with-curated-sandbox-card!
  "Put the current SHOPS sandbox card in a collection All Users curate, call `(f card-id)`, then restore the card and
  delete the collection. Setup and teardown run as an admin: the guard would refuse them as the sandboxed user."
  [f]
  (let [card-id (t2/select-one-fn :card_id :model/Sandbox :table_id (mt/id :shops))
        card    (t2/select-one :model/Card :id card-id)
        coll    (mt/with-test-user :crowberto
                  (:id (t2/insert-returning-instance! :model/Collection {:name (str "sandbox questions " (random-uuid))})))]
    (try
      (mt/with-test-user :crowberto
        (t2/update! :model/Card card-id {:collection_id coll})
        ;; a new collection may inherit a grant from its parent; replace it
        (t2/delete! :model/Permissions :group_id (:id (perms/all-users-group)) :collection_id coll)
        (perms/grant-collection-readwrite-permissions! (perms/all-users-group) coll))
      (f card-id)
      (finally
        (mt/with-test-user :crowberto
          (t2/update! :model/Card card-id (select-keys card [:collection_id :archived :type :name]))
          (t2/delete! :model/Collection :id coll))))))

(deftest ^:synchronized sandbox-card-edits-by-a-curator-test
  (testing "A sandboxed user who curates the sandbox card's collection cannot change what the sandbox reads, and no edit they can make widens it"
    (mt/test-drivers #{:h2}
      (doseq [policy-k                          (:policies (ep/setup :sandbox-card-edits))
              [edit-k [expected-status body-fn]] sandbox-card-edits]
        (harness/in-mode
         :production
         #(harness/do-with-policy!
           policy-k
           (fn [{:keys [ids] :as ctx}]
             (do-with-curated-sandbox-card!
              (fn [card-id]
                (let [status (:status (mt/user-http-request-full-response :rasta :put (str "card/" card-id) (body-fn ids)))]
                  (testing (format "%s on the sandbox card under %s" edit-k policy-k)
                    (is ((if (set? expected-status) expected-status #{expected-status}) status))
                    (doseq [[probe-name probe-fn] hand-written/all
                            :let [{:keys [outcome] :as result} (harness/run-probe (assoc ctx :transport :in-process) probe-name probe-fn)]]
                      (testing (format "then %s" probe-name)
                        (is (not (invariants/leak-outcomes outcome))
                            (pr-str (select-keys result [:outcome :differential :canary :user :expected]))))))))))))))))
