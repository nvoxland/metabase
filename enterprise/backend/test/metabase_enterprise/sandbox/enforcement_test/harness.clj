(ns metabase-enterprise.sandbox.enforcement-test.harness
  "Set up one policy on a temporary copy of the dataset and run probes under it.

  [[do-with-policy!]] loads the base dataset and the policy's restricted variant, creates the same cards in both,
  installs the sandbox for `:rasta` on a copy of the base database, and calls its fn with a ctx map. [[run-probe]]
  sends one probe as the sandboxed user and as an admin on both databases, applies every invariant, and returns a
  result map. The `run-*!` fns run one probe family under one policy; [[summarize]] and [[repro-report]] read results
  at the REPL. [[modes]] runs a thunk with schema checks on or off."
  (:require
   [clojure.string :as str]
   [clojure.test.check :as tc]
   [clojure.test.check.properties :as prop]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.queries.formats :as formats]
   [metabase-enterprise.sandbox.enforcement-test.queries.generated :as generated]
   [metabase-enterprise.sandbox.enforcement-test.queries.grid :as grid]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase-enterprise.sandbox.test-util :as met]
   [metabase.permissions.models.data-permissions :as data-perms]
   [metabase.sync.core :as sync]
   [metabase.test :as mt]
   [metabase.test.util :as tu]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [metabase.util.malli.fn :as mu.fn]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------- Cards and harness -----------------------------------------------

(defn- do-with-cards!
  "Create the source cards the probes use in the current database and call `(f cards)` with card key -> id."
  [f]
  (let [shops-q  (mt/mbql-query shops)
        orders-q (mt/mbql-query orders {:fields [$id $name $shop_id $shop_id->shops.name $shop_id->shops.price]})]
    (mt/with-temp [:model/Card {shops-card :id}   {:dataset_query shops-q}
                   :model/Card {shops-model :id}  {:dataset_query shops-q :type :model :result_metadata (hand-written/model-metadata shops-q)}
                   :model/Card {orders-card :id}  {:dataset_query orders-q}
                   :model/Card {orders-model :id} {:dataset_query orders-q :type :model :result_metadata (hand-written/model-metadata orders-q)}
                   :model/Card {native-card :id}  {:dataset_query (mt/native-query {:query (format "SELECT * FROM %s"
                                                                                                   (mt/format-name "shops"))})}]
      (f {:native/shops          native-card
          :card/shops            shops-card
          :model/shops           shops-model
          :card/orders-implicit  orders-card
          :model/orders-implicit orders-model}))))

(defn- do-with-maybe-persisted-cards!
  "[[do-with-cards!]], with every model persisted by an admin first when `persist?`. The cache tables hold unsandboxed
  rows, so a sandboxed user's query must never read them."
  [persist? f]
  (if-not persist?
    (do-with-cards! f)
    (mt/with-persistence-enabled! [persist-models!]
      (do-with-cards! (fn [cards]
                        (mt/with-test-user :crowberto
                          (persist-models!))
                        (assert (every? #{"persisted"} (t2/select-fn-vec :state :model/PersistedInfo :database_id (mt/id)))
                                "every model persisted")
                        (f cards))))))

(defn- allowed-canary-ids
  "The canaries in the rows an admin gets from the current (restricted) database. Rows only: response metadata can
  carry values of the whole table (a value remap's list, for one), which would hide a leak."
  []
  (into #{}
        (mapcat (fn [table]
                  (dataset/canary-ids
                   (get-in (mt/user-http-request :crowberto :post 202 "dataset"
                                                 {:database (mt/id) :type "query" :query {:source-table (mt/id table)}})
                           [:data :rows]))))
        [:shops :regions]))

(defonce ^:private fully-synced-dbs (atom #{}))

(defn- fully-synced!
  "Give the current database the fingerprints and field values a real instance has, once per JVM. The dataset loader
  only syncs the schema, and binning needs fingerprints."
  []
  (let [db (mt/db)]
    (when-not (contains? @fully-synced-dbs (:id db))
      (sync/sync-database! db {:scan :full})
      (swap! fully-synced-dbs conj (:id db)))))

(defn- do-with-remaps!
  "Call `thunk` with the config's display remappings in place for the current database: an FK remap
  `[table column target-table target-column]`, or a value remap `[table column {value label}]`."
  [remaps thunk]
  (tu/do-with-column-remappings!
   (into {} (for [[table column target-table target-column] remaps]
              [(mt/id table column) (if (map? target-table) target-table (mt/id target-table target-column))]))
   thunk))

(defn do-with-policy!
  "Load the base dataset and the config's restricted variant, create the cards in both, set up the sandbox on a temp
  copy of the base database as `:rasta`, and call `(f ctx)` inside it. `ctx` has `:ids` for the base copy,
  `:restricted-ids` for the restricted database, `:allowed` canaries, and the config's fields. `base-dataset` (default
  the corpus) is the dataset the sandboxed user queries."
  ([policy-k f]
   (do-with-policy! policy-k #'dataset/shops f))
  ([policy-k base-dataset f]
   (let [{:keys [gtaps attributes restricted setup expect hidden-cols accepted-rejections remaps persist? premium-features]
          :or   {expect :restricted}} (policies/policy policy-k)
         in-sandbox-copy (fn [base restricted-ids allowed]
                           (mt/with-db base
                             (met/with-gtaps! {:gtaps (gtaps) :attributes attributes}
                               (doseq [table dataset/tables]
                                 (data-perms/set-table-permission! &group (mt/id table) :perms/create-queries :query-builder))
                               (data-perms/set-database-permission! &group (mt/id) :perms/download-results :one-million-rows)
                               (when setup (setup))
                               (mt/with-additional-premium-features (or premium-features #{})
                                 (do-with-remaps!
                                  remaps
                                  (fn []
                                    (do-with-maybe-persisted-cards!
                                     persist?
                                     (fn [cards]
                                       (f {:policy              policy-k
                                           :expect              expect
                                           ;; as the driver names the columns in a result
                                           :hidden-cols         (into #{} (map (comp mt/format-name name)) hidden-cols)
                                           :accepted-rejections (or accepted-rejections [])
                                           :sandboxed      (policies/sandboxed-tables policy-k)
                                           :ids            (dataset/ids-map cards)
                                           :restricted-ids restricted-ids
                                           :allowed        allowed})))))))))]
     (mt/dataset (var-get base-dataset)
       (dataset/ensure-fk-metadata!)
       (fully-synced!)
       (let [base (mt/db)]
         (mt/dataset (var-get restricted)
           (dataset/ensure-fk-metadata!)
           (fully-synced!)
           (do-with-remaps! remaps
                            (fn []
                              (do-with-cards!
                               (fn [restricted-cards]
                                 (in-sandbox-copy base (dataset/ids-map restricted-cards) (allowed-canary-ids))))))))))))

;;; ------------------------------------------------ Schema checks -------------------------------------------------

(defn do-without-schema-checks!
  "Call `thunk` with `mu/defn` input and output checks off. Production does not compile those checks in, so a query
  that fails closed on a schema check in a test can fail open in production."
  [thunk]
  ;; the root value, not a binding: API requests run on other threads
  (alter-var-root #'mu.fn/*enforce* (constantly false))
  (try
    (thunk)
    (finally
      (alter-var-root #'mu.fn/*enforce* (constantly true)))))

(def modes
  "Mode name -> fn of a thunk that runs it in that mode."
  (array-map
   :test-mode       (fn [thunk] (thunk))
   :production-mode do-without-schema-checks!))

(defn- query-response->result
  "`:rows`, `:cols` (names) and `:body` of a query response, or `:error`, `:status` and `:body`."
  [{:keys [status body]}]
  (if (and (= 202 status) (= "completed" (:status body)))
    {:rows (get-in body [:data :rows])
     :cols (mapv :name (get-in body [:data :cols]))
     :body body}
    {:error  (u/truncate (str status " " (or (:error body) (:message body) body)) 300)
     :status status
     :body   body}))

(defn- run-as
  "Run a probe (its `:outer` request body, or its legacy inner query and parameters) through the dataset API as `user`
  against `ids`' database. Returns `:rows`, `:cols` (names) and `:body`, or `:error` and `:body`."
  [user ids probe]
  (query-response->result
   (mt/user-http-request-full-response user :post "dataset" (or (:outer probe) (formats/legacy-outer ids probe)))))

(defn- run-saved-as
  "Like [[run-as]], but save the probe's query as a card declaring one parameter per probe parameter, put it on a
  dashboard with each dashboard parameter mapped to the same target, and run it through `via`: `:card` sends the parameters to the
  card query API, `:dashcard` sends their values to the dashcard query API."
  [user via ids {:keys [parameters] :as probe}]
  (let [param-id    (fn [i] (str "p" i))
        card-params (map-indexed (fn [i {:keys [type target]}]
                                   {:id (param-id i) :slug (param-id i) :name (param-id i) :type type :target target})
                                 parameters)]
    (mt/with-temp [:model/Card {card-id :id} {:dataset_query (formats/legacy-outer ids (dissoc probe :parameters))
                                              :parameters    card-params}
                   :model/Dashboard {dash-id :id} {:parameters (map-indexed (fn [i {:keys [type]}]
                                                                              {:id (param-id i) :slug (param-id i) :name (param-id i) :type type})
                                                                            parameters)}
                   :model/DashboardCard {dashcard-id :id} {:dashboard_id       dash-id
                                                           :card_id            card-id
                                                           :parameter_mappings (map-indexed (fn [i {:keys [target]}]
                                                                                              {:parameter_id (param-id i) :card_id card-id :target target})
                                                                                            parameters)}]
      (query-response->result
       (case via
         :card     (mt/user-http-request-full-response
                    user :post (format "card/%d/query" card-id)
                    {:parameters (map-indexed (fn [i param] (assoc param :id (param-id i))) parameters)})
         :dashcard (mt/user-http-request-full-response
                    user :post (format "dashboard/%d/dashcard/%d/card/%d/query" dash-id dashcard-id card-id)
                    {:parameters (map-indexed (fn [i {:keys [type value]}] {:id (param-id i) :type type :value value})
                                              parameters)}))))))

(defn- run-probe*
  "[[run-probe]] with the rendered `probe`."
  [{:keys [ids restricted-ids expect via] :as ctx} probe-name probe-fn probe]
  (let [user        (if via (run-saved-as :rasta via ids probe) (run-as :rasta ids probe))
        admin-base  (run-as :crowberto ids probe)
        expected    (if (= expect :unrestricted)
                      admin-base
                      (run-as :crowberto restricted-ids (probe-fn restricted-ids)))
        verdicts    {:differential  (invariants/differential ctx user expected admin-base)
                     :canary        (invariants/canary ctx user expected)
                     :structural    (invariants/structural ctx probe)
                     :hygiene       (invariants/hygiene ctx user)
                     :metadata-diff (invariants/metadata user expected)}]
    (merge verdicts
           {:policy   (:policy ctx)
            :probe    probe-name
            :outcome  (invariants/outcome verdicts)
            :user     (-> user (dissoc :body) (update :rows #(some->> % (take 5))))
            :expected (-> expected (dissoc :body) (update :rows #(some->> % (take 5))))
            :format   (:format ctx :legacy)
            :via      (:via ctx :dataset)
            :query    probe})))

(defn run-probe
  "Run `probe-fn`, a fn of an ids map returning `{:query inner-query, :parameters ...}`, under `ctx`, sending the
  sandboxed user's query in `(:format ctx)` (default `:legacy`). Returns a result map with every detector's verdict
  and `:outcome`; `:unrenderable` when the probe has no form in that format."
  [{:keys [ids] fmt :format :or {fmt :legacy} :as ctx} probe-name probe-fn]
  (if-let [probe (formats/render fmt ids (probe-fn ids))]
    (run-probe* ctx probe-name probe-fn probe)
    {:policy (:policy ctx) :probe probe-name :format fmt :outcome :unrenderable}))

(defn run-hand-written!
  "Run the hand-written probes (or those named by `probe-names`) under one config, or every config. Returns result maps."
  ([]
   (mapcat run-hand-written! (keys policies/all)))
  ([policy-k]
   (run-hand-written! policy-k (keys hand-written/all)))
  ([policy-k probe-names]
   (do-with-policy!
    policy-k
    (fn [ctx]
      (doall (for [probe-name probe-names]
               (run-probe ctx probe-name (hand-written/all probe-name))))))))

(defn run-hand-written-formats!
  "Run the hand-written probes (or those named by `probe-names`) under `policy-k`, once per query format in `fmts`."
  ([policy-k fmts]
   (run-hand-written-formats! policy-k fmts (keys hand-written/all)))
  ([policy-k fmts probe-names]
   (do-with-policy!
    policy-k
    (fn [ctx]
      (doall (for [fmt fmts, probe-name probe-names]
               (run-probe (assoc ctx :format fmt) probe-name (hand-written/all probe-name))))))))

(def ^:private parameter-paths
  "How the sandboxed user's parameters reach the QP: ad hoc through the dataset API, or applied to a saved card by the
  dashcard query API. The card query API is left out: it accepts only parameters that match a template tag."
  [nil :dashcard])

(defn run-parameters!
  "Run the parameter probes (or those named by `probe-names`) under `policy-k`, once per parameter path. Returns
  result maps."
  ([policy-k]
   (run-parameters! policy-k (keys hand-written/parameters)))
  ([policy-k probe-names]
   (do-with-policy!
    policy-k
    (fn [ctx]
      (doall (for [via parameter-paths, probe-name probe-names]
               (run-probe (assoc ctx :via via) probe-name (hand-written/parameters probe-name))))))))

(defn run-authored!
  "Run the authored-content probes under `policy-k`: the sandboxed user saves the content in the sandboxed database and
  an admin saves the same in the restricted copy. Returns result maps."
  [policy-k]
  (do-with-policy!
   policy-k
   (fn [{:keys [ids restricted-ids] :as ctx}]
     (doall (for [[probe-name {:keys [author probe]}] hand-written/authored]
              (let [created          (author ids :rasta)
                    expected-created (author restricted-ids :crowberto)]
                (try
                  (run-probe ctx probe-name #(probe % (if (= % ids) created expected-created)))
                  (finally
                    (hand-written/delete-authored! created)
                    (hand-written/delete-authored! expected-created)))))))))

(defn run-grid!
  "Run the grid probes named by `probe-names` (default: all) under `policy-k`, or under every config."
  ([] (mapcat #(run-grid! % (grid/probe-names)) (keys policies/all)))
  ([policy-k probe-names]
   (do-with-policy!
    policy-k
    (fn [ctx]
      (doall (for [[edge-k shape-k position-k target :as probe-name] probe-names
                   :let [probe-fn (grid/probe-fn edge-k shape-k position-k target)]
                   :when probe-fn]
               (run-probe ctx probe-name probe-fn)))))))

(defn run-grid-formats!
  "[[run-grid!]] once per query format in `fmts`."
  [policy-k fmts probe-names]
  (do-with-policy!
   policy-k
   (fn [ctx]
     (doall (for [fmt                                              fmts
                  [edge-k shape-k position-k target :as probe-name] probe-names
                  :let                                             [probe-fn (grid/probe-fn edge-k shape-k position-k target)]
                  :when                                            probe-fn]
              (run-probe (assoc ctx :format fmt) probe-name probe-fn))))))

(defn grid-summary
  "Outcome -> count, and the probes per failing outcome."
  [results]
  {:counts   (frequencies (map :outcome results))
   :failing  (->> results
                  (filter (comp invariants/failing-outcomes :outcome))
                  (map (juxt :outcome :probe (comp #(u/truncate (str %) 100) :error :user)))
                  sort
                  vec)})

(defn- run-case!
  "Generate, mutate and run one case under `ctx`. A generator failure is recorded as `:generator-error`, not a
  sandbox failure."
  [{:keys [ids restricted-ids] :as ctx} {:keys [source seed n-steps mutations format] :or {format :legacy} :as case}]
  (let [generated (try (generated/apply-mutations ids (generated/generate-legacy-query ids case) mutations)
                       (catch Throwable e {::generator-error (ex-message e)}))]
    (if-let [error (::generator-error generated)]
      {:outcome :generator-error :case case :error (u/truncate error 300) :policy (:policy ctx)}
      (let [q-base       (select-keys generated [:query :parameters])
            q-restricted (select-keys (generated/translate-query generated ids restricted-ids) [:query :parameters])
            probe-fn     (fn [i] (if (= (:db i) (:db ids)) q-base q-restricted))]
        (assoc (run-probe (assoc ctx :format format) [:generated source seed n-steps mutations format] probe-fn) :case case)))))

(defn run-generated!
  "Run `n` generated cases under `policy-k` with test.check seed `seed`. Returns the quick-check result and every
  case result, including those run while shrinking."
  [policy-k n seed]
  (do-with-policy!
   policy-k
   (fn [ctx]
     (let [outcomes (atom [])
           property (prop/for-all [c generated/gen-case]
                      (let [r (run-case! ctx c)]
                        (swap! outcomes conj r)
                        (not (invariants/failing-outcomes (:outcome r)))))
           ;; the property needs the expensive harness around it, so it cannot be a top-level `defspec`
           result   #_{:clj-kondo/ignore [:discouraged-var]} (tc/quick-check n property :seed seed)]
       {:result result :outcomes @outcomes}))))

(defn- sandboxed-responses!
  "Probe name -> the sandboxed user's masked response to each canonical and parameter probe under `policy-k`, on
  `dataset`."
  [policy-k dataset]
  (do-with-policy!
   policy-k dataset
   (fn [{:keys [ids]}]
     (into {} (for [[probe-name probe-fn] (merge hand-written/all hand-written/parameters)]
                [probe-name (invariants/mask-response (:body (run-as :rasta ids (probe-fn ids))))])))))

(defn run-noninterference!
  "For `policy-k`, each varied dataset paired with its restricted dataset, and each probe, the sandboxed
  user's masked responses on the base and the varied dataset, as `{:config :varied :probe :base :varied-response}`."
  [policy-k]
  (let [base (sandboxed-responses! policy-k #'dataset/shops)]
    (doall (for [varied                (policies/varied-datasets (:restricted (policies/policy policy-k)))
                 :let                  [responses (sandboxed-responses! policy-k varied)]
                 [probe-name response] base]
             {:policy policy-k :varied varied :probe probe-name
              :base response :varied-response (responses probe-name)}))))

(defn summarize
  "Config -> outcome -> count, for reading at the REPL."
  [results]
  (into (sorted-map)
        (map (fn [[config rs]] [config (frequencies (map :outcome rs))]))
        (group-by :policy results)))

(defn repro-report
  "The smallest failing case as a reproduction: its generator inputs, the legacy query as JSON, and every verdict."
  [{:keys [shrunk fail] :as result} outcomes]
  (when-not (:pass? result)
    (let [smallest  (first (or (:smallest shrunk) fail))
          r         (last (filter #(= smallest (:case %)) outcomes))
          ;; a property that threw has no result record; test.check keeps the exception
          exception (some-> (or (:result shrunk) (:result result)) ex-message)]
      (str/join "\n"
                ["Smallest failing case:" (pr-str smallest)
                 "Outcome:" (pr-str (select-keys r [:outcome :differential :canary :structural :hygiene :error]))
                 "Exception:" (pr-str exception)
                 "Legacy query (base copy ids):" (json/encode (:query r))
                 "Sandboxed user got:" (pr-str (:user r))
                 "Expected:" (pr-str (:expected r))]))))

(defn drivers
  "The drivers the enforcement tests run on: every normal driver with nested queries and left joins."
  []
  (mt/normal-drivers-with-feature :nested-queries :left-join))
