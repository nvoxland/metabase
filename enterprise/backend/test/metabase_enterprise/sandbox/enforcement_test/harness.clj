(ns metabase-enterprise.sandbox.enforcement-test.harness
  "Set up one policy on a temporary copy of the dataset and run probes under it.

  [[do-with-policy!]] loads the base dataset and the policy's restricted variant, creates the same cards in both,
  installs the sandbox for `:rasta` on a copy of the base database, and calls its fn with a ctx map:

    {:policy k :expect :restricted|:unrestricted|:error :hidden-cols #{\"PRICE\" ...} :accepted-rejections [..]
     :sandboxed #{:shops} :ids {..} :restricted-ids {..} :allowed #{[:shops 1] ..}
     :restricted-dataset #'dataset/shops-tier1 :remaps [..] :persist? bool
     :hidden-names {\"PRICE\" #{fid ..}} :secrets (dataset/secrets restricted)}

  `:policy` is a key of `policies/all` or a generated spec; `:restricted-dataset` a var or a derived definition.

  [[run-probe]] runs one probe as the sandboxed user **in-process**, handing the query processor exactly what the
  dataset endpoint would, and observes it at three points ([[observe]]): the preprocessed MBQL after alias
  resolution, the compiled SQL, and the executed result. When `(:transport ctx)` is `[:http via]` it also sends the
  probe through that HTTP entry point and the [[invariants/http-equivalence]] invariant compares the two. The
  expected answers are admin runs, cached per query across policies ([[expected-results]]). Every invariant's verdict is
  recorded on the result map:

    {:policy :probe :format :transport :query :outcome
     :differential :canary :error-secrets :structural :sql :hygiene :metadata-diff :http-equivalence
     :user {:rows :cols | :error :status} :expected {..} :compiled \"sql\" :http {:rows :cols | :error :status}}

  The `run-*!` fns run one probe family under one policy; [[summarize]] and [[repro-report]] read results at the
  REPL. [[modes]] runs a thunk with schema checks on (`:test`) or off (`:production`)."
  (:require
   [clojure.string :as str]
   [clojure.test.check :as tc]
   [clojure.test.check.properties :as prop]
   [mb.hawk.parallel]
   [medley.core :as m]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.queries.formats :as formats]
   [metabase-enterprise.sandbox.enforcement-test.queries.generated :as generated]
   [metabase-enterprise.sandbox.enforcement-test.queries.grid :as grid]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase-enterprise.sandbox.test-util :as met]
   [metabase.api.common :as api]
   [metabase.driver :as driver]
   [metabase.lib-be.core :as lib-be]
   [metabase.lib.core :as lib]
   [metabase.permissions.models.data-permissions :as data-perms]
   [metabase.query-processor :as qp]
   [metabase.query-processor.compile :as qp.compile]
   [metabase.query-processor.error-type :as qp.error-type]
   [metabase.query-processor.preprocess :as qp.preprocess]
   [metabase.query-processor.setup :as qp.setup]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.sync.core :as sync]
   [metabase.test :as mt]
   [metabase.test.util :as tu]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [metabase.util.malli.fn :as mu.fn]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; ------------------------------------------- Running the processor ----------------------------------------------

(defn wire
  "`x` as the wire would carry it: a JSON round trip, so numbers, dates and namespaced keys look the same whether a
  result came from the processor in-process or through the API."
  [x]
  (json/decode+kw (json/encode x)))

(defn- error-info [^Throwable e]
  {:message (u/truncate (ex-message e) 300)
   :type    (:type (ex-data e))
   :class   (.getName (class e))})

(defn- status-code
  "The HTTP status the streaming response would give a failed userland result (the API's private rule, copied)."
  [body]
  (cond
    (:status-code body)                                          (:status-code body)
    (-> body :ex-data :status-code)                              (-> body :ex-data :status-code)
    (-> body :error_type qp.error-type/connection-pool-saturated?) 503
    (-> body :error_type qp.error-type/permission-error?)         403
    (-> body :error_type qp.error-type/client-error?)             400
    :else                                                        500))

(defn- endpoint-query
  "`outer` prepared the way `POST /api/dataset` prepares a body before the processor: normalized to MBQL 5 (the
  endpoint's body schema does that, and it matters: a failure carries the preprocessed query, fingerprints included,
  only when the processor was handed a legacy map), the userland middleware with default constraints, and an `:info`
  map naming the source card and, for a model, its stored metadata."
  [outer]
  (let [query          (lib-be/normalize-query outer)
        source-card-id (when (= (lib/normalized-query-type query) :mbql/query)
                         (lib/primary-source-card-id query))
        source-card    (when source-card-id
                         (t2/select-one [:model/Card :type :result_metadata] :id source-card-id))
        info           (cond-> {:executed-by api/*current-user-id*
                                :context     :ad-hoc
                                :card-id     source-card-id}
                         (= (:type source-card) :model)
                         (assoc :metadata/model-metadata (:result_metadata source-card)))]
    (-> query
        (dissoc :cache-strategy)
        qp/userland-query-with-default-constraints
        (assoc :info info))))

(defn- query-response->result
  "`:rows`, `:cols` (names), `:col-ids` (each column's field id, or nil) and `:body` of a query response, or `:error`,
  `:status` and `:body`."
  [{:keys [status body]}]
  (if (and (= 202 status) (= "completed" (:status body)))
    {:rows    (get-in body [:data :rows])
     :cols    (mapv :name (get-in body [:data :cols]))
     :col-ids (mapv :id (get-in body [:data :cols]))
     :body    body}
    {:error  (u/truncate (str status " " (or (:error body) (:message body) body)) 300)
     :status status
     :body   body}))

(defn execute-as
  "Run `outer` through the processor as `user`, in-process, as the dataset endpoint would, and return the result in
  the shape [[query-response->result]] gives an API response: a completed result becomes status 202 with the wire
  body; a failed userland result gets the status the API would send; a throw before the pipeline becomes a 500 (or the
  exception's own status) carrying its message, as the API catches it."
  [user outer]
  (mt/with-test-user user
    (query-response->result
     (try
       (let [body (wire (qp/process-query (endpoint-query outer)))]
         (if (= "completed" (:status body))
           {:status 202 :body body}
           {:status (status-code body) :body body}))
       (catch Throwable e
         {:status (or (:status-code (ex-data e)) 500) :body {:error (ex-message e)}})))))

(defn- preprocess-as
  "The preprocessed MBQL 5 query, after alias resolution, as `user` sees it; or the error."
  [user outer]
  (mt/with-test-user user
    (try
      (qp.setup/with-qp-setup [outer outer]
        {:value (-> outer qp.preprocess/preprocess add/add-alias-info)})
      (catch Throwable e
        {:error (error-info e)}))))

(defn- compile-as
  "The compiled native query as `user`; or the error."
  [user outer]
  (mt/with-test-user user
    (try
      {:value (qp.compile/compile outer)}
      (catch Throwable e
        {:error (error-info e)}))))

(defn observe
  "`outer` as `user` sees it at the three in-process observation points, each taken on its own (a later point is still
  tried when an earlier one failed, since compiling can fail where preprocessing passed):

    {:preprocessed {:value mbql5 | :error {..}}
     :compiled     {:value {:query \"sql\" :params [..]} | :error {..}}
     :executed     {:rows :cols :body | :error :status :body}}

  Each point binds the user afresh, as a request would: the permission caches are per-binding snapshots."
  [user outer]
  {:preprocessed (preprocess-as user outer)
   :compiled     (compile-as user outer)
   :executed     (execute-as user outer)})

;;; ------------------------------------------------ Cards and setup -----------------------------------------------

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
                   (:rows (execute-as :crowberto {:database (mt/id) :type "query" :query {:source-table (mt/id table)}})))))
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
  "Call `thunk` with the policy's display remappings in place for the current database: an FK remap
  `[table column target-table target-column]`, or a value remap `[table column {value label}]`."
  [remaps thunk]
  (tu/do-with-column-remappings!
   (into {} (for [[table column target-table target-column] remaps]
              [(mt/id table column) (if (map? target-table) target-table (mt/id target-table target-column))]))
   thunk))

(defn- policy-ctx
  "The ctx `f` gets for `policy-k`, against the base copy's `cards` and the restricted variant `restricted`."
  [policy-k restricted cards restricted-ids allowed]
  (let [{:keys [expect hidden accepted-rejections remaps persist?] :or {expect :restricted}} (policies/policy policy-k)]
    {:policy              policy-k
     :user                :rasta
     :expect              expect
     ;; as the driver names the columns in a result
     :hidden-cols         (into #{} (for [[_table cols] hidden, c cols] (mt/format-name (name c))))
     ;; in both databases, so a result from either can be judged by field id
     :hidden-field-ids    (into #{} (for [i            [(dataset/ids-map cards) restricted-ids]
                                          [table cols] hidden
                                          c            cols]
                                      (dataset/fid i table c)))
     ;; only names no other table uses: a same-named visible column in an error is not the hidden one
     :hidden-names        (into {} (for [[table cols] hidden
                                         c            cols
                                         :when        (not-any? #(some #{c} (dataset/columns %)) (remove #{table} dataset/tables))]
                                     [(mt/format-name (name c))
                                      (into #{} (map #(dataset/fid % table c)) [(dataset/ids-map cards) restricted-ids])]))
     :secrets             (dataset/secrets restricted)
     :accepted-rejections (or accepted-rejections [])
     :sandboxed           (policies/sandboxed-tables policy-k)
     :ids                 (dataset/ids-map cards)
     :restricted-ids      restricted-ids
     :restricted-dataset  restricted
     :remaps              remaps
     :persist?            (boolean persist?)
     :allowed             allowed}))

(defn- do-with-restricted!
  "Load the restricted variant `restricted`, with the policy's `remaps` and the source cards, and call
  `(f restricted-ids allowed-canaries)` inside it."
  [restricted remaps f]
  (mt/dataset (dataset/definition restricted)
    (dataset/ensure-fk-metadata!)
    (fully-synced!)
    (do-with-remaps! remaps
                     (fn []
                       (do-with-cards!
                        (fn [restricted-cards]
                          (f (dataset/ids-map restricted-cards) (allowed-canary-ids))))))))

(defn- grant-query-access!
  "Let the sandbox `group` query every table of the dataset and download results."
  [group]
  (doseq [table dataset/tables]
    (data-perms/set-table-permission! group (mt/id table) :perms/create-queries :query-builder))
  (data-perms/set-database-permission! group (mt/id) :perms/download-results :one-million-rows))

(defn do-with-policy!
  "Load the base dataset and the policy's restricted variant, create the cards in both, set up the sandbox on a temp
  copy of the base database as `:rasta`, and call `(f ctx)` inside it; see the namespace docstring for `ctx`.
  `base-dataset` (default [[dataset/shops]]) is the dataset the sandboxed user queries."
  ([policy-k f]
   (do-with-policy! policy-k #'dataset/shops f))
  ([policy-k base-dataset f]
   (let [{:keys [gtaps attributes restricted setup remaps persist? premium-features]} (policies/policy policy-k)]
     (mt/dataset (dataset/definition base-dataset)
       (dataset/ensure-fk-metadata!)
       (fully-synced!)
       (let [base (mt/db)]
         (do-with-restricted!
          restricted remaps
          (fn [restricted-ids allowed]
            (mt/with-db base
              (met/with-gtaps! {:gtaps (gtaps) :attributes attributes}
                (grant-query-access! &group)
                (when setup (setup))
                (mt/with-additional-premium-features (or premium-features #{})
                  (do-with-remaps!
                   remaps
                   (fn []
                     (do-with-maybe-persisted-cards!
                      persist?
                      (fn [cards]
                        (f (policy-ctx policy-k restricted cards restricted-ids allowed))))))))))))))))

(defn do-with-two-users!
  "Like [[do-with-policy!]], but with two sandboxed users under `policy-k` on the same copy of the database: `:rasta`
  with the policy's attributes, and the second user of [[policies/second-users]] with theirs, each in their own group
  and judged against their own restricted variant. Calls `(f [rasta-ctx second-ctx])`; each ctx carries `:user`."
  [policy-k f]
  (let [{:keys [gtaps attributes restricted remaps persist?]} (policies/policy policy-k)
        {second-user :user second-attributes :attributes second-restricted :restricted} (policies/second-users policy-k)]
    (mt/dataset dataset/shops
      (dataset/ensure-fk-metadata!)
      (fully-synced!)
      (let [base (mt/db)]
        (do-with-restricted!
         restricted remaps
         (fn [restricted-ids allowed]
           (do-with-restricted!
            second-restricted remaps
            (fn [second-restricted-ids second-allowed]
              (mt/with-db base
                (met/with-gtaps-for-user! :rasta {:gtaps (gtaps) :attributes attributes}
                  (grant-query-access! &group)
                  ;; a second sandbox on the same tables, in a second group, on the same copy of the database
                  (met/with-gtaps-for-user! second-user {:gtaps (gtaps) :attributes second-attributes}
                    (grant-query-access! &group)
                    (do-with-remaps!
                     remaps
                     (fn []
                       (do-with-maybe-persisted-cards!
                        persist?
                        (fn [cards]
                          (f [(policy-ctx policy-k restricted cards restricted-ids allowed)
                              (assoc (policy-ctx policy-k second-restricted cards second-restricted-ids second-allowed)
                                     :user second-user)]))))))))))))))))

;;; ------------------------------------------------ Schema checks -------------------------------------------------

(defn do-without-schema-checks!
  "Call `thunk` with `mu/defn` input and output checks off. Production does not compile those checks in, so a query
  that fails closed on a schema check in a test can fail open in production. A binding: the mock HTTP client runs
  handlers in a future, which conveys it."
  [thunk]
  (mb.hawk.parallel/assert-test-is-not-parallel "do-without-schema-checks!")
  (binding [mu.fn/*enforce* false]
    (thunk)))

(def modes
  "Mode name -> fn of a thunk that runs it in that mode: `:test` with schema checks on, `:production` without."
  (array-map
   :test       (fn [thunk] (thunk))
   :production do-without-schema-checks!))

(defn in-mode
  "Run `thunk` in `mode`, a key of [[modes]]."
  [mode thunk]
  ((get modes mode) thunk))

;;; ------------------------------------------------- HTTP transports ----------------------------------------------

(defn run-as
  "Run a probe (its `:outer` request body, or its legacy inner query and parameters) through the dataset API as `user`
  against `ids`' database. Returns `:rows`, `:cols` (names) and `:body`, or `:error` and `:body`."
  [user ids probe]
  (query-response->result
   (mt/user-http-request-full-response user :post "dataset" (or (:outer probe) (formats/legacy-outer ids probe)))))

(defn run-saved-as
  "Like [[run-as]], but save the probe's query as a card declaring one parameter per probe parameter, put it on a
  dashboard with each dashboard parameter mapped to the same target, and run it through `via`: `:card` sends the
  parameters to the card query API, `:dashcard` sends their values to the dashcard query API."
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

(defn- run-http
  "The sandboxed user's result through the HTTP entry point `via`, or nil when the transport is in-process only.
  `[:http :shared-card]` runs the card `(:shared-card-id ctx)`, which several users share, with the cache allowed."
  [{:keys [ids transport user shared-card-id] :or {user :rasta}} probe]
  (when-let [[_ via] (when (vector? transport) transport)]
    (case via
      :dataset     (run-as user ids probe)
      :shared-card (query-response->result
                    (mt/user-http-request-full-response user :post (format "card/%d/query" shared-card-id)
                                                        {:ignore_cache false :parameters []}))
      (run-saved-as user via ids probe))))

;;; ------------------------------------------------ Expected results ----------------------------------------------

(defonce ^:private expected-cache
  ;; see [[expected-results]]
  (atom {}))

(defn clear-expected-cache!
  "Forget every cached admin result; for the REPL."
  []
  (reset! expected-cache {}))

(defn- symbolic
  "`outer`, built against `ids`, with every table, field and card id replaced by a made-up stable one, so that two
  copies of the same database render the same query the same way."
  [ids outer]
  (generated/translate-query outer ids generated/fake-ids))

(defn- symbolic-col-ids
  "`result` with its `:col-ids` as `[table column]` keys of `ids` instead of ids, so the result can be reused on another
  copy of the same dataset; ids that name no dataset field are kept."
  [ids result]
  (let [id->col (into {} (map (fn [[tc id]] [id tc])) (:fields ids))]
    (m/update-existing result :col-ids (partial mapv #(get id->col % %)))))

(defn- concrete-col-ids
  "The inverse of [[symbolic-col-ids]] for the database `ids` describes."
  [ids result]
  (m/update-existing result :col-ids (partial mapv #(if (vector? %) (get-in ids [:fields %]) %))))

(defn- cached-admin-run
  "An admin's result for `outer` against the database `ids` describes, cached per driver, dataset, remaps,
  persistence, schema-check mode and symbolic query. Datasets are immutable per JVM and the key holds no id, so the
  cache never needs invalidating. Each policy runs on a fresh copy of the base dataset with new field ids, so the
  cached result's column ids are stored symbolically and mapped back to the current copy's. `cacheable?` false runs
  it every time."
  [{:keys [remaps persist?]} dataset-var ids outer cacheable?]
  (let [key [driver/*driver* (dataset/dataset-name dataset-var) remaps persist? mu.fn/*enforce* (symbolic ids outer)]]
    (if-not cacheable?
      (execute-as :crowberto outer)
      (concrete-col-ids
       ids
       (or (get @expected-cache key)
           (let [r (symbolic-col-ids ids (execute-as :crowberto outer))]
             (swap! expected-cache assoc key r)
             r))))))

(defn expected-results
  "The admin's results a probe is judged against: `:restricted`, on the policy's restricted variant, and `:base`, on
  the unrestricted dataset. Both cached across policies; see [[cached-admin-run]]."
  [{:keys [ids restricted-ids restricted-dataset] :as ctx} probe-fn probe cacheable?]
  {:restricted (cached-admin-run ctx restricted-dataset restricted-ids
                                 (formats/legacy-outer restricted-ids (probe-fn restricted-ids)) cacheable?)
   :base       (cached-admin-run ctx #'dataset/shops ids
                                 (or (:outer probe) (formats/legacy-outer ids probe)) cacheable?)})

;;; --------------------------------------------------- Running ----------------------------------------------------

(defn- trim [result]
  (-> result (dissoc :body) (update :rows #(some->> % (take 5)))))

(defn- card-columns
  "Card id -> the column names in its stored metadata, for each saved question `outer` reads, including one the probe
  made with forged metadata: the user sees those names."
  [outer]
  (let [ids (into #{} (map (fn [[_ a b]] (parse-long (or a b))))
                  (re-seq #"card__(\d+)|source-card\"? (\d+)" (pr-str outer)))]
    (into {} (for [id ids]
               [id (into #{} (map :name) (t2/select-one-fn :result_metadata :model/Card :id id))]))))

(defn- run-probe*
  "[[run-probe]] with the rendered `probe`."
  [{:keys [ids expect transport cacheable?] :or {transport :in-process cacheable? true} :as ctx} probe-name probe-fn probe]
  (let [outer        (or (:outer probe) (formats/legacy-outer ids probe))
        observations (observe (:user ctx :rasta) outer)
        http         (run-http ctx probe)
        {:keys [restricted base]} (expected-results ctx probe-fn probe cacheable?)
        expected     (if (= expect :unrestricted) base restricted)
        verdicts     (invariants/judge {:ctx          (assoc ctx :card-columns (card-columns outer))
                                        :query        outer
                                        :observations observations
                                        :user         (:executed observations)
                                        :expected     expected
                                        :base         base
                                        :http         http})
        sql          (get-in observations [:compiled :value :query])]
    (merge verdicts
           {:policy    (:policy ctx)
            :probe     probe-name
            :user      (trim (:executed observations))
            :expected  (trim expected)
            :http      (some-> http trim)
            :cached?   (boolean (get-in http [:body :cached]))
            :compiled  (or sql (get-in observations [:compiled :error :message]))
            :format    (:format ctx :legacy)
            :transport transport
            :query     probe})))

(defn run-probe
  "Run `probe-fn`, a fn of an ids map returning `{:query inner-query, :parameters ...}`, under `ctx`, sending the
  sandboxed user's query in `(:format ctx)` (default `:legacy`) and through `(:transport ctx)` (default
  `:in-process`; `[:http :dataset]`, `[:http :card]` or `[:http :dashcard]` add that entry point). Returns a result
  map with every invariant's verdict and `:outcome`; `:unrenderable` when the probe has no form in that format."
  [{:keys [ids] fmt :format :or {fmt :legacy} :as ctx} probe-name probe-fn]
  (if-let [probe (formats/render fmt ids (probe-fn ids))]
    (run-probe* ctx probe-name probe-fn probe)
    {:policy (:policy ctx) :probe probe-name :format fmt :outcome :unrenderable}))

(defn run-hand-written!
  "Run the hand-written probes (or those named by `probe-names`) under one policy, or every policy. Returns result
  maps."
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

(def parameter-transports
  "How the sandboxed user's parameters reach the processor: in-process as the dataset API would hand them over, and
  applied to a saved card by the dashcard query API. The card query API is left out: it accepts only parameters
  that match a template tag."
  [:in-process [:http :dashcard]])

(defn run-parameters!
  "Run the parameter probes (or those named by `probe-names`) under `policy-k`, once per parameter transport.
  Returns result maps."
  ([policy-k]
   (run-parameters! policy-k (keys hand-written/parameters)))
  ([policy-k probe-names]
   (do-with-policy!
    policy-k
    (fn [ctx]
      (doall (for [transport parameter-transports, probe-name probe-names]
               (run-probe (assoc ctx :transport transport) probe-name (hand-written/parameters probe-name))))))))

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
                  ;; the content's ids change every run, so its expected results cannot be cached
                  (run-probe (assoc ctx :cacheable? false) probe-name #(probe % (if (= % ids) created expected-created)))
                  (finally
                    (hand-written/delete-authored! created)
                    (hand-written/delete-authored! expected-created)))))))))

(defn run-forged-metadata!
  "Run the forged-metadata probes under `policy-k`: for each forgery the sandboxed user saves the model in the
  sandboxed database and an admin saves the same forgery in the restricted copy, then every query of
  [[hand-written/forged-metadata-queries]] runs over it. Probe names are `[:forged-metadata forgery query]`. Returns
  result maps."
  ([policy-k]
   (run-forged-metadata! policy-k [:http :dataset]))
  ([policy-k transport]
   (do-with-policy!
    policy-k
    (fn [{:keys [ids restricted-ids] :as ctx}]
      (doall
       (for [forgery-k (keys hand-written/forged-metadata)
             :let      [model          (hand-written/save-forged-model! :rasta ids forgery-k)
                        expected-model (hand-written/save-forged-model! :crowberto restricted-ids forgery-k)]
             result    (try
                         (doall (for [[query-k query-fn] hand-written/forged-metadata-queries]
                                  ;; the models' ids change every run, so their expected results cannot be cached
                                  (run-probe (assoc ctx :transport transport :cacheable? false)
                                             [:forged-metadata forgery-k query-k]
                                             #(query-fn % (if (= % ids) model expected-model)))))
                         (finally
                           (t2/delete! :model/Card :id [:in [model expected-model]])))]
         result))))))

(defn- sandbox-card-id
  "The card the current database's SHOPS sandbox is built out of."
  []
  (t2/select-one-fn :card_id :model/Sandbox :table_id (mt/id :shops)))

(def sandbox-card-forgeries
  "Forgery name -> fn of an ids map and the sandbox card's stored metadata, returning forged metadata. These rewrite
  the policy itself, as if the guard on non-admin edits had been bypassed."
  (let [shops-col (fn [ids c] (first (filter #(= (mt/format-name (name c)) (:name %))
                                             (hand-written/model-metadata (hand-written/table-query ids :shops)))))
        on-name   (fn [md f] (mapv #(if (= (mt/format-name "name") (:name %)) (f %) %) md))]
    (array-map
     :add-hidden-columns (fn [ids md] (into (vec md) (map #(shops-col ids %)) [:price :secret :secret_num]))
     :lib-names-hidden   (fn [_ md] (on-name md #(assoc %
                                                        :lib/original-name (mt/format-name "price")
                                                        :lib/deduplicated-name (mt/format-name "price")
                                                        :lib/source-column-alias (mt/format-name "price")
                                                        :lib/desired-column-alias (mt/format-name "price"))))
     :id-to-hidden       (fn [ids md] (on-name md #(assoc %
                                                          :id (dataset/fid ids :shops :price)
                                                          :field_ref [:field (dataset/fid ids :shops :price) nil]))))))

(defn run-with-forged-sandbox-card!
  "Run the hand-written probes under `policy-k` after rewriting its sandbox card's stored result metadata with
  `forgery-k` of [[sandbox-card-forgeries]], as a server-side write the guard does not see. A native sandbox card
  gets its metadata on its first sandboxed run, so one probe runs first. Returns result maps."
  [policy-k forgery-k]
  (do-with-policy!
   policy-k
   (fn [{:keys [ids] :as ctx}]
     (let [ctx     (assoc ctx :transport :in-process)
           card-id (sandbox-card-id)]
       (run-probe (assoc ctx :cacheable? false) :warm-up (hand-written/all :shops-count))
       (let [stored (t2/select-one-fn :result_metadata :model/Card :id card-id)]
         (binding [api/*current-user-id* nil]
           (t2/update! :model/Card card-id {:result_metadata ((sandbox-card-forgeries forgery-k) ids stored)})))
       (doall (for [[probe-name probe-fn] hand-written/all]
                (assoc (run-probe ctx probe-name probe-fn) :forgery forgery-k)))))))

(defn run-grid!
  "Run the grid probes named by `probe-names` (default: all) under `policy-k`, or under every policy."
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
  ([policy-k n seed]
   (run-generated! policy-k n seed :in-process))
  ([policy-k n seed transport]
   (do-with-policy!
    policy-k
    (fn [ctx]
      (let [outcomes (atom [])
            property (prop/for-all [c generated/gen-case]
                       (let [r (run-case! (assoc ctx :transport transport) c)]
                         (swap! outcomes conj r)
                         (not (invariants/failing-outcomes (:outcome r)))))
            ;; the property needs the expensive harness around it, so it cannot be a top-level `defspec`
            result   #_{:clj-kondo/ignore [:discouraged-var]} (tc/quick-check n property :seed seed)]
        {:result result :outcomes @outcomes})))))

(defn- sandboxed-responses!
  "Probe name -> the sandboxed user's masked executed result for each hand-written and parameter probe under
  `policy-k`, on `dataset`."
  [policy-k dataset]
  (do-with-policy!
   policy-k dataset
   (fn [{:keys [ids]}]
     (into {} (for [[probe-name probe-fn] (merge hand-written/all hand-written/parameters)]
                [probe-name (invariants/mask-response (:body (execute-as :rasta (formats/legacy-outer ids (probe-fn ids)))))])))))

(defn run-noninterference!
  "For `policy-k`, each varied dataset paired with its restricted dataset, and each probe, the sandboxed
  user's masked results on the base and the varied dataset, as `{:policy :varied :probe :base :varied-response}`."
  [policy-k]
  (let [base (sandboxed-responses! policy-k #'dataset/shops)]
    (doall (for [varied                (policies/varied policy-k)
                 :let                  [responses (sandboxed-responses! policy-k varied)]
                 [probe-name response] base]
             {:policy policy-k :varied varied :probe probe-name
              :base response :varied-response (responses probe-name)}))))

(defn summarize
  "Policy -> outcome -> count, for reading at the REPL."
  [results]
  (into (sorted-map)
        (map (fn [[policy rs]] [policy (frequencies (map :outcome rs))]))
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
                 "Outcome:" (pr-str (select-keys r [:outcome :differential :canary :error-secrets :structural :sql :hygiene :http-equivalence :error]))
                 "Exception:" (pr-str exception)
                 "Legacy query (base copy ids):" (json/encode (:query r))
                 "Compiled:" (pr-str (:compiled r))
                 "Sandboxed user got:" (pr-str (:user r))
                 "Expected:" (pr-str (:expected r))]))))

(defn drivers
  "The drivers the enforcement tests run on: every normal driver with nested queries and left joins."
  []
  (mt/normal-drivers-with-feature :nested-queries :left-join))
