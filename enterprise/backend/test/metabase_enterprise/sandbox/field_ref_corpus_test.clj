(ns ^:mb/driver-tests metabase-enterprise.sandbox.field-ref-corpus-test
  "Property check for sandboxing: a sandboxed user never sees a value that comes from a sandboxed row, whatever shape
  of field ref the query uses to reach it.

  VENUES is sandboxed by price, through a login attribute. Each probe is a legacy-MBQL query that reaches VENUES.PRICE
  from a CHECKINS query through one ref shape in one clause position. Each probe runs under every sandbox
  configuration in [[sandbox-configs]], and two oracles judge it:

  * Rows: the probe runs through the dataset API as an admin and as the sandboxed user. Every VENUES.PRICE value the
    user sees must be one the sandbox allows, and every count of CHECKINS rows filtered on VENUES must equal the
    count over the allowed venues.
  * Structure: the probe is preprocessed as the sandboxed user and given alias info, as compilation would. No field ref
    outside a sandbox stage may have VENUES as its physical source, since that compiles to the raw table name. This
    oracle does not depend on the driver rejecting the SQL.

  Verdicts per probe, worst first:

  * `:leak`      -- the sandboxed user sees VENUES data the sandbox does not allow.
  * `:raw-ref`   -- the user's query references raw VENUES outside the sandbox. Here the database rejected it, but
                    the invariant is already broken and other drivers or query shapes may accept it.
  * `:over-restricted` -- the sandboxed user sees fewer rows than the sandbox allows. Not a leak.
  * `:enforced`  -- admin sees disallowed VENUES data, the sandboxed user sees only allowed data, no raw reference.
  * `:rejected`  -- the shape fails for the admin and the sandboxed user alike, with no raw reference.
  * `:user-only-error` -- admin succeeds, the sandboxed user's query fails, no raw reference.
  * `:user-ok-admin-error` -- the admin's run fails but the sandboxed user's succeeds with clean rows.
  * `:vacuous`   -- admin succeeds but sees no disallowed VENUES data, so the probe proves nothing."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.test-util :as met]
   [metabase.lib.walk :as lib.walk]
   [metabase.permissions.models.data-permissions :as data-perms]
   [metabase.query-processor.preprocess :as qp.preprocess]
   [metabase.query-processor.setup :as qp.setup]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.test :as mt]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [metabase.util.match :as match]))

;;; ------------------------------------------------ Ref shapes ---------------------------------------------------

(defn- ids []
  {:checkins          (mt/id :checkins)
   :venues            (mt/id :venues)
   :users             (mt/id :users)
   :checkins.id       (mt/id :checkins :id)
   :checkins.venue-id (mt/id :checkins :venue_id)
   :checkins.user-id  (mt/id :checkins :user_id)
   :venues.id         (mt/id :venues :id)
   :venues.price      (mt/id :venues :price)
   :venues.name       (mt/id :venues :name)
   :venues.category   (mt/id :venues :category_id)
   :users.id          (mt/id :users :id)
   ;; physical names, which differ in case by driver
   :name/price        (mt/format-name "price")
   :name/venue-id     (mt/format-name "venue_id")
   :name/venues       (mt/format-name "venues")})

(def ^:private ref-shapes
  "Ref shape name -> fn of the [[ids]] map returning a legacy ref that names VENUES.PRICE from a CHECKINS stage."
  (array-map
   ;; what the query builder produces
   :canonical-source-field          (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.venue-id i)}])
   ;; legacy forms normalization still accepts
   :legacy-fk->                     (fn [i] ["fk->" ["field-id" (:checkins.venue-id i)] ["field-id" (:venues.price i)]])
   :legacy-fk->-field               (fn [i] ["fk->" ["field" (:checkins.venue-id i) nil] ["field" (:venues.price i) nil]])
   ;; string ref plus source-field: implicit joins only create joins for integer refs
   :name-ref+source-field           (fn [i] ["field" (:name/price i) {"base-type" "type/Integer" "source-field" (:checkins.venue-id i)}])
   ;; source-field-name / source-field-join-alias variants
   :source-field-name-matching      (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.venue-id i) "source-field-name" (:name/venue-id i)}])
   :source-field-name-bogus         (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.venue-id i) "source-field-name" "NOPE"}])
   :source-field-join-alias-bogus   (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.venue-id i) "source-field-join-alias" "Nope"}])
   ;; refs that name the other table with no join at all
   :bare-id-off-table               (fn [i] ["field" (:venues.price i) nil])
   :bare-id-off-table+base-type     (fn [i] ["field" (:venues.price i) {"base-type" "type/Integer"}])
   :join-alias-of-implicit-join     (fn [i] ["field" (:venues.price i) {"join-alias" (str (:name/venues i) "__via__" (:name/venue-id i))}])
   :field-literal                   (fn [i] ["field-literal" (:name/price i) "type/Integer"])
   ;; source-field that is not the right FK
   :source-field-wrong-fk           (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.user-id i)}])
   :source-field-not-an-fk          (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.id i)}])
   :source-field-from-venues        (fn [i] ["field" (:venues.price i) {"source-field" (:venues.category i)}])))

;;; ---------------------------------------------- Clause positions -----------------------------------------------

(def ^:private positions
  "Clause position name -> `{:query (fn [ids ref] inner-query), :result result-kind}`, with an optional `:parameters`
  fn. `result-kind` is `[:values col]` when column `col` holds VENUES.PRICE values, or `[:count]` when the first cell
  counts CHECKINS rows whose venue passes the probe."
  (array-map
   :fields
   {:query  (fn [i r] {:source-table (:checkins i) :fields [r] :limit 50})
    :result [:values 0]}
   :breakout
   {:query  (fn [i r] {:source-table (:checkins i) :breakout [r] :aggregation [["count"]]})
    :result [:values 0]}
   :filter
   {:query  (fn [i r] {:source-table (:checkins i) :aggregation [["count"]] :filter ["not-null" r]})
    :result [:count]}
   :aggregation
   {:query  (fn [i r] {:source-table (:checkins i) :aggregation [["max" r]]})
    :result [:values 0]}
   :expression
   {:query  (fn [i r] {:source-table (:checkins i) :expressions {"x" ["+" r 0]} :fields [["expression" "x"]] :limit 50})
    :result [:values 0]}
   ;; an integer canonical ref creates the implicit join, then the ref under test rides along with it
   :fields-mixed-with-canonical
   {:query  (fn [i r] {:source-table (:checkins i)
                       :fields       [["field" (:venues.name i) {"source-field" (:checkins.venue-id i)}] r]
                       :limit        50})
    :result [:values 1]}
   ;; the same, but the canonical ref spells out `source-field-name`, which the implicit-join code treats as a
   ;; separate join unless it matches the FK's own name
   :fields-mixed-with-source-field-name
   {:query  (fn [i r] {:source-table (:checkins i)
                       :fields       [["field" (:venues.name i) {"source-field"      (:checkins.venue-id i)
                                                                 "source-field-name" (:name/venue-id i)}]
                                      r]
                       :limit        50})
    :result [:values 1]}
   ;; the ref under test first, the canonical ref second
   :fields-mixed-canonical-after
   {:query  (fn [i r] {:source-table (:checkins i)
                       :fields       [r ["field" (:venues.name i) {"source-field" (:checkins.venue-id i)}]]
                       :limit        50})
    :result [:values 0]}
   ;; saved question and model over CHECKINS as the source, the way stored legacy refs reach the QP
   :card-source
   {:query  (fn [i r] {:source-table (str "card__" (:card/checkins i)) :fields [r] :limit 50})
    :result [:values 0]}
   :card-source-mixed-with-canonical
   {:query  (fn [i r] {:source-table (str "card__" (:card/checkins i))
                       :fields       [["field" (:venues.name i) {"source-field" (:checkins.venue-id i)}] r]
                       :limit        50})
    :result [:values 1]}
   :model-source
   {:query  (fn [i r] {:source-table (str "card__" (:model/checkins i)) :fields [r] :limit 50})
    :result [:values 0]}
   :card-source-filter
   {:query  (fn [i r] {:source-table (str "card__" (:card/checkins i)) :aggregation [["count"]] :filter ["not-null" r]})
    :result [:count]}
   ;; the ref under test in a second stage, over a first stage that is plain CHECKINS
   :second-stage
   {:query  (fn [i r] {:source-query {:source-table (:checkins i)} :fields [r] :limit 50})
    :result [:values 0]}
   ;; an explicit join to VENUES exists; the ref under test does not name its alias
   :explicit-join-present
   {:query  (fn [i r] {:source-table (:checkins i)
                       :joins        [{:source-table (:venues i)
                                       :alias        "V"
                                       :condition    ["=" ["field" (:checkins.venue-id i) nil]
                                                      ["field" (:venues.id i) {"join-alias" "V"}]]
                                       :fields       "none"}]
                       :fields       [r]
                       :limit        50})
    :result [:values 0]}
   ;; the ref under test inside the condition of an inner join to an unsandboxed table
   :join-condition
   {:query  (fn [i r] {:source-table (:checkins i)
                       :joins        [{:source-table (:users i)
                                       :alias        "U"
                                       :strategy     "inner-join"
                                       :condition    ["and"
                                                      ["=" ["field" (:checkins.user-id i) nil]
                                                       ["field" (:users.id i) {"join-alias" "U"}]]
                                                      ["not-null" r]]
                                       :fields       "none"}]
                       :aggregation  [["count"]]})
    :result [:count]}
   ;; the ref under test as a parameter target
   :parameter
   {:query      (fn [i _r] {:source-table (:checkins i) :aggregation [["count"]]})
    :parameters (fn [_i r] [{:type "number/=" :target ["dimension" r] :value [1 2 3 4]}])
    :result     [:count]}))

;;; ------------------------------------------------- Running ------------------------------------------------------

(defn- probe-query [i shape-k position-k]
  (let [r                                 ((ref-shapes shape-k) i)
        {:keys [query parameters]}        (positions position-k)]
    (cond-> {:database (mt/id) :type "query" :query (query i r)}
      parameters (assoc :parameters (parameters i r)))))

(defn- run-as [user query]
  (let [{:keys [status body]} (mt/user-http-request-full-response user :post "dataset" query)]
    (if (and (= 202 status) (= "completed" (:status body)))
      {:rows (get-in body [:data :rows])}
      {:error (u/truncate (str status " " (or (:error body) (:message body) body)) 300)})))

(defn- raw-refs-to-table
  "Field refs in `query`, outside any sandbox stage, whose physical source after alias resolution is `table-id`. Runs
  as the current user. Returns `{:refs [...]}` or `{:error msg}` when the query does not preprocess."
  [query table-id]
  ;; round-trip through JSON so the query arrives in the same shape the dataset API decodes
  (let [query (json/decode+kw (json/encode query))
        acc   (volatile! [])]
    (try
      (qp.setup/with-qp-setup [query query]
        (lib.walk/walk-stages
         (-> query qp.preprocess/preprocess add/add-alias-info)
         (fn [_query path stage]
           (when-not (:query-permissions/sandboxed-table stage)
             ;; a join's own stages are walked separately, but its conditions belong to this stage
             (let [own (-> stage
                           (dissoc :lib/stage-metadata)
                           (update :joins (partial mapv #(dissoc % :stages :lib/stage-metadata))))]
               (vswap! acc into
                       (map (fn [ref] {:path path :ref ref}))
                       (match/match-many own
                         [:field (opts :guard (= table-id (::add/source-table opts))) _] &match))))
           nil)))
      {:refs @acc}
      (catch Throwable e
        {:error (u/truncate (ex-message e) 300)}))))

(defn- judge
  "How `rows` compare to what the sandbox allows: `:clean` when every VENUES value is allowed, `:dirty` when some
  value is not, `:short` when a count is below the allowed count."
  [[kind col] {:keys [allowed allowed-count]} rows]
  (case kind
    :values (if (some #(when-some [v (nth % col)] (not (contains? allowed v))) rows) :dirty :clean)
    :count  (let [n (or (ffirst rows) 0)]
              (cond (> n allowed-count) :dirty
                    (< n allowed-count) :short
                    :else               :clean))))

(defn- classify [{admin-rows :rows admin-error :error}
                 {user-rows :rows user-error :error}
                 {raw-refs :refs}
                 result-kind
                 ctx]
  (let [user-judgement (some->> user-rows (judge result-kind ctx))]
    (cond
      (= :dirty user-judgement)                                :leak
      (seq raw-refs)                                           :raw-ref
      admin-error                                              (if user-error :rejected :user-ok-admin-error)
      (not= :dirty (judge result-kind ctx admin-rows))         :vacuous
      user-error                                               :user-only-error
      (= :short user-judgement)                                :over-restricted
      :else                                                    :enforced)))

(defn- corpus-drivers []
  (mt/normal-drivers-with-feature :nested-queries :left-join))

(defn- run-probe [ctx i shape-k position-k]
  (let [query     (probe-query i shape-k position-k)
        admin     (run-as :crowberto query)
        user      (run-as :rasta query)
        structure (raw-refs-to-table query (:venues i))]
    {:config    (:config ctx)
     :shape     shape-k
     :position  position-k
     :outcome   (classify admin user structure (:result (positions position-k)) ctx)
     :admin     admin
     :user      user
     :structure structure
     :query     query}))

(defn- price-row-sandbox []
  {:remappings {"price" ["variable" [:field (mt/id :venues :price) nil]]}})

(defn- hide-price-column-sandbox []
  {:query (mt/mbql-query venues {:fields [$id $name $category_id $latitude $longitude]})})

(def ^:private sandbox-configs
  "Config name -> the VENUES sandbox definition, the login attributes, and the prices that leaves visible."
  (array-map
   :empty      {:gtap price-row-sandbox         :attributes {"price" "999"} :allowed #{}}
   :price-1    {:gtap price-row-sandbox         :attributes {"price" "1"}   :allowed #{1}}
   ;; every row visible, PRICE column dropped by the sandbox card
   :hide-price {:gtap hide-price-column-sandbox :attributes {}              :allowed #{}}))

(defn- do-with-venues-sandbox!
  "Run `(thunk card-ids)` as the sandboxed :rasta user, with VENUES sandboxed by `(gtap-fn)`, CHECKINS and USERS
  fully queryable, and a saved question and a model over CHECKINS. `gtap-fn` runs inside the sandbox's copy of the
  test database, so the ids it reads belong to that copy."
  [gtap-fn attributes thunk]
  (met/with-gtaps! {:gtaps {:venues (gtap-fn)} :attributes attributes}
    (doseq [table [:checkins :users]]
      (data-perms/set-table-permission! &group (mt/id table) :perms/create-queries :query-builder))
    (mt/with-temp [:model/Card {card-id :id}  {:dataset_query (mt/mbql-query checkins)}
                   :model/Card {model-id :id} {:dataset_query (mt/mbql-query checkins) :type :model}]
      (thunk {:card/checkins card-id :model/checkins model-id}))))

(defn- allowed-checkins-count
  "Number of CHECKINS rows whose venue has a price in `allowed`, counted as admin."
  [i allowed]
  (if (empty? allowed)
    0
    (-> (run-as :crowberto {:database (mt/id)
                            :type     "query"
                            :query    {:source-table (:checkins i)
                                       :aggregation  [["count"]]
                                       :filter       (into ["=" ((ref-shapes :canonical-source-field) i)] allowed)}})
        :rows
        ffirst)))

(defn- do-with-config!
  "Run `(f ctx ids)` under sandbox config `config-k`, where `ctx` carries what [[judge]] needs."
  [config-k f]
  (let [{:keys [gtap attributes allowed]} (sandbox-configs config-k)]
    (do-with-venues-sandbox!
     gtap
     attributes
     (fn [card-ids]
       (let [i (merge (ids) card-ids)]
         (f {:config        config-k
             :allowed       allowed
             :allowed-count (allowed-checkins-count i allowed)}
            i))))))

(defn run-corpus!
  "Run every ref shape in every clause position under sandbox config `config-k`, or under every config. Returns a seq
  of probe results."
  ([]
   (mapcat run-corpus! (keys sandbox-configs)))
  ([config-k]
   (do-with-config!
    config-k
    (fn [ctx i]
      (doall (for [shape-k (keys ref-shapes), position-k (keys positions)]
               (run-probe ctx i shape-k position-k)))))))

(defn summarize
  "A shape x position grid of outcome keywords, for reading at the REPL."
  [results]
  (into (sorted-map)
        (map (fn [[shape rs]] [shape (into (array-map) (map (juxt :position :outcome)) rs)]))
        (group-by :shape results)))

;;; -------------------------------------------------- Tests -------------------------------------------------------

(deftest ^:synchronized baseline-probe-is-meaningful-test
  (mt/test-drivers (corpus-drivers)
    (doseq [config-k (keys sandbox-configs)]
      (testing (format "Under %s, the canonical ref sees disallowed VENUES data as admin and only allowed data as the sandboxed user"
                       config-k)
        (let [{:keys [outcome] :as result} (do-with-config! config-k #(run-probe %1 %2 :canonical-source-field :fields))]
          (is (= :enforced outcome) (pr-str result)))))))

(deftest ^:synchronized field-ref-shapes-cannot-escape-sandbox-test
  (mt/test-drivers (corpus-drivers)
    (doseq [{:keys [config shape position outcome] :as result} (run-corpus!)]
      (testing (format "%s in %s under %s" shape position config)
        (testing "the sandboxed user sees no rows the sandbox does not allow"
          (is (not= :leak outcome) (pr-str (select-keys result [:user :query]))))
        (testing "the sandboxed user's query never references the raw sandboxed table"
          (is (not= :raw-ref outcome) (pr-str (select-keys result [:structure :user :query]))))))))

(def ^:private forged-stage-keys
  "Keys the sandbox rewrite or the permission walk set on stages themselves, which a client must not be able to supply."
  {:sandbox?-flag    (fn [_vid] {"metabase-enterprise.sandbox.query-processor.middleware.sandboxing/sandbox?" true})
   :marker           (fn [vid] {"query-permissions/sandboxed-table" vid})
   :flag-and-marker  (fn [vid] {"metabase-enterprise.sandbox.query-processor.middleware.sandboxing/sandbox?" true
                                "query-permissions/sandboxed-table" vid})
   :from-source-card (fn [_vid] {"qp/stage-is-from-source-card" 1})})

(deftest ^:synchronized forged-internal-stage-keys-do-not-skip-sandbox-test
  (mt/test-drivers (corpus-drivers)
    (do-with-config!
     :empty
     (fn [_ctx i]
       (doseq [[k extra-fn] forged-stage-keys
               :let         [extra (extra-fn (:venues i))
                             field ["field" (:venues.price i) nil]]
               [fmt query]  {:legacy {:database (mt/id)
                                      :type     "query"
                                      :query    (merge {:source-table (:venues i) :fields [field] :limit 5} extra)}
                             :mbql5  {:database (mt/id)
                                      :lib/type "mbql/query"
                                      :stages   [(merge {:lib/type     "mbql.stage/mbql"
                                                         :source-table (:venues i)
                                                         :fields       [["field" {:lib/uuid (str (random-uuid))} (:venues.price i)]]
                                                         :limit        5}
                                                        extra)]}}]
         (testing (format "%s as %s" k fmt)
           (is (= 5 (count (:rows (run-as :crowberto query))))
               "admin sees VENUES rows, so the probe is meaningful")
           (is (= [] (:rows (run-as :rasta query))))))))))
