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
   [metabase-enterprise.sandbox.query-processor.middleware.sandboxing :as sandboxing]
   [metabase-enterprise.sandbox.restricted-db-test-util :as rdb]
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
  {:db                (mt/id)
   :checkins          (mt/id :checkins)
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
   ;; the VENUES column the ref shapes aim at; see [[retarget]]
   :target            (mt/id :venues :price)
   :name/target       (mt/format-name "price")
   :target-type       "type/Integer"
   :target-param      {:type "number/=" :value [1 2 3 4]}
   :name/venue-id     (mt/format-name "venue_id")
   :name/venues       (mt/format-name "venues")})

(def ^:private ref-shapes
  "Ref shape name -> fn of the [[ids]] map returning a legacy ref that names the target VENUES column from a CHECKINS
  stage."
  (array-map
   ;; what the query builder produces
   :canonical-source-field          (fn [i] ["field" (:target i) {"source-field" (:checkins.venue-id i)}])
   ;; legacy forms normalization still accepts
   :legacy-fk->                     (fn [i] ["fk->" ["field-id" (:checkins.venue-id i)] ["field-id" (:target i)]])
   :legacy-fk->-field               (fn [i] ["fk->" ["field" (:checkins.venue-id i) nil] ["field" (:target i) nil]])
   ;; string ref plus source-field: implicit joins only create joins for integer refs
   :name-ref+source-field           (fn [i] ["field" (:name/target i) {"base-type" (:target-type i) "source-field" (:checkins.venue-id i)}])
   ;; source-field-name / source-field-join-alias variants
   :source-field-name-matching      (fn [i] ["field" (:target i) {"source-field" (:checkins.venue-id i) "source-field-name" (:name/venue-id i)}])
   :source-field-name-bogus         (fn [i] ["field" (:target i) {"source-field" (:checkins.venue-id i) "source-field-name" "NOPE"}])
   :source-field-join-alias-bogus   (fn [i] ["field" (:target i) {"source-field" (:checkins.venue-id i) "source-field-join-alias" "Nope"}])
   ;; refs that name the other table with no join at all
   :bare-id-off-table               (fn [i] ["field" (:target i) nil])
   :bare-id-off-table+base-type     (fn [i] ["field" (:target i) {"base-type" (:target-type i)}])
   :join-alias-of-implicit-join     (fn [i] ["field" (:target i) {"join-alias" (str (:name/venues i) "__via__" (:name/venue-id i))}])
   :field-literal                   (fn [i] ["field-literal" (:name/target i) (:target-type i)])
   ;; source-field that is not the right FK
   :source-field-wrong-fk           (fn [i] ["field" (:target i) {"source-field" (:checkins.user-id i)}])
   :source-field-not-an-fk          (fn [i] ["field" (:target i) {"source-field" (:checkins.id i)}])
   :source-field-from-venues        (fn [i] ["field" (:target i) {"source-field" (:venues.category i)}])))

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
    :parameters (fn [i r] [(assoc (:target-param i) :target ["dimension" r])])
    :result     [:count]}))

;;; ------------------------------------------------- Canaries -----------------------------------------------------
;;;
;;; In the differential databases every VENUES row carries two canary values that decode back to its ID: a text token
;;; in SECRET and a number in SECRET_NUM. A canary from a venue the sandbox does not allow, anywhere in a response the
;;; sandboxed user gets (rows, error text, metadata), is a leak, whatever endpoint or query shape produced it.

(def ^:private canary-nonce "5b0dca7e")
(def ^:private canary-num-base 900000000000)
(def ^:private canary-num-step 7919)

(def ^:private canary-prepare-sql
  "Adds the canary columns. Runs in both differential databases, before the sandbox is applied to the restricted one."
  ["ALTER TABLE VENUES ADD COLUMN SECRET VARCHAR"
   (format "UPDATE VENUES SET SECRET = CONCAT('CANARY-V', ID, '-%s')" canary-nonce)
   "ALTER TABLE VENUES ADD COLUMN SECRET_NUM BIGINT"
   (format "UPDATE VENUES SET SECRET_NUM = %d + ID * %d" canary-num-base canary-num-step)])

(def ^:private canary-text-pattern (re-pattern (str "CANARY-V(\\d+)-" canary-nonce)))

;; SECRET_NUM values are 12-digit numbers starting with 9; JSON may also print them as doubles, e.g. 9.00000007919E11
(def ^:private canary-num-pattern #"(?<![\d.])9(?:\d{11}|\.\d+E11)(?![\d.])")

(defn- canary-num->venue-id [s]
  (let [n (- (long (bigdec s)) canary-num-base)]
    (when (and (pos? n) (zero? (mod n canary-num-step)))
      (quot n canary-num-step))))

(defn canary-venue-ids
  "IDs of the VENUES rows whose canaries appear anywhere in `x`, a response body or any other value."
  [x]
  (let [text (pr-str x)]
    (into (into #{} (map (comp parse-long second)) (re-seq canary-text-pattern text))
          (keep canary-num->venue-id)
          (re-seq canary-num-pattern text))))

(defn- retarget
  "Aim the ref shapes in ids map `i` at `target`: `:price`, or `:secret`, the text canary column. Call it inside the
  database the ids belong to."
  [i target]
  (case target
    :price  i
    :secret (assoc i
                   :target       (mt/id :venues :secret)
                   :name/target  (mt/format-name "secret")
                   :target-type  "type/Text"
                   :target-param {:type "string/starts-with" :value ["CANARY"]})))

;;; ------------------------------------------------- Running ------------------------------------------------------

(defn- probe-query [i shape-k position-k]
  (let [r                                 ((ref-shapes shape-k) i)
        {:keys [query parameters]}        (positions position-k)]
    (cond-> {:database (:db i) :type "query" :query (query i r)}
      parameters (assoc :parameters (parameters i r)))))

(defn- run-as
  "Run `query` through the dataset API as `user`. Returns `:rows` or `:error`, and `:canary-ids`, the venues whose
  canaries appear anywhere in the response body."
  [user query]
  (let [{:keys [status body]} (mt/user-http-request-full-response user :post "dataset" query)]
    (assoc (if (and (= 202 status) (= "completed" (:status body)))
             {:rows (get-in body [:data :rows])}
             {:error (u/truncate (str status " " (or (:error body) (:message body) body)) 300)})
           :canary-ids (canary-venue-ids body))))

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
   :empty      {:gtap         price-row-sandbox
                :attributes   {"price" "999"}
                :allowed      #{}
                :restrict-sql ["DELETE FROM VENUES"]}
   :price-1    {:gtap         price-row-sandbox
                :attributes   {"price" "1"}
                :allowed      #{1}
                :restrict-sql ["DELETE FROM VENUES WHERE PRICE IS NULL OR PRICE <> 1"]}
   ;; every row visible, PRICE column dropped by the sandbox card
   :hide-price {:gtap         hide-price-column-sandbox
                :attributes   {}
                :allowed      #{}
                :restrict-sql ["UPDATE VENUES SET PRICE = NULL"
                               ;; the sandbox card does not select the canary columns either
                               "UPDATE VENUES SET SECRET = NULL, SECRET_NUM = NULL"]}))

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

;;; ------------------------------------------- Differential oracle -----------------------------------------------
;;;
;;; A sandbox is a view. The sandboxed user running Q on the real data must get the same rows as an admin running Q on
;;; a copy of the data with the sandbox applied by hand, or else a clean error. This needs no per-position oracle, so
;;; it judges counts, joins and aggregates the same way.

(def ^:private differential-tables ["CATEGORIES" "VENUES" "USERS" "CHECKINS"])

(def ^:private differential-fks
  [["CHECKINS" "VENUE_ID" "VENUES" "ID"]
   ["CHECKINS" "USER_ID" "USERS" "ID"]
   ["VENUES" "CATEGORY_ID" "CATEGORIES" "ID"]])

(defn- unlimited
  "`query` without its row limit, so result sets compare as wholes."
  [query]
  (update query :query dissoc :limit))

(defn- classify-differential
  "Compare the sandboxed user's result on the base data with an admin's result on the restricted copy. `admin-base` is
  an admin's result on the base data; when it equals the restricted result the probe cannot tell the two apart."
  [{user-rows :rows user-error :error}
   {expected-rows :rows expected-error :error}
   {admin-rows :rows}]
  (let [freq frequencies]
    (cond
      (and expected-error user-error)               :rejected
      expected-error                                :expected-error
      user-error                                    :user-error
      (= (freq user-rows) (freq expected-rows))     (if (= (freq admin-rows) (freq expected-rows)) :vacuous :consistent)
      ;; any row the user got more often than the restricted data has is data the sandbox should have hidden
      (some (fn [[row n]] (> n (get (freq expected-rows) row 0))) (freq user-rows))
      :extra-rows
      :else                                         :missing-rows)))

(defn- run-differential-probe
  "Run one probe. `:differential` is the differential verdict. `:leaked-venue-ids` are venues outside
  `allowed-venue-ids` whose canaries reached the sandboxed user. `:outcome` is `:canary-leak` when there are any, and
  the differential verdict otherwise."
  [i-restricted allowed-venue-ids ctx i target shape-k position-k]
  (let [user-query   (unlimited (probe-query i shape-k position-k))
        user         (run-as :rasta user-query)
        expected     (run-as :crowberto (unlimited (probe-query i-restricted shape-k position-k)))
        admin-base   (run-as :crowberto user-query)
        differential (classify-differential user expected admin-base)
        leaked       (not-empty (into (sorted-set) (remove allowed-venue-ids) (:canary-ids user)))]
    {:config           (:config ctx)
     :target           target
     :shape            shape-k
     :position         position-k
     :outcome          (if leaked :canary-leak differential)
     :differential     differential
     :leaked-venue-ids leaked
     :user             (update user :rows #(some->> % (take 5)))
     :expected         (update expected :rows #(some->> % (take 5)))
     :query            user-query}))

(defn- allowed-canary-venue-ids
  "Venues whose canaries survive in the restricted copy, read as an admin. Run inside the restricted database."
  []
  (canary-venue-ids (run-as :crowberto {:database (mt/id)
                                        :type     "query"
                                        :query    {:source-table (mt/id :venues)
                                                   :fields       [["field" (mt/id :venues :secret) nil]
                                                                  ["field" (mt/id :venues :secret_num) nil]]}})))

(defn- with-checkins-cards!
  "Call `(f card-ids)` with a saved question and a model over CHECKINS in the current database."
  [f]
  (mt/with-temp [:model/Card {card-id :id}  {:dataset_query (mt/mbql-query checkins)}
                 :model/Card {model-id :id} {:dataset_query (mt/mbql-query checkins) :type :model}]
    (f {:card/checkins card-id :model/checkins model-id})))

(def ^:private differential-targets [:price :secret])

(defn- all-differential-probes []
  (for [target differential-targets, shape-k (keys ref-shapes), position-k (keys positions)]
    [target shape-k position-k]))

(defn run-differential!
  "Run `probes` (triples of target column, ref shape and clause position, by default all of them) under sandbox config
  `config-k`, or every config, judged by the differential oracle and the canary scan. H2 only: the restricted copy is
  built in H2. Returns a seq of probe results."
  ([]
   (mapcat run-differential! (keys sandbox-configs)))
  ([config-k]
   (run-differential! config-k (all-differential-probes)))
  ([config-k probes]
   (mt/dataset test-data
     (rdb/do-with-restricted-pair!
      {:tables       differential-tables
       :fks          differential-fks
       :prepare-sql  canary-prepare-sql
       :restrict-sql (:restrict-sql (sandbox-configs config-k))}
      (fn [base restricted]
        (mt/with-db restricted
          (with-checkins-cards!
            (fn [card-ids]
              (let [i-restricted      (merge (ids) card-ids)
                    i-restricted-by-t (into {} (for [t differential-targets] [t (retarget i-restricted t)]))
                    allowed-venue-ids (allowed-canary-venue-ids)]
                (mt/with-db base
                  (do-with-config!
                   config-k
                   (fn [ctx i]
                     (let [i-by-t (into {} (for [t differential-targets] [t (retarget i t)]))]
                       (doall (for [[target shape-k position-k] probes]
                                (run-differential-probe (i-restricted-by-t target) allowed-venue-ids ctx
                                                        (i-by-t target) target shape-k position-k))))))))))))))))

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

(deftest ^:synchronized differential-oracle-detects-dropped-sandbox-filter-test
  (testing "With the sandbox's attribute filter dropped, the differential oracle reports rows the sandbox should hide"
    (mt/test-drivers #{:h2}
      (with-redefs-fn {#'sandboxing/sandbox->parameters (constantly [])}
        #(is (= [:extra-rows]
                (map :outcome (run-differential! :price-1 [[:price :canonical-source-field :fields]]))))))))

(deftest ^:synchronized sandboxed-results-match-restricted-data-test
  (testing "Every probe the sandboxed user can run returns exactly what an admin gets from the data with the sandbox applied by hand"
    (mt/test-drivers #{:h2}
      (doseq [{:keys [config target shape position outcome] :as result} (run-differential!)]
        (testing (format "%s aimed at %s in %s under %s" shape target position config)
          (is (not (#{:extra-rows :missing-rows :canary-leak} outcome))
              (pr-str (select-keys result [:leaked-venue-ids :user :expected :query]))))))))

;;; ---------------------------------------------- Canary endpoints ------------------------------------------------

(defn- endpoint-requests
  "Endpoint name -> `[method path body]` for endpoints that read VENUES data or metadata, aimed at the canary columns."
  [i]
  (let [secret     (mt/id :venues :secret)
        secret-num (mt/id :venues :secret_num)
        venues     (mt/id :venues)
        canonical  ["field" secret {"source-field" (:checkins.venue-id i)}]]
    (array-map
     :field-values-secret        [:get (format "field/%d/values" secret)]
     :field-values-secret-num    [:get (format "field/%d/values" secret-num)]
     :field-search-secret        [:get (format "field/%d/search/%d?value=CANARY&limit=1000" secret secret)]
     :field-summary-secret       [:get (format "field/%d/summary" secret)]
     :field-secret               [:get (format "field/%d" secret)]
     :field-secret-num           [:get (format "field/%d" secret-num)]
     :table-query-metadata       [:get (format "table/%d/query_metadata" venues)]
     :table                      [:get (format "table/%d" venues)]
     :database-metadata          [:get (format "database/%d/metadata" (mt/id))]
     :parameter-values           [:post "dataset/parameter/values"
                                  {:parameter {:id "p" :type "string/=" :slug "s"} :field_ids [secret]}]
     :parameter-search           [:post "dataset/parameter/search/CANARY"
                                  {:parameter {:id "p" :type "string/=" :slug "s"} :field_ids [secret]}]
     :pivot                      [:post "dataset/pivot"
                                  {:database (mt/id) :type "query"
                                   :query    {:source-table (:checkins i) :breakout [canonical] :aggregation [["count"]]}}]
     :search                     [:get "search?q=CANARY"])))

(defn run-canary-endpoints!
  "Call each of [[endpoint-requests]] as the sandboxed user under sandbox config `config-k`. Returns endpoint name ->
  `{:status s, :leaked-venue-ids ids}`, where `ids` are venues outside the sandbox whose canaries the response carries."
  [config-k]
  (mt/dataset test-data
    (rdb/do-with-restricted-pair!
     {:tables       differential-tables
      :fks          differential-fks
      :prepare-sql  canary-prepare-sql
      :restrict-sql (:restrict-sql (sandbox-configs config-k))}
     (fn [base restricted]
       (let [allowed-venue-ids (mt/with-db restricted (allowed-canary-venue-ids))]
         (mt/with-db base
           (do-with-config!
            config-k
            (fn [_ctx i]
              (into (array-map)
                    (for [[endpoint [method path body]] (endpoint-requests i)]
                      (let [{:keys [status] response :body} (if body
                                                              (mt/user-http-request-full-response :rasta method path body)
                                                              (mt/user-http-request-full-response :rasta method path))]
                        [endpoint {:status           status
                                   :leaked-venue-ids (not-empty (into (sorted-set)
                                                                      (remove allowed-venue-ids)
                                                                      (canary-venue-ids response)))}])))))))))))

(deftest ^:synchronized endpoints-reveal-no-canaries-outside-sandbox-test
  (mt/test-drivers #{:h2}
    (doseq [config-k [:price-1 :hide-price]
            [endpoint {:keys [status leaked-venue-ids]}] (run-canary-endpoints! config-k)]
      (testing (format "%s under %s (HTTP %s)" endpoint config-k status)
        (is (empty? leaked-venue-ids))))))
