(ns metabase-enterprise.sandbox.field-ref-corpus-test
  "Property check for sandboxing: a sandboxed user never sees a value that comes from a sandboxed row, whatever shape
  of field ref the query uses to reach it.

  VENUES is sandboxed to an attribute value that matches no row. Each probe is a legacy-MBQL query that reaches
  VENUES.PRICE from a CHECKINS query through one ref shape in one clause position. Two oracles judge it:

  * Rows: the probe runs through the dataset API as an admin and as the sandboxed user. The user must see no VENUES
    value, since every path to VENUES goes through the empty sandbox.
  * Structure: the probe is preprocessed as the sandboxed user and given alias info, as compilation would. No field ref
    outside a sandbox stage may have VENUES as its physical source, since that compiles to the raw table name. This
    oracle does not depend on the driver rejecting the SQL.

  Verdicts per probe, worst first:

  * `:leak`      -- the sandboxed user sees VENUES data.
  * `:raw-ref`   -- the user's query references raw VENUES outside the sandbox. Here the database rejected it, but
                    the invariant is already broken and other drivers or query shapes may accept it.
  * `:enforced`  -- admin sees VENUES data, the sandboxed user sees none, no raw reference.
  * `:rejected`  -- the shape fails for the admin and the sandboxed user alike, with no raw reference.
  * `:user-only-error` -- admin succeeds, the sandboxed user's query fails, no raw reference.
  * `:user-ok-admin-error` -- the admin's run fails but the sandboxed user's succeeds with clean rows.
  * `:vacuous`   -- admin succeeds but sees no VENUES data, so the probe proves nothing."
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
   :users.id          (mt/id :users :id)})

(def ^:private ref-shapes
  "Ref shape name -> fn of the [[ids]] map returning a legacy ref that names VENUES.PRICE from a CHECKINS stage."
  (array-map
   ;; what the query builder produces
   :canonical-source-field          (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.venue-id i)}])
   ;; legacy forms normalization still accepts
   :legacy-fk->                     (fn [i] ["fk->" ["field-id" (:checkins.venue-id i)] ["field-id" (:venues.price i)]])
   :legacy-fk->-field               (fn [i] ["fk->" ["field" (:checkins.venue-id i) nil] ["field" (:venues.price i) nil]])
   ;; string ref plus source-field: implicit joins only create joins for integer refs
   :name-ref+source-field           (fn [i] ["field" "PRICE" {"base-type" "type/Integer" "source-field" (:checkins.venue-id i)}])
   ;; source-field-name / source-field-join-alias variants
   :source-field-name-matching      (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.venue-id i) "source-field-name" "VENUE_ID"}])
   :source-field-name-bogus         (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.venue-id i) "source-field-name" "NOPE"}])
   :source-field-join-alias-bogus   (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.venue-id i) "source-field-join-alias" "Nope"}])
   ;; refs that name the other table with no join at all
   :bare-id-off-table               (fn [i] ["field" (:venues.price i) nil])
   :bare-id-off-table+base-type     (fn [i] ["field" (:venues.price i) {"base-type" "type/Integer"}])
   :join-alias-of-implicit-join     (fn [i] ["field" (:venues.price i) {"join-alias" "VENUES__via__VENUE_ID"}])
   :field-literal                   (fn [_] ["field-literal" "PRICE" "type/Integer"])
   ;; source-field that is not the right FK
   :source-field-wrong-fk           (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.user-id i)}])
   :source-field-not-an-fk          (fn [i] ["field" (:venues.price i) {"source-field" (:checkins.id i)}])
   :source-field-from-venues        (fn [i] ["field" (:venues.price i) {"source-field" (:venues.category i)}])))

;;; ---------------------------------------------- Clause positions -----------------------------------------------

(defn- all-nil-in-col [n] (fn [rows] (every? #(nil? (nth % n)) rows)))
(defn- some-non-nil-in-col [n] (fn [rows] (boolean (some #(some? (nth % n)) rows))))
(defn- first-cell-zero? [rows] (contains? #{0 nil} (ffirst rows)))
(defn- first-cell-positive? [rows] (boolean (some-> (ffirst rows) pos?)))

(def ^:private positions
  "Clause position name -> {:query (fn [ids ref] inner-query), :clean? (fn [rows]), :dirty? (fn [rows])}. `:clean?`
  holds when the rows carry no VENUES data; `:dirty?` holds when they do."
  (array-map
   :fields
   {:query  (fn [i r] {:source-table (:checkins i) :fields [r] :limit 50})
    :clean? (all-nil-in-col 0) :dirty? (some-non-nil-in-col 0)}
   :breakout
   {:query  (fn [i r] {:source-table (:checkins i) :breakout [r] :aggregation [["count"]]})
    :clean? (all-nil-in-col 0) :dirty? (some-non-nil-in-col 0)}
   :filter
   {:query  (fn [i r] {:source-table (:checkins i) :aggregation [["count"]] :filter ["not-null" r]})
    :clean? first-cell-zero? :dirty? first-cell-positive?}
   :aggregation
   {:query  (fn [i r] {:source-table (:checkins i) :aggregation [["max" r]]})
    :clean? (all-nil-in-col 0) :dirty? (some-non-nil-in-col 0)}
   :expression
   {:query  (fn [i r] {:source-table (:checkins i) :expressions {"x" ["+" r 0]} :fields [["expression" "x"]] :limit 50})
    :clean? (all-nil-in-col 0) :dirty? (some-non-nil-in-col 0)}
   ;; an integer canonical ref creates the implicit join, then the ref under test rides along with it
   :fields-mixed-with-canonical
   {:query  (fn [i r] {:source-table (:checkins i)
                       :fields       [["field" (:venues.name i) {"source-field" (:checkins.venue-id i)}] r]
                       :limit        50})
    :clean? (all-nil-in-col 1) :dirty? (some-non-nil-in-col 1)}
   ;; the ref under test in a second stage, over a first stage that is plain CHECKINS
   :second-stage
   {:query  (fn [i r] {:source-query {:source-table (:checkins i)} :fields [r] :limit 50})
    :clean? (all-nil-in-col 0) :dirty? (some-non-nil-in-col 0)}
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
    :clean? (all-nil-in-col 0) :dirty? (some-non-nil-in-col 0)}
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
    :clean? first-cell-zero? :dirty? first-cell-positive?}
   ;; the ref under test as a parameter target
   :parameter
   {:query      (fn [i _r] {:source-table (:checkins i) :aggregation [["count"]]})
    :parameters (fn [_i r] [{:type "number/=" :target ["dimension" r] :value [1 2 3 4]}])
    :clean?     first-cell-zero? :dirty? first-cell-positive?}))

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

(defn- classify [{admin-rows :rows admin-error :error}
                 {user-rows :rows user-error :error}
                 {raw-refs :refs}
                 {:keys [clean? dirty?]}]
  (cond
    (and user-rows (dirty? user-rows))        :leak
    (seq raw-refs)                            :raw-ref
    admin-error                               (if user-error :rejected :user-ok-admin-error)
    (not (dirty? admin-rows))                 :vacuous
    user-error                                :user-only-error
    (clean? user-rows)                        :enforced
    :else                                     :leak))

(defn- run-probe [i shape-k position-k]
  (let [query     (probe-query i shape-k position-k)
        admin     (run-as :crowberto query)
        user      (run-as :rasta query)
        structure (raw-refs-to-table query (:venues i))]
    {:shape     shape-k
     :position  position-k
     :outcome   (classify admin user structure (positions position-k))
     :admin     admin
     :user      user
     :structure structure
     :query     query}))

(defn- do-with-empty-venues-sandbox!
  "Run `thunk` as the sandboxed :rasta user, with VENUES sandboxed to no rows and CHECKINS and USERS fully queryable."
  [thunk]
  (met/with-gtaps! {:gtaps      {:venues {:remappings {"price" ["variable" [:field (mt/id :venues :price) nil]]}}}
                    :attributes {"price" "999"}}
    (doseq [table [:checkins :users]]
      (data-perms/set-table-permission! &group (mt/id table) :perms/create-queries :query-builder))
    (thunk)))

(defn run-corpus!
  "Run every ref shape in every clause position. Returns a seq of probe results."
  []
  (do-with-empty-venues-sandbox!
   (fn []
     (let [i (ids)]
       (doall (for [shape-k (keys ref-shapes), position-k (keys positions)]
                (run-probe i shape-k position-k)))))))

(defn summarize
  "A shape x position grid of outcome keywords, for reading at the REPL."
  [results]
  (into (sorted-map)
        (map (fn [[shape rs]] [shape (into (array-map) (map (juxt :position :outcome)) rs)]))
        (group-by :shape results)))

;;; -------------------------------------------------- Tests -------------------------------------------------------

(deftest ^:synchronized baseline-probe-is-meaningful-test
  (testing "The canonical ref in a :fields clause sees VENUES data as admin and none as the sandboxed user"
    (mt/test-drivers #{:h2}
      (let [{:keys [outcome] :as result} (do-with-empty-venues-sandbox!
                                          #(run-probe (ids) :canonical-source-field :fields))]
        (is (= :enforced outcome) (pr-str result))))))

(deftest ^:synchronized field-ref-shapes-cannot-escape-sandbox-test
  (mt/test-drivers #{:h2}
    (doseq [{:keys [shape position outcome] :as result} (run-corpus!)]
      (testing (format "%s in %s" shape position)
        (testing "the sandboxed user sees no sandboxed rows"
          (is (not= :leak outcome) (pr-str (select-keys result [:user :query]))))
        (testing "the sandboxed user's query never references the raw sandboxed table"
          (is (not= :raw-ref outcome) (pr-str (select-keys result [:structure :user :query]))))))))
