(ns metabase-enterprise.sandbox.enforcement-test.queries.generated
  "Seeded random queries from the Lib generator over the dataset, converted to legacy MBQL, plus a catalogue of ref
  encodings a client could send but the query builder never produces ([[mutation-kinds]]). [[gen-case]] is the
  test.check generator of one case; the harness generates, mutates and runs it. [[translate-query]] rewrites a query
  built against one database to name the same things in another."
  (:require
   [clojure.test.check.generators :as gen]
   [clojure.walk :as walk]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.queries.formats :as formats]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.lib.test-util.generators :as lib.tu.gen]
   [metabase.test :as mt]
   [metabase.test.util.random :as tu.rng]
   [metabase.util :as u]))

(set! *warn-on-reflection* true)

;;; --------------------------------------------- Generated queries -----------------------------------------------
;;;
;;; Random valid queries from the lib generator, started from each source kind, converted to legacy MBQL the way a
;;; client would send them, then (step 6) rewritten into non-standard ref encodings. test.check shrinks a failing
;;; case to the shortest walk and fewest mutations that still fail.

(defn translate-query
  "`legacy-query`, built against database `from`, rewritten to name the same tables, fields and cards in database
  `to`. Only ids in table, field and card positions are touched."
  [legacy-query from to]
  (let [tbl     (into {} (for [t dataset/tables] [(dataset/tid from t) (dataset/tid to t)]))
        fld     (into {} (for [[t cols] dataset/columns, c cols] [(dataset/fid from t c) (dataset/fid to t c)]))
        crd     (into {} (for [k (keys (:cards from))] [(dataset/card-source from k) (dataset/card-source to k)]))
        xf-opts (fn [opts] (cond-> opts (and (map? opts) (int? (:source-field opts))) (update :source-field #(fld % %))))]
    (-> (walk/postwalk
         (fn [x]
           (cond
             (and (map? x) (contains? x :source-table))
             (update x :source-table #(or (tbl %) (crd %) %))

             (and (vector? x) (= :field (first x)) (int? (second x)))
             [:field (fld (second x) (second x)) (xf-opts (nth x 2 nil))]

             (and (vector? x) (= :field-id (first x)) (int? (second x)))
             [:field-id (fld (second x) (second x))]

             :else x))
         legacy-query)
        (assoc :database (:db to)))))

(def ^:private source-kinds
  [:table/shops :table/orders :table/items :table/regions :card/shops :model/shops :card/orders-implicit :nested/shops])

(defn- start-query
  "A lib query over the current database to start a random walk from."
  [ids source]
  (let [mp    (mt/metadata-provider)
        table (fn [t] (lib/query mp (lib.metadata/table mp (dataset/tid ids t))))
        card  (fn [k] (lib/query mp (lib.metadata/card mp (get-in ids [:cards k]))))]
    (case source
      :table/shops          (table :shops)
      :table/orders         (table :orders)
      :table/items          (table :items)
      :table/regions        (table :regions)
      :card/shops           (card :card/shops)
      :model/shops          (card :model/shops)
      :card/orders-implicit (card :card/orders-implicit)
      :nested/shops         (lib/append-stage (table :shops)))))

(defn generate-legacy-query
  "A random walk of `n-steps` from `source`, seeded, as a legacy query over the current database."
  [ids {:keys [source seed n-steps]}]
  (let [mp (mt/metadata-provider)]
    (binding [tu.rng/*generator*             (java.util.Random. ^long seed)
              lib.tu.gen/*safe-for-old-refs* false
              ;; SQL question results are not sandboxed, by design; see [[saved-sql-probes]]
              lib.tu.gen/*available-cards*   (mapv #(lib.metadata/card mp %) (vals (dissoc (:cards ids) :native/shops)))]
      (-> (lib.tu.gen/random-queries-from (start-query ids source) n-steps)
          last
          lib/->legacy-MBQL))))

(defn- field-ref? [x]
  (and (vector? x) (= :field (first x)) (or (int? (second x)) (string? (second x)))))

(defn- field-id->column
  "Field id -> `[table column]` for the ids map."
  [ids]
  (into {} (map (fn [[tc id]] [id tc])) (:fields ids)))

(defn- ref-column
  "`[table column]` a field ref names, when it names one by id."
  [ids [_ id-or-name]]
  (when (int? id-or-name) ((field-id->column ids) id-or-name)))

(defn- nth-field-ref
  "The k-th (mod count) field ref of `x` in traversal order, refs to SHOPS first; nil when there are none."
  [ids x k]
  (let [refs  (atom [])
        _     (walk/postwalk (fn [v] (when (field-ref? v) (swap! refs conj v)) v) x)
        shops (filter #(= :shops (first (ref-column ids %))) @refs)
        all   (concat shops (remove #(= :shops (first (ref-column ids %))) @refs))]
    (when (seq all)
      (nth all (mod k (count all))))))

(defn- replace-ref
  "`x` with every occurrence of field ref `r` replaced by `(f r)`."
  [x r f]
  (walk/postwalk (fn [v] (if (= v r) (f v) v)) x))

(defn- name-ref
  "`r` rewritten as a name ref, with or without its base type."
  [ids [_ _ opts :as r] base-type?]
  (if-let [[table column] (ref-column ids r)]
    [:field (dataset/fname ids table column) (cond-> (or opts {})
                                               true       (dissoc :source-field :source-field-name :source-field-join-alias :join-alias)
                                               base-type? (assoc :base-type (dataset/base-types column)))]
    r))

(defn- fk-column-name [ids opts]
  (when-let [[table column] (ref-column ids [:field (:source-field opts) nil])]
    (dataset/fname ids table column)))

(def ^:private mutation-kinds
  "Mutation name -> fn of ids, the outer legacy query and the k-th ref, returning the mutated outer query. Each is a
  way a client could encode a ref that the query builder never produces."
  (array-map
   :id->name                   (fn [ids q r] (replace-ref q r #(name-ref ids % true)))
   :id->name-no-base-type      (fn [ids q r] (replace-ref q r #(name-ref ids % false)))
   :source-field-name          (fn [ids q [_ id opts :as r]]
                                 (if-let [n (and (:source-field opts) (fk-column-name ids opts))]
                                   (replace-ref q r (constantly [:field id (assoc opts :source-field-name n)]))
                                   q))
   :source-field-name-bogus    (fn [_ q [_ id opts :as r]]
                                 (if (:source-field opts)
                                   (replace-ref q r (constantly [:field id (assoc opts :source-field-name "NOPE")]))
                                   q))
   :source-field-join-bogus    (fn [_ q [_ id opts :as r]]
                                 (if (:source-field opts)
                                   (replace-ref q r (constantly [:field id (assoc opts :source-field-join-alias "Nope")]))
                                   q))
   :fk->                       (fn [_ q [_ id opts :as r]]
                                 (if (and (int? id) (:source-field opts))
                                   (replace-ref q r (constantly [:fk-> [:field-id (:source-field opts)] [:field-id id]]))
                                   q))
   :field-literal              (fn [ids q r]
                                 (if-let [[table column] (ref-column ids r)]
                                   (replace-ref q r (constantly [:field-literal (dataset/fname ids table column) (dataset/base-types column)]))
                                   q))
   :joined-field               (fn [_ q [_ id opts :as r]]
                                 (if (and (int? id) (:join-alias opts))
                                   (replace-ref q r (constantly [:joined-field (:join-alias opts) [:field-id id]]))
                                   q))
   :drop-join-alias            (fn [_ q [_ id opts :as r]]
                                 (if (:join-alias opts)
                                   (replace-ref q r (constantly [:field id (not-empty (dissoc opts :join-alias))]))
                                   q))
   :bogus-join-alias           (fn [_ q [_ id opts :as r]]
                                 (replace-ref q r (constantly [:field id (assoc opts :join-alias "Nope")])))
   :drop-source-field          (fn [_ q [_ id opts :as r]]
                                 (if (:source-field opts)
                                   (replace-ref q r (constantly [:field id (not-empty (dissoc opts :source-field :source-field-name :source-field-join-alias))]))
                                   q))
   :duplicate-into-filter      (fn [_ q r]
                                 (update-in q [:query :filter] #(if % [:and % [:not-null r]] [:not-null r])))
   :outer-stage                (fn [_ q r]
                                 (assoc q :query {:source-query (:query q) :fields [r]}))
   :outer-stage-name-ref       (fn [ids q r]
                                 (assoc q :query {:source-query (:query q) :fields [(name-ref ids r true)]}))
   :parameter                  (fn [ids q r]
                                 (if-let [[_ column] (ref-column ids r)]
                                   (let [text? (= :type/Text (dataset/base-types column))]
                                     (update q :parameters (fnil conj [])
                                             {:type   (if text? "string/!=" "number/!=")
                                              :value  [(if text? "__none__" -424242)]
                                              :target [:dimension r]}))
                                   q))))

(defn apply-mutations
  "Rewrite refs of `legacy-query` per `mutations`, each `[kind k]`: the k-th field ref (SHOPS refs first) gets the
  kind's encoding. Mutations that do not apply to the chosen ref leave the query unchanged."
  [ids legacy-query mutations]
  (reduce (fn [q [kind k]]
            (if-let [r (nth-field-ref ids (:query q) k)]
              ((mutation-kinds kind) ids q r)
              q))
          legacy-query
          mutations))

(def gen-case
  "A test.check generator of one case: a source to start from, a seed, a walk length, up to three mutations and a
  query format."
  (gen/hash-map :source    (gen/elements source-kinds)
                :seed      (gen/no-shrink gen/large-integer)
                :n-steps   (gen/choose 1 12)
                :mutations (gen/vector (gen/tuple (gen/elements (keys mutation-kinds)) gen/nat) 0 3)
                ;; shrinks toward `:legacy`, the first format
                :format    (gen/elements (keys formats/query-formats))))

(defn env-long
  "The long value of environment variable `var-name`, or `default`."
  [var-name default]
  (or (some-> (System/getenv var-name) parse-long) default))

(defn cases-per-policy
  "How many generated cases to run per policy; `MB_SANDBOX_GEN_CASES` overrides."
  []
  (env-long "MB_SANDBOX_GEN_CASES" 40))

(defn seed
  "The test.check seed; `MB_SANDBOX_GEN_SEED` overrides."
  []
  (env-long "MB_SANDBOX_GEN_SEED" 20261026))

(def fake-ids
  "An ids map with made-up ids, for testing the mutation catalogue without a database."
  {:db     1
   :tables (zipmap dataset/tables (range 100 104))
   :fields (into {} (map-indexed (fn [i tc] [tc (+ 200 i)])) (for [[t cols] dataset/columns, c cols] [t c]))
   :names  (into {} (for [[t cols] dataset/columns, c cols] [[t c] (u/upper-case-en (name c))]))
   :cards  {:card/shops 5 :model/shops 6 :card/orders-implicit 7 :model/orders-implicit 8 :native/shops 9}})
