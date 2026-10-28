(ns metabase-enterprise.sandbox.enforcement-test.policies.generated
  "Generated policies: a sandbox described as data (a *spec*), a generator of specs, and the derivation of a spec's
  specification, the restricted dataset an admin queries to get what the sandboxed user must get.

  A spec is `table -> table spec`, for SHOPS, REGIONS or both:

    {:shops {:kind :card
             :filters [{:column :tier :op := :value \"1\" :via :attribute}
                       {:column :price :op :< :value 3 :via :literal}]
             :hidden #{:price :secret}}}

  | kind         | sandbox                                                | filters                          | hidden      |
  |--------------|--------------------------------------------------------|----------------------------------|-------------|
  | `:table`     | the table itself, attributes as `variable` remappings  | 1-2, attribute `=`               | none        |
  | `:card`      | an MBQL card; literal filters in it, attributes as `dimension` remappings | 0-2, any      | SHOPS only  |
  | `:card-join` | a SHOPS card joining REGIONS as `R`, filtered on REGIONS columns | 1-2, any, on REGIONS     | SHOPS only  |
  | `:native`    | a SQL card `WHERE col = {{attr}}`                      | 1-2, attribute `=`               | SHOPS only  |

  An attribute filter's `:value` is the login attribute string; the sandbox coerces it to the column type as
  [[coerce]] does. A literal filter's `:value` is typed. Hidden columns are on SHOPS only: the SQL invariants and the
  name fallback of the differential identify hidden columns by name, and REGIONS shares names with SHOPS.

  [[policy]] turns a spec into a policy config in the shape of `policies/all`; [[restricted]] and [[varied]] derive
  the datasets; [[in-scope]] is the run's sample. `policies.generated-test` checks the derivation against the query
  processor and against the hand-written variants."
  (:require
   [clojure.string :as str]
   [clojure.test.check.generators :as gen]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.queries.generated :as queries.generated]
   [metabase.test :as mt]
   [metabase.test.data.interface :as tx]))

(set! *warn-on-reflection* true)

;;; --------------------------------------------------- Columns ----------------------------------------------------

(def filter-columns
  "Table -> the columns a sandbox may filter on. Not the canaries, and not OPENED_ON: dates are their own round."
  {:shops   [:id :tier :price :region_id :parent_shop_id :rating :name]
   :regions [:id :tier :name :hq_shop_id]})

(def ^:private hideable-columns
  "SHOPS columns a sandbox may hide: all but the primary key."
  [:name :tier :price :region_id :parent_shop_id :rating :opened_on :secret :secret_num])

(defn- text? [column] (isa? (dataset/base-types column) :type/Text))

(def ^:private absent-values
  "A value no row holds, by base type: an attribute or literal with it matches nothing."
  {:type/Integer 99 :type/BigInteger 99 :type/Float 9.25 :type/Text "Nope"})

(defn- absent-value [column]
  (some (fn [[t v]] (when (isa? (dataset/base-types column) t) v)) absent-values))

(defn- distinct-values
  "The distinct non-nil values of `table`.`column` in the base dataset, sorted."
  [table column]
  (sort (distinct (keep column (dataset/base-rows table)))))

;;; -------------------------------------------------- Operators ---------------------------------------------------

(def ^:private operators
  "Operator -> `{:pred (fn [cell value])}`, the rows a filter keeps, as the query processor computes them."
  {:=           {:pred (fn [cell v] (and (some? cell) (= cell v)))}
   ;; the SQL compiler adds `OR col IS NULL` to a not-equal filter, so NULL passes it
   :!=          {:pred (fn [cell v] (or (nil? cell) (not= cell v)))}
   :<           {:pred (fn [cell v] (and (some? cell) (< cell v)))}
   :>           {:pred (fn [cell v] (and (some? cell) (> cell v)))}
   :is-null     {:pred (fn [cell _] (nil? cell))}
   :not-null    {:pred (fn [cell _] (some? cell))}
   :starts-with {:pred (fn [cell v] (and (some? cell) (str/starts-with? cell v)))}})

(defn operators-for
  "The literal operators a filter on `column` may use."
  [column]
  (if (text? column)
    [:= :!= :is-null :not-null :starts-with]
    [:= :!= :< :> :is-null :not-null]))

(defn sample-values
  "Values a literal `op` filter on `table`.`column` is tested with: two sample values and the absent value, or `[nil]`
  for an operator that takes none."
  [table column op]
  (case op
    (:is-null :not-null) [nil]
    :starts-with         (map #(subs % 0 (min 2 (count %))) (take 2 (distinct-values table column)))
    (concat (take 2 (distinct-values table column)) [(absent-value column)])))

(defn- coerce
  "A login attribute string coerced to `column`'s type, as the sandbox middleware does
  (`attr-value->param-value`): parsed as a long for an integer column, as a double for a float column."
  [column s]
  (let [t (dataset/base-types column)]
    (cond
      (isa? t :type/Integer) (parse-long s)
      (isa? t :type/Float)   (parse-double s)
      :else                  s)))

(defn- filter-value [{:keys [via column value]}]
  (if (= via :attribute) (coerce column value) value))

(defn- attribute-name
  "The login attribute an attribute filter on `table`.`column` reads; per table, so two tables never share one."
  [table column]
  (str (name table) "_" (name column)))

;;; ------------------------------------------------- Derivation ---------------------------------------------------

(defn- filter-table
  "The table a table spec's filters name columns of."
  [table {:keys [kind]}]
  (if (= kind :card-join) :regions table))

(def ^:private regions-by-id
  (delay (into {} (map (juxt :id identity)) (dataset/base-rows :regions))))

(defn- keeps?
  "Does the sandbox `table-spec` on `table` keep `row`? A `:card-join` filters on the shop's region, nil (so every
  filter sees NULL) when the shop has none."
  [_table {:keys [kind filters]} row]
  (let [subject (if (= kind :card-join) (@regions-by-id (:region_id row)) row)]
    (every? (fn [{:keys [column op] :as f}]
              ((get-in operators [op :pred]) (get subject column) (filter-value f)))
            filters)))

(defn restricted-rows
  "The base rows of `table` that `spec` keeps, as maps."
  [spec table]
  (if-let [table-spec (spec table)]
    (filterv #(keeps? table table-spec %) (dataset/base-rows table))
    (dataset/base-rows table)))

(defn spec-name
  "A stable name for `spec`, from its hash, which is the same in every JVM."
  [spec]
  (format "shops-spec-%08x" (hash spec)))

(def restricted
  "The restricted dataset `spec` describes: the base dataset with the rows each sandbox drops deleted and the columns it
  hides nulled. One definition object per spec."
  (memoize
   (fn [spec]
     (apply tx/transformed-dataset-definition (spec-name spec) dataset/shops
            (concat (for [[table table-spec] spec]
                      (dataset/keep-rows (name table) #(keeps? table table-spec %)))
                    (for [[table {:keys [hidden]}] spec
                          :when (seq hidden)]
                      (dataset/null-columns (name table) hidden)))))))

(def varied
  "The dataset that changes only what `spec` hides, which must leave every sandboxed result alone: every dropped row
  gets new data values (not its filter columns, so it stays dropped), the dropped row with the highest id is deleted,
  and every row gets new values in the hidden columns that are not filter columns."
  (memoize
   (fn [spec]
     (apply tx/transformed-dataset-definition (str (spec-name spec) "-varied") dataset/shops
            (for [[table {:keys [filters hidden] :as table-spec}] spec
                  :let [filter-cols  (if (= :card-join (:kind table-spec)) #{} (set (map :column filters)))
                        changeable   (remove filter-cols (dataset/varyable-columns table))
                        dropped      (into (sorted-set)
                                           (comp (remove #(keeps? table table-spec %)) (map :id))
                                           (dataset/base-rows table))
                        hidden-vary  (filter (set hidden) changeable)]
                  transform [(dataset/update-rows (name table) #(contains? dropped (:id %)) #(dataset/vary % changeable))
                             (dataset/update-rows (name table) (constantly true) #(dataset/vary % hidden-vary))
                             (dataset/keep-rows (name table) #(not= (:id %) (last dropped)))]]
              transform)))))

;;; ---------------------------------------------------- Gtaps -----------------------------------------------------

(defn- field-ref
  ([table column] (field-ref table column nil))
  ([table column join-alias]
   [:field (mt/id table column) (when join-alias {:join-alias join-alias})]))

(defn- mbql-filter [{:keys [op] :as f} ref]
  (case op
    (:is-null :not-null) [op ref]
    [op ref (filter-value f)]))

(defn- combine [clauses]
  (case (count clauses)
    0 nil
    1 (first clauses)
    (into [:and] clauses)))

(defn- visible-columns [table hidden]
  (remove (set hidden) (dataset/columns table)))

(defn card-query
  "The sandbox card query of the `:card` or `:card-join` `table-spec` on `table`, for the current database. Attribute
  filters are not in it: they reach the card as parameters."
  [table {:keys [kind filters hidden]}]
  (let [join-alias (when (= kind :card-join) "R")
        literals   (combine (for [{:keys [via column] :as f} filters
                                  :when (= via :literal)]
                              (mbql-filter f (field-ref (if join-alias :regions table) column join-alias))))]
    {:database (mt/id)
     :type     :query
     :query    (cond-> {:source-table (mt/id table)}
                 join-alias     (assoc :joins [{:source-table (mt/id :regions)
                                                :alias        join-alias
                                                :condition    [:= (field-ref :shops :region_id) (field-ref :regions :id join-alias)]
                                                :fields       :none}])
                 (or join-alias (seq hidden)) (assoc :fields (mapv #(field-ref table %) (visible-columns table hidden)))
                 literals       (assoc :filter literals))}))

(defn- attribute-filters [filters]
  (filter #(= :attribute (:via %)) filters))

(defn- native-query
  "A native sandbox card on `table` returning its visible columns, each attribute filter a `{{tag}}`."
  [table {:keys [filters hidden]}]
  (let [attrs (attribute-filters filters)]
    (mt/native-query
     {:query         (str "SELECT " (str/join ", " (map (comp mt/format-name name) (visible-columns table hidden)))
                          " FROM " (mt/format-name (name table))
                          (when (seq attrs)
                            (str " WHERE " (str/join " AND " (for [{:keys [column]} attrs]
                                                               (format "%s = {{%s}}" (mt/format-name (name column))
                                                                       (attribute-name table column)))))))
      :template-tags (into {} (for [{:keys [column]} attrs
                                    :let [tag (attribute-name table column)]]
                                [tag {:name tag :display-name tag :type (if (text? column) :text :number) :required true}]))})))

(defn- gtap
  "The `met/with-gtaps!` entry for `table-spec` on `table`, in the current database."
  [table {:keys [kind filters] :as table-spec}]
  (let [attrs      (attribute-filters filters)
        remap      (fn [kind-k ref-fn]
                     (into {} (for [{:keys [column]} attrs]
                                [(attribute-name (filter-table table table-spec) column) [kind-k (ref-fn column)]])))
        remappings (case kind
                     :table     (remap "variable" #(field-ref table %))
                     :card      (remap "dimension" #(field-ref table %))
                     :card-join (remap "dimension" #(field-ref :regions % "R"))
                     :native    (into {} (for [{:keys [column]} attrs
                                               :let [tag (attribute-name table column)]]
                                           [tag ["variable" ["template-tag" tag]]])))]
    (cond-> {}
      (#{:card :card-join} kind) (assoc :query (card-query table table-spec))
      (= :native kind)           (assoc :query (native-query table table-spec))
      (seq remappings)           (assoc :remappings remappings))))

(defn policy
  "The policy config for `spec`, in the shape of a `policies/all` entry."
  [spec]
  {:spec       spec
   :gtaps      (fn [] (into {} (for [[table table-spec] spec] [table (gtap table table-spec)])))
   :attributes (into {} (for [[table table-spec] spec
                              {:keys [column value]} (attribute-filters (:filters table-spec))]
                          [(attribute-name (filter-table table table-spec) column) value]))
   :restricted (restricted spec)
   :varied     (varied spec)
   :hidden     (into {} (for [[table {:keys [hidden]}] spec :when (seq hidden)] [table (set hidden)]))
   :sandboxed  (set (keys spec))
   :feature    (when (some #(= :native (:kind %)) (vals spec)) :native-parameters)})

;;; -------------------------------------------------- Generators --------------------------------------------------

(defn- gen-sample-value
  "Mostly a value some row holds, sometimes one none does."
  [table column]
  (gen/frequency [[8 (gen/elements (distinct-values table column))]
                  [1 (gen/return (absent-value column))]]))

(defn- gen-attribute-filter [table column]
  (gen/fmap (fn [v] {:column column :op := :value (str v) :via :attribute})
            (gen-sample-value table column)))

(defn- gen-literal-filter [table column]
  (gen/bind (gen/elements (operators-for column))
            (fn [op]
              (case op
                (:is-null :not-null) (gen/return {:column column :op op :via :literal})
                :starts-with         (gen/fmap (fn [v] {:column column :op op :via :literal :value (subs v 0 (min 2 (count v)))})
                                               (gen/elements (distinct-values table column)))
                (gen/fmap (fn [v] {:column column :op op :via :literal :value v})
                          (gen-sample-value table column))))))

(defn- gen-filters
  "Between `lo` and `hi` filters on distinct columns of `table`, each through a via from `vias`."
  [table lo hi vias]
  (gen/bind (gen/vector-distinct (gen/elements (filter-columns table)) {:min-elements lo :max-elements hi})
            (fn [columns]
              (gen/fmap vec (apply gen/tuple (for [column columns]
                                               (gen/bind (gen/elements vias)
                                                         #(if (= % :attribute)
                                                            (gen-attribute-filter table column)
                                                            (gen-literal-filter table column)))))))))

(defn- gen-hidden [table]
  (if (= table :shops)
    (gen/set (gen/elements hideable-columns) {:max-elements 3})
    (gen/return #{})))

(defn- gen-table-spec
  "A table spec of `kind` on `table`."
  [table kind]
  (gen/such-that
   (fn [{:keys [filters hidden]}] (or (seq filters) (seq hidden)))
   (gen/let [filters (case kind
                       :table     (gen-filters table 1 2 [:attribute])
                       :native    (gen-filters table 1 2 [:attribute])
                       :card      (gen-filters table (if (= table :shops) 0 1) 2 [:attribute :literal])
                       :card-join (gen-filters :regions 1 2 [:attribute :literal]))
             hidden  (if (= kind :table) (gen/return #{}) (gen-hidden table))]
     {:kind    kind
      :filters filters
      ;; a dimension remapping needs its column in the card's result
      :hidden  (if (= kind :card)
                 (apply disj hidden (map :column (attribute-filters filters)))
                 hidden)})
   100))

(def kinds
  "The strata of generated policies: one per kind of SHOPS sandbox, and two-table sandboxes."
  [:table :card :card-join :native :two-tables])

(defn- gen-spec [kind]
  (if (= kind :two-tables)
    (gen/let [shops-kind   (gen/elements [:table :card :native])
              regions-kind (gen/elements [:table :card :native])
              shops        (gen-table-spec :shops shops-kind)
              regions      (gen-table-spec :regions regions-kind)]
      {:shops shops :regions regions})
    (gen/fmap (fn [table-spec] {:shops table-spec}) (gen-table-spec :shops kind))))

(defn spec-kind
  "The stratum `spec` belongs to."
  [spec]
  (if (= 2 (count spec)) :two-tables (:kind (:shops spec))))

(defn sample
  "`n` distinct specs of `kind`, the same for the same `seed`."
  [kind n seed]
  (->> (range 500)
       ;; a seed per kind and index, so the kinds do not repeat each other's choices
       (map #(gen/generate (gen-spec kind) 30 (hash [seed kind %])))
       distinct
       (take n)
       vec))

(defn policies-per-kind
  "How many generated policies per kind a run uses; `MB_SANDBOX_GEN_POLICIES` overrides."
  []
  (queries.generated/env-long "MB_SANDBOX_GEN_POLICIES" 2))

(defn in-scope
  "The generated policies of this run, by kind, then sample order. `MB_SANDBOX_GEN_SEED` changes the sample."
  []
  (vec (for [kind kinds, spec (sample kind (policies-per-kind) (queries.generated/seed))] spec)))
