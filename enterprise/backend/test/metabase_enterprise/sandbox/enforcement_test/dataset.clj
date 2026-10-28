(ns metabase-enterprise.sandbox.enforcement-test.dataset
  "A small synthetic dataset for sandbox enforcement tests, built so that the sandboxed table SHOPS plays every role a
  table can play in a query: query source, explicit join target, implicit-join target through one FK (ORDERS.SHOP_ID),
  target of two FKs from one table (ORDERS.SHOP_ID and ORDERS.RETURN_SHOP_ID), end of a multi-hop chain (ITEMS -> ORDERS
  -> SHOPS), self-referencing (SHOPS.PARENT_SHOP_ID), source of an FK out (SHOPS.REGION_ID), and one half of a two-table
  cycle (REGIONS.HQ_SHOP_ID). Every table has ID and NAME, so name refs are ambiguous on purpose.

  Every SHOPS and REGIONS row carries two canary values that decode back to the row: a text token in SECRET and a
  number in SECRET_NUM. A canary from a row the sandbox hides, anywhere in a response the sandboxed user gets, is a
  leak. See [[canary-hits]].

  The dataset declares no foreign keys: the SQL test loader adds FK constraints before it loads rows, which would
  reject the cycle and the dangling keys of the restricted variants. [[ensure-fk-metadata!]] sets the FK metadata by
  hand after a dataset is loaded.

  The restricted variants ([[shops-tier1]] and friends) are the same dataset with a sandbox applied by hand: rows
  deleted or columns nulled. They are the specification of every policy: a query run as a sandboxed user on the base
  dataset must return what an admin gets from the matching variant. [[variant-contents]] tables what each holds.

  [[tables]], [[columns]], [[base-types]] and [[fks]] describe the schema for the other namespaces; [[ids-map]] and the
  accessors [[tid]], [[fid]], [[fname]] and [[card-source]] name the loaded copy's ids."
  (:require
   [clojure.walk :as walk]
   [metabase.test :as mt]
   [metabase.test.data.interface :as tx]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; ------------------------------------------------- Canaries -----------------------------------------------------

(def ^:private canary-nonce "5b0dca7e")

(def ^:private canary-num-step 7919)

(def ^:private canary-table-codes
  "Table -> [text code, numeric base]. The numeric base keeps the two tables' canaries apart and recognizable."
  {:shops   ["S" 900000000000]
   :regions ["R" 800000000000]})

(defn canary-text
  "The SECRET value of row `id` of `table`."
  [table id]
  (format "CANARY-%s%d-%s" (first (canary-table-codes table)) id canary-nonce))

(defn canary-num
  "The SECRET_NUM value of row `id` of `table`: the table's base plus the step times 2^id. Powers of two keep sums and
  averages of valid canaries (which result metadata computes over the returned rows) from decoding as another row."
  [table id]
  (+ (second (canary-table-codes table)) (* canary-num-step (bit-shift-left 1 id))))

(def ^:private canary-text-pattern (re-pattern (str "CANARY-([SR])(\\d+)-" canary-nonce)))

;; 12-digit numbers starting with 8 or 9, or the same printed as a double such as 9.00000007919E11

(def ^:private canary-num-pattern #"(?<![\d.])[89](?:\d{11}|\.\d+E11)(?![\d.])")

(def ^:private code->table (into {} (map (fn [[table [code _]]] [code table])) canary-table-codes))

(defn- decode-canary-num [s]
  (let [n (long (bigdec s))]
    (some (fn [[table [_ base]]]
            (let [d (- n base)]
              (when (and (pos? d) (< d (* 1000000 canary-num-step)) (zero? (mod d canary-num-step)))
                (let [q (quot d canary-num-step)]
                  (when (= 1 (Long/bitCount q))
                    [table (Long/numberOfTrailingZeros q)])))))
          canary-table-codes)))

(defn- canaries-in-string
  "`[table id kind]` for every canary in `s`; `kind` is `:text` or `:number`."
  [s]
  (concat (for [[_ code id] (re-seq canary-text-pattern s)]
            [(code->table code) (parse-long id) :text])
          (for [[table id] (keep decode-canary-num (re-seq canary-num-pattern s))]
            [table id :number])))

(defn canary-hits
  "Every canary in `x`, a response body or any other value: a seq of `{:table t, :id n, :kind k, :path p}` where `k`
  is `:text` or `:number` and `p` is the path to the string or number that held it. Scalars are checked as strings,
  so numbers printed as doubles and canaries inside error text or metadata are found too."
  [x]
  (let [hits (volatile! [])]
    (letfn [(scan [path v]
              (cond
                (map? v)        (doseq [[k child] v] (scan (conj path k) child))
                (sequential? v) (doall (map-indexed (fn [i child] (scan (conj path i) child)) v))
                (or (string? v) (number? v))
                (doseq [[table id kind] (canaries-in-string (str v))]
                  (vswap! hits conj {:table table :id id :kind kind :path path}))
                :else           nil))]
      (scan [] (walk/keywordize-keys x)))
    @hits))

(defn canary-ids
  "The set of `[table id]` whose canaries appear anywhere in `x`."
  [x]
  (into #{} (map (juxt :table :id)) (canary-hits x)))

;;; -------------------------------------------------- Dataset -----------------------------------------------------

(defn- text [n] {:field-name n :base-type :type/Text})

(defn- int-col [n] {:field-name n :base-type :type/Integer})

(defn- pk [] {:field-name "id" :base-type :type/Integer :pk? true})

(defn- big [n] {:field-name n :base-type :type/BigInteger})

(defn- float-col [n] {:field-name n :base-type :type/Float})

(defn- date-col [n] {:field-name n :base-type :type/Date})

(defn- with-ratings-and-dates
  "Append each SHOPS row's RATING, its tier plus one half (exact in binary floating point), and OPENED_ON, in 2024 with
  the tier as month and the id as day, so that each tier is its own month."
  [rows]
  (mapv (fn [[id _name tier :as row]] (conj row (+ tier 0.5) (java.time.LocalDate/of 2024 (int tier) (int id)))) rows))

(defn- with-canaries
  "Append the canary columns' values to each row of `table`, where the row's id is its first value."
  [table rows]
  (mapv (fn [[id :as row]] (conj row (canary-text table id) (canary-num table id))) rows))

(mt/defdataset shops
  "See the namespace docstring. Columns in row order."
  [["regions"
    [(pk) (text "name") (int-col "tier") (int-col "hq_shop_id") (text "secret") (big "secret_num")]
    (with-canaries :regions
      [[1 "North" 1 1]
       [2 "South" 2 3]
       [3 "East"  1 5]
       [4 "West"  2 nil]])]
   ["shops"
    [(pk) (text "name") (int-col "tier") (int-col "price") (int-col "region_id") (int-col "parent_shop_id")
     (float-col "rating") (date-col "opened_on") (text "secret") (big "secret_num")]
    (with-canaries :shops
      (with-ratings-and-dates
        [[1 "Alpha" 1 1 1   nil]
         [2 "Beta"  2 2 1   1]
         [3 "Gamma" 1 3 2   1]
         [4 "Delta" 2 1 2   2]
         [5 "Alpha" 1 2 3   3]
         [6 "Zeta"  2 3 3   4]
         [7 "Eta"   1 1 4   nil]
         [8 "Theta" 2 2 nil 7]]))]
   ["orders"
    [(pk) (text "name") (int-col "shop_id") (int-col "return_shop_id") (int-col "amount")]
    [[1  "o1"  1 nil 10]
     [2  "o2"  2 1   20]
     [3  "o3"  3 nil 30]
     [4  "o4"  4 2   40]
     [5  "o5"  5 6   50]
     [6  "o6"  6 nil 60]
     [7  "o7"  7 8   70]
     [8  "o8"  8 nil 80]
     [9  "o9"  1 3   90]
     [10 "o10" 2 4   15]
     [11 "o11" 3 5   25]
     [12 "o12" 4 7   35]]]
   ["items"
    [(pk) (text "name") (int-col "order_id") (int-col "qty")]
    (mapv (fn [id order-id] [id (str "i" id) order-id id])
          (range 1 13)
          [1 1 2 3 4 5 6 7 8 9 10 12])]])

(def fks
  "Foreign keys of the dataset as `[from-table from-column to-table to-column]`, in the form [[ensure-fk-metadata!]]
  and query builders need."
  [[:orders  :shop_id        :shops   :id]
   [:orders  :return_shop_id :shops   :id]
   [:items   :order_id       :orders  :id]
   [:shops   :region_id      :regions :id]
   [:shops   :parent_shop_id :shops   :id]
   [:regions :hq_shop_id     :shops   :id]])

(defn ensure-fk-metadata!
  "Mark every entry of [[fks]] as a foreign key in the metadata of the current test database, the way sync would from
  a real constraint. Idempotent. Call it inside `mt/dataset` right after loading any variant of the dataset."
  []
  (doseq [[from-table from-col to-table to-col] fks]
    (t2/update! :model/Field (mt/id from-table from-col)
                {:semantic_type      :type/FK
                 :fk_target_field_id (mt/id to-table to-col)})))

;;; ---------------------------------------------------- Ids -------------------------------------------------------

(def tables
  "The tables, in the order the harness grants permissions on them."
  [:regions :shops :orders :items])

(def columns
  "Table -> its columns, in row order."
  {:regions [:id :name :tier :hq_shop_id :secret :secret_num]
   :shops   [:id :name :tier :price :region_id :parent_shop_id :rating :opened_on :secret :secret_num]
   :orders  [:id :name :shop_id :return_shop_id :amount]
   :items   [:id :name :order_id :qty]})

(def base-types
  "Column -> its base type, for name refs and field literals."
  {:id :type/Integer :name :type/Text :tier :type/Integer :price :type/Integer :amount :type/Integer :qty :type/Integer
   :region_id :type/Integer :parent_shop_id :type/Integer :hq_shop_id :type/Integer :shop_id :type/Integer
   :return_shop_id :type/Integer :order_id :type/Integer :secret :type/Text :secret_num :type/BigInteger
   :rating :type/Float :opened_on :type/Date})

(defn ids-map
  "Every table, field and card id of the current database, by name, plus the physical column names."
  [cards]
  {:db     (mt/id)
   :tables (into {} (for [t tables] [t (mt/id t)]))
   :fields (into {} (for [[t cols] columns, c cols] [[t c] (mt/id t c)]))
   :names  (into {} (for [[t cols] columns, c cols] [[t c] (mt/format-name (name c))]))
   :cards  cards})

(defn tid
  "The id of `table` in the database `ids` describes."
  [ids table]
  (get-in ids [:tables table]))

(defn fid
  "The id of `table`.`column` in the database `ids` describes."
  [ids table column]
  (get-in ids [:fields [table column]]))

(defn fname
  "The physical name of `table`.`column` as the driver stores it."
  [ids table column]
  (get-in ids [:names [table column]]))

(defn card-source
  "The `card__N` source-table string for the card `card-k` names in `ids`."
  [ids card-k]
  (str "card__" (get-in ids [:cards card-k])))

;;; -------------------------------------------- Restricted variants -----------------------------------------------

(defn- row->map [{:keys [field-definitions]} row]
  (zipmap (map (comp keyword :field-name) field-definitions) row))

(defn- map->row [{:keys [field-definitions]} m]
  (mapv (comp m keyword :field-name) field-definitions))

(defn- keep-rows
  "Transform fn: keep only the rows of `table-name` for which `pred` of the row as a map is truthy."
  [table-name pred]
  (tx/transform-dataset-update-table
   table-name
   :table (fn [tabledef]
            (update tabledef :rows #(filterv (comp pred (partial row->map tabledef)) %)))))

(defn- null-columns
  "Transform fn: set `columns` to nil in every row of `table-name`."
  [table-name columns]
  (tx/transform-dataset-update-table
   table-name
   :table (fn [tabledef]
            (update tabledef :rows
                    #(mapv (fn [row] (map->row tabledef (merge (row->map tabledef row) (zipmap columns (repeat nil)))))
                           %)))))

(defn- update-rows
  "Transform fn: replace each row of `table-name` for which `pred` of the row as a map is truthy with `(f row-map)`."
  [table-name pred f]
  (tx/transform-dataset-update-table
   table-name
   :table (fn [tabledef]
            (update tabledef :rows
                    #(mapv (fn [row]
                             (let [m (row->map tabledef row)]
                               (if (pred m) (map->row tabledef (f m)) row)))
                           %)))))

(defn- vary-hidden-columns
  "A SHOPS row map with PRICE and the canary columns changed. The new values are not canaries."
  [{:keys [id price] :as shop}]
  (assoc shop
         :price      (+ 100 price)
         :secret     (str "VARIED-" id)
         :secret_num (+ 100000 id)))

(def shops-tier2-varied
  "The base dataset with only the tier 2 SHOPS rows changed: every column but id and tier, a later opening date, and
  shop 8 removed. A sandbox that hides tier 2 must give the same answers here as on the base dataset."
  (tx/transformed-dataset-definition "shops-tier2-varied" shops
                                     (update-rows "shops" #(= 2 (:tier %))
                                                  #(-> (vary-hidden-columns %)
                                                       (assoc :name      (str "Varied " (:id %))
                                                              :rating    9.5
                                                              :opened_on (java.time.LocalDate/of 2025 6 (int (:id %))))))
                                     (keep-rows "shops" #(not= 8 (:id %)))))

(def shops-hidden-cols-varied
  "The base dataset with PRICE and the canary columns of every SHOPS row changed. A sandbox that hides those columns
  must give the same answers here as on the base dataset."
  (tx/transformed-dataset-definition "shops-hidden-cols-varied" shops
                                     (update-rows "shops" (constantly true) vary-hidden-columns)))

(def tier-1-regions
  "IDs of the REGIONS rows with tier 1."
  #{1 3})

(def shops-tier1
  "SHOPS restricted to tier 1 rows."
  (tx/transformed-dataset-definition "shops-tier1" shops
                                     (keep-rows "shops" #(= 1 (:tier %)))))

(def shops-tier1-regions-tier1
  "SHOPS and REGIONS both restricted to tier 1 rows."
  (tx/transformed-dataset-definition "shops-tier1-regions-tier1" shops
                                     (keep-rows "shops" #(= 1 (:tier %)))
                                     (keep-rows "regions" #(= 1 (:tier %)))))

(def shops-in-tier1-regions
  "SHOPS restricted to the shops whose region is tier 1, as a sandbox card that joins REGIONS would."
  (tx/transformed-dataset-definition "shops-in-tier1-regions" shops
                                     (keep-rows "shops" #(contains? tier-1-regions (:region_id %)))))

(def shops-empty
  "No SHOPS rows at all, as a sandbox whose filter matches nothing."
  (tx/transformed-dataset-definition "shops-empty" shops
                                     (keep-rows "shops" (constantly false))))

(def ^:private hidden-columns [:price :secret :secret_num])

(def shops-hide-price
  "Every SHOPS row, with PRICE and the canary columns hidden, as a sandbox card that leaves them out would. A hidden
  column reads as NULL here; Metabase may instead drop it or fail, which the classifier must allow for."
  (tx/transformed-dataset-definition "shops-hide-price" shops
                                     (null-columns "shops" hidden-columns)))

(def shops-tier1-hide-price
  "Tier 1 SHOPS rows with PRICE and the canary columns hidden."
  (tx/transformed-dataset-definition "shops-tier1-hide-price" shops
                                     (keep-rows "shops" #(= 1 (:tier %)))
                                     (null-columns "shops" hidden-columns)))

(def shops-tier2
  "SHOPS restricted to tier 2 rows: what a second sandboxed user, with the tier attribute 2, sees."
  (tx/transformed-dataset-definition "shops-tier2" shops
                                     (keep-rows "shops" #(= 2 (:tier %)))))

(def shops-tier2-hide-price
  "Tier 2 SHOPS rows with PRICE and the canary columns hidden."
  (tx/transformed-dataset-definition "shops-tier2-hide-price" shops
                                     (keep-rows "shops" #(= 2 (:tier %)))
                                     (null-columns "shops" hidden-columns)))

(def variant-contents
  "Dataset variant -> the SHOPS and REGIONS rows whose canaries an admin sees in it, and the SHOPS row count."
  {#'shops                  {:shops #{1 2 3 4 5 6 7 8} :regions #{1 2 3 4} :shop-rows 8}
   #'shops-tier1            {:shops #{1 3 5 7}         :regions #{1 2 3 4} :shop-rows 4}
   #'shops-tier1-regions-tier1       {:shops #{1 3 5 7}         :regions #{1 3}     :shop-rows 4}
   #'shops-in-tier1-regions     {:shops #{1 2 5 6}         :regions #{1 2 3 4} :shop-rows 4}
   #'shops-hide-price       {:shops #{}                :regions #{1 2 3 4} :shop-rows 8}
   #'shops-tier1-hide-price {:shops #{}                :regions #{1 2 3 4} :shop-rows 4}
   #'shops-empty         {:shops #{}                :regions #{1 2 3 4} :shop-rows 0}
   #'shops-tier2            {:shops #{2 4 6 8}         :regions #{1 2 3 4} :shop-rows 4}
   #'shops-tier2-hide-price {:shops #{}                :regions #{1 2 3 4} :shop-rows 4}})
