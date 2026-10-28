(ns metabase-enterprise.sandbox.enforcement-test.queries.hand-written
  "Probes written by hand, the way the query builder would, plus a few that are invalid on purpose ([[invalid]]). A probe
  is a fn of an ids map returning `{:query inner-query}` and optionally `:parameters`, so one probe renders against the
  sandboxed copy and against the restricted database. [[all]] is the set the processor tests run under every policy;
  [[parameters]], [[authored]] and [[saved-sql]] have their own runners in the harness."
  (:require
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase.query-processor :as qp]
   [metabase.test :as mt]
   [metabase.util.json :as json]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

(defn implicit
  "A ref to `target-table`.`target-col` through the FK `table`.`fk`, as the query builder writes an implicit join."
  [ids table fk target-table target-col]
  [:field (dataset/fid ids target-table target-col) {:source-field (dataset/fid ids table fk)}])

(defn- implicit-join-alias
  "The alias the QP gives the implicit join from ORDERS.SHOP_ID to SHOPS."
  [ids]
  (str (mt/format-name "shops") "__via__" (dataset/fname ids :orders :shop_id)))

(def roles
  "SHOPS in every role a table can play, written as the query builder would: source, FK target through each edge,
  explicit join, two hops, self join, FK out to REGIONS and back, breakout and order by an FK, a weighted average
  through the join. Reaches `add-implicit-joins`, `resolve-joins`,
  `fix-bad-field-id-refs` and the sandbox rewrite's second pass."
  (array-map
   :shops-source            (fn [ids] {:query {:source-table (dataset/tid ids :shops)}})
   :shops-count             (fn [ids] {:query {:source-table (dataset/tid ids :shops) :aggregation [[:count]]}})
   :orders-plain            (fn [ids] {:query {:source-table (dataset/tid ids :orders)}})
   :orders-fk-only          (fn [ids] {:query {:source-table (dataset/tid ids :orders) :fields [[:field (dataset/fid ids :orders :shop_id) nil]]}})
   :orders-implicit-price   (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                               :fields       [[:field (dataset/fid ids :orders :id) nil]
                                                              (implicit ids :orders :shop_id :shops :price)]}})
   :orders-second-fk        (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                               :fields       [[:field (dataset/fid ids :orders :id) nil]
                                                              (implicit ids :orders :return_shop_id :shops :price)]}})
   :orders-both-fks         (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                               :fields       [(implicit ids :orders :shop_id :shops :secret)
                                                              (implicit ids :orders :return_shop_id :shops :secret)]}})
   :orders-explicit-join    (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                               :joins        [{:source-table (dataset/tid ids :shops)
                                                               :alias        "S"
                                                               :condition    [:= [:field (dataset/fid ids :orders :shop_id) nil]
                                                                              [:field (dataset/fid ids :shops :id) {:join-alias "S"}]]
                                                               :fields       :all}]}})
   :items-two-hops          (fn [ids] {:query {:source-table (dataset/tid ids :items)
                                               :joins        [{:source-table (dataset/tid ids :orders)
                                                               :alias        "O"
                                                               :condition    [:= [:field (dataset/fid ids :items :order_id) nil]
                                                                              [:field (dataset/fid ids :orders :id) {:join-alias "O"}]]
                                                               :fields       :none}]
                                               :fields       [[:field (dataset/fid ids :items :id) nil]
                                                              [:field (dataset/fid ids :shops :price) {:source-field            (dataset/fid ids :orders :shop_id)
                                                                                                       :source-field-join-alias "O"}]]}})
   :shops-self-join         (fn [ids] {:query {:source-table (dataset/tid ids :shops)
                                               :fields       [[:field (dataset/fid ids :shops :id) nil]
                                                              (implicit ids :shops :parent_shop_id :shops :secret)]}})
   :shops-out-to-regions    (fn [ids] {:query {:source-table (dataset/tid ids :shops)
                                               :fields       [[:field (dataset/fid ids :shops :id) nil]
                                                              (implicit ids :shops :region_id :regions :secret)]}})
   :shops-filter-region-tier (fn [ids] {:query {:source-table (dataset/tid ids :shops)
                                                :aggregation  [[:count]]
                                                :filter       [:= (implicit ids :shops :region_id :regions :tier) 1]}})
   :shops-breakout-region   (fn [ids] {:query {:source-table (dataset/tid ids :shops)
                                               :aggregation  [[:count]]
                                               :breakout     [(implicit ids :shops :region_id :regions :name)]}})
   ;; orders 1, 9 and 3 point at shops 1, 1 and 3: the average of their SECRET_NUM canaries equals shop 2's
   :orders-avg-secret-weighted (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                                  :aggregation  [[:avg (implicit ids :orders :shop_id :shops :secret_num)]]
                                                  :filter       [:= [:field (dataset/fid ids :orders :id) nil] 1 3 9]}})
   :regions-into-shops      (fn [ids] {:query {:source-table (dataset/tid ids :regions)
                                               :fields       [[:field (dataset/fid ids :regions :id) nil]
                                                              (implicit ids :regions :hq_shop_id :shops :secret)]}})
   :orders-breakout-fk      (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                               :breakout     [[:field (dataset/fid ids :orders :shop_id) nil]]
                                               :aggregation  [[:count]]}})
   :orders-order-by-fk      (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                               :order-by     [[:asc [:field (dataset/fid ids :orders :shop_id) nil]]]}})
   :items-join-orders-all   (fn [ids] {:query {:source-table (dataset/tid ids :items)
                                               :joins        [{:source-table (dataset/tid ids :orders)
                                                               :alias        "O"
                                                               :condition    [:= [:field (dataset/fid ids :items :order_id) nil]
                                                                              [:field (dataset/fid ids :orders :id) {:join-alias "O"}]]
                                                               :fields       :all}]}})
   ;; SHOPS rows bucketed by month through the implicit join; each tier is its own month
   :orders-breakout-shop-month (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                                  :breakout     [[:field (dataset/fid ids :shops :opened_on) {:source-field  (dataset/fid ids :orders :shop_id)
                                                                                                              :temporal-unit :month}]]
                                                  :aggregation  [[:count]]}})))

(def sources
  "SHOPS behind a saved question, a model (with stored result metadata) and a nested query. Reaches
  `fetch-source-query`, model metadata merging and persisted-model substitution."
  (array-map
   :card-shops              (fn [ids] {:query {:source-table (dataset/card-source ids :card/shops)}})
   :model-shops             (fn [ids] {:query {:source-table (dataset/card-source ids :model/shops)}})
   :card-orders-implicit    (fn [ids] {:query {:source-table (dataset/card-source ids :card/orders-implicit)}})
   :model-orders-implicit   (fn [ids] {:query {:source-table (dataset/card-source ids :model/orders-implicit)}})
   :model-shops-count       (fn [ids] {:query {:source-table (dataset/card-source ids :model/shops) :aggregation [[:count]]}})
   :nested-shops            (fn [ids] {:query {:source-query {:source-table (dataset/tid ids :shops)}}})))

(def name-refs
  "Refs by column name instead of id: a nested stage read by name, a month-bucketed column whose unit is
  inherited, the joined-column naming convention, an expression, deduplicated names (`NAME_2`, `NAME_3`). Reaches
  `lib.field.resolution` and `add-alias-info`."
  (array-map
   :nested-shops-name-ref   (fn [ids] {:query {:source-query {:source-table (dataset/tid ids :shops)}
                                               :fields       [[:field (dataset/fname ids :shops :secret) {:base-type :type/Text}]]}})
   ;; the next stage reads the month-bucketed column by name, so its unit is inherited
   :month-bucket-name-ref   (fn [ids] {:query {:source-query {:source-table (dataset/tid ids :shops)
                                                              :breakout     [[:field (dataset/fid ids :shops :opened_on) {:temporal-unit :month}]]
                                                              :aggregation  [[:sum [:field (dataset/fid ids :shops :secret_num) nil]]]}
                                               :fields       [[:field (dataset/fname ids :shops :opened_on) {:base-type :type/Date}]
                                                              [:field "sum" {:base-type :type/BigInteger}]]}})
   ;; a name ref "S__SECRET" that points into join S by the joined-column naming convention
   :joined-dunder-name-ref  (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                               :joins        [{:source-table (dataset/tid ids :shops)
                                                               :alias        "S"
                                                               :condition    [:= [:field (dataset/fid ids :orders :shop_id) nil]
                                                                              [:field (dataset/fid ids :shops :id) {:join-alias "S"}]]
                                                               :fields       :all}]
                                               :fields       [[:field (dataset/fid ids :orders :id) nil]
                                                              [:field (str "S__" (dataset/fname ids :shops :secret)) {:base-type :type/Text}]]}})
   ;; a name ref to an expression over a SHOPS column
   :expression-name-ref     (fn [ids] {:query {:source-table (dataset/tid ids :shops)
                                               :expressions  {"secret_copy" [:concat [:field (dataset/fid ids :shops :secret) nil] ""]}
                                               :fields       [[:field (dataset/fid ids :shops :id) nil]
                                                              [:field "secret_copy" {:base-type :type/Text}]]}})
   ;; NAME, NAME_2 and NAME_3 in one stage; the next stage reads NAME_3 by name
   :deduplicated-name-ref-3 (fn [ids] {:query {:source-query {:source-table (dataset/tid ids :orders)
                                                              :fields       [[:field (dataset/fid ids :orders :name) nil]
                                                                             (implicit ids :orders :shop_id :shops :name)
                                                                             (implicit ids :orders :return_shop_id :shops :name)]}
                                               :fields       [[:field (str (dataset/fname ids :shops :name) "_3") {:base-type :type/Text}]]}})
   ;; ORDERS.NAME and SHOPS.NAME in one stage come out as NAME and NAME_2; the next stage reads them by name
   :deduplicated-name-ref   (fn [ids] {:query {:source-query {:source-table (dataset/tid ids :orders)
                                                              :fields       [[:field (dataset/fid ids :orders :name) nil]
                                                                             (implicit ids :orders :shop_id :shops :name)
                                                                             (implicit ids :orders :shop_id :shops :secret)]}
                                               :fields       [[:field (str (dataset/fname ids :shops :name) "_2") {:base-type :type/Text}]
                                                              [:field (dataset/fname ids :shops :secret) {:base-type :type/Text}]]}})))

(def join-forgery
  "Explicit joins dressed up as, or colliding with, the implicit join to SHOPS, and two implicit joins that
  collide. Reaches `add-implicit-joins`' alias handling. Two of these are invalid queries on purpose (see [[invalid]])."
  (array-map
   ;; a ref through RETURN_SHOP_ID that claims the name SHOP_ID gets the implicit join alias of SHOP_ID; the two
   ;; implicit joins collide and one is renamed
   :colliding-implicit-joins (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                                :fields       [[:field (dataset/fid ids :orders :id) nil]
                                                               (implicit ids :orders :shop_id :shops :price)
                                                               [:field (dataset/fid ids :shops :secret) {:source-field      (dataset/fid ids :orders :return_shop_id)
                                                                                                         :source-field-name (dataset/fname ids :orders :shop_id)}]]}})
   ;; an explicit REGIONS join dressed up as the implicit join through ORDERS.SHOP_ID; the implicit ref must not reuse it
   :forged-implicit-join    (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                               :joins        [{:source-table          (dataset/tid ids :regions)
                                                               :alias                 (implicit-join-alias ids)
                                                               :condition             [:= [:field (dataset/fid ids :orders :shop_id) nil]
                                                                                       [:field (dataset/fid ids :regions :id) {:join-alias (implicit-join-alias ids)}]]
                                                               :fields                :none
                                                               :fk-field-id           (dataset/fid ids :orders :shop_id)
                                                               :qp/is-implicit-join   true}]
                                               :fields       [[:field (dataset/fid ids :orders :id) nil]
                                                              (implicit ids :orders :shop_id :shops :secret)]}})
   ;; the same explicit join without the forged keys: the implicit join's alias collides with it and must be renamed
   :colliding-join-alias    (fn [ids] {:query {:source-table (dataset/tid ids :orders)
                                               :joins        [{:source-table (dataset/tid ids :regions)
                                                               :alias        (implicit-join-alias ids)
                                                               :condition    [:= [:field (dataset/fid ids :orders :shop_id) nil]
                                                                              [:field (dataset/fid ids :regions :id) {:join-alias (implicit-join-alias ids)}]]
                                                               :fields       :none}]
                                               :fields       [[:field (dataset/fid ids :orders :id) nil]
                                                              (implicit ids :orders :shop_id :shops :secret)]}})))

(def bogus-join-aliases
  "A join alias that names no join, over a card and over a model: invalid on purpose. Reaches the
  validation that runs after the card's stage metadata is spliced in, whose error text echoes that metadata."
  (array-map
   ;; a bogus join alias over a card source fails validation after the card's stage metadata is spliced in, and the
   ;; error text echoes that metadata, fingerprints included
   :model-bogus-join-alias  (fn [ids] {:query {:source-table (dataset/card-source ids :model/shops)
                                               :fields       [[:field (dataset/fid ids :shops :name) {:join-alias "Nope"}]]}})
   :card-bogus-join-alias   (fn [ids] {:query {:source-table (dataset/card-source ids :card/shops)
                                               :fields       [[:field (dataset/fid ids :shops :name) {:join-alias "Nope"}]]}})))

(def regions-columns
  "REGIONS columns that only REGIONS names (BUDGET, CODE), which a REGIONS sandbox may hide: REGIONS as the source, an
  aggregation over a hidden column, and the same columns reached from SHOPS through the implicit join, as a field, a
  breakout and a filter."
  (array-map
   :regions-source            (fn [ids] {:query {:source-table (dataset/tid ids :regions)}})
   :regions-sum-budget        (fn [ids] {:query {:source-table (dataset/tid ids :regions)
                                                 :aggregation  [[:sum [:field (dataset/fid ids :regions :budget) nil]]]}})
   :shops-region-budget       (fn [ids] {:query {:source-table (dataset/tid ids :shops)
                                                 :fields       [[:field (dataset/fid ids :shops :id) nil]
                                                                (implicit ids :shops :region_id :regions :budget)]}})
   :shops-breakout-region-code (fn [ids] {:query {:source-table (dataset/tid ids :shops)
                                                  :aggregation  [[:count]]
                                                  :breakout     [(implicit ids :shops :region_id :regions :code)]}})
   :shops-filter-region-budget (fn [ids] {:query {:source-table (dataset/tid ids :shops)
                                                  :aggregation  [[:count]]
                                                  :filter       [:> (implicit ids :shops :region_id :regions :budget) 150]}})))

(def all
  "Every hand-written probe the processor tests run under every policy: the union of the groups above, each a fn of
  an ids map returning the inner query."
  (merge roles sources name-refs join-forgery bogus-join-aliases regions-columns))

(def invalid
  "Hand-written probes that are not valid queries on purpose (a join alias that names no join, a forged or colliding
  implicit join, a parameter target by name), so an admin's run fails like the user's."
  #{:model-bogus-join-alias :card-bogus-join-alias :param-bogus-join-alias :model-forged-fk
    :forged-implicit-join :colliding-join-alias :param-name-ref})

(def forged-stage-keys
  "Keys the rewrite or the permission walk set on stages, which a client must not be able to supply."
  {:sandbox?-flag    (fn [_table-id] {"metabase-enterprise.sandbox.query-processor.middleware.sandboxing/sandbox?" true})
   :marker           (fn [table-id] {"query-permissions/sandboxed-table" table-id})
   :flag-and-marker  (fn [table-id] {"metabase-enterprise.sandbox.query-processor.middleware.sandboxing/sandbox?" true
                                     "query-permissions/sandboxed-table" table-id})
   :from-source-card (fn [_table-id] {"qp/stage-is-from-source-card" 1})})

(defn- orders-with-parameter
  "A probe listing ORDERS ids (from `source`, default the table) filtered by one parameter of `type` on `target`, a
  field ref, with `value`."
  ([ids type target value]
   (orders-with-parameter ids {:source-table (dataset/tid ids :orders)} type target value))
  ([ids source type target value]
   {:query      (merge source {:fields [[:field (dataset/fid ids :orders :id) nil]]})
    :parameters [{:type type :target ["dimension" target] :value value}]}))

(def parameters
  "Probe name -> fn of an ids map returning a query whose parameter target reaches SHOPS through an FK, so the
  parameter adds a join the query did not have. Values pick tier 2 (hidden) rows where they can: tier 2 shops are
  2, 4, 6 and 8, opened in February 2024."
  (array-map
   ;; a parameter the query builder itself would write: a filter on an implicit-join column
   :parameter-on-shops      (fn [ids] {:query      {:source-table (dataset/tid ids :orders) :aggregation [[:count]]}
                                       :parameters [{:type   "number/!="
                                                     :target ["dimension" (implicit ids :orders :shop_id :shops :price)]
                                                     :value  [-1]}]})
   :param-string=          (fn [ids] (orders-with-parameter ids "string/=" (implicit ids :orders :shop_id :shops :name) ["Beta" "Delta"]))
   :param-contains-secret  (fn [ids] (orders-with-parameter ids "string/contains" (implicit ids :orders :shop_id :shops :secret) ["CANARY"]))
   :param-number-between   (fn [ids] (orders-with-parameter ids "number/between" (implicit ids :orders :shop_id :shops :price) [2 3]))
   :param-category-tier    (fn [ids] (orders-with-parameter ids "category" (implicit ids :orders :shop_id :shops :tier) [2]))
   :param-id               (fn [ids] (orders-with-parameter ids "id" (implicit ids :orders :shop_id :shops :id) [2 4]))
   :param-float            (fn [ids] (orders-with-parameter ids "number/>=" (implicit ids :orders :shop_id :shops :rating) [2.5]))
   :param-month-year       (fn [ids] (orders-with-parameter ids "date/month-year" (implicit ids :orders :shop_id :shops :opened_on) "2024-02"))
   :param-date-range       (fn [ids] (orders-with-parameter ids "date/range" (implicit ids :orders :shop_id :shops :opened_on) "2024-02-01~2024-02-29"))
   :param-second-fk        (fn [ids] (orders-with-parameter ids "category" (implicit ids :orders :return_shop_id :shops :tier) [2]))
   :param-name-ref         (fn [ids] (orders-with-parameter ids "category"
                                                            [:field (dataset/fname ids :shops :tier) {:base-type    :type/Integer
                                                                                                      :source-field (dataset/fid ids :orders :shop_id)}]
                                                            [2]))
   :param-bogus-join-alias (fn [ids] (orders-with-parameter ids "category"
                                                            [:field (dataset/fid ids :shops :tier) {:join-alias "Shops"}]
                                                            [2]))
   :param-on-model         (fn [ids] (orders-with-parameter ids {:source-table (dataset/card-source ids :model/orders-implicit)}
                                                            "category" (implicit ids :orders :shop_id :shops :tier) [2]))
   :param-on-nested        (fn [ids] (orders-with-parameter ids {:source-query {:source-table (dataset/tid ids :orders)}}
                                                            "category" (implicit ids :orders :shop_id :shops :tier) [2]))
   :param-two-fks          (fn [ids] {:query      {:source-table (dataset/tid ids :orders)
                                                   :fields       [[:field (dataset/fid ids :orders :id) nil]]}
                                      :parameters [{:type   "category"
                                                    :target ["dimension" (implicit ids :orders :shop_id :shops :tier)]
                                                    :value  [2]}
                                                   {:type   "number/>="
                                                    :target ["dimension" (implicit ids :orders :return_shop_id :shops :secret_num)]
                                                    :value  [0]}]})
   :param-temporal-unit    (fn [ids] (let [opened (assoc-in (implicit ids :orders :shop_id :shops :opened_on) [2 :temporal-unit] :day)]
                                       {:query      {:source-table (dataset/tid ids :orders)
                                                     :breakout     [opened]
                                                     :aggregation  [[:count]]}
                                        :parameters [{:type "temporal-unit" :target ["dimension" opened] :value "month"}]}))))

;;; ----------------------------------------------- Authored content -----------------------------------------------
;;;
;;; Content the sandboxed user saves (models with metadata they wrote, metrics) and content an admin saves that the
;;; sandboxed user uses (segments). One probe per path; the same content is created by an admin in the restricted copy
;;; for the expected answer.

(defn table-query
  "The outer query selecting everything from `table`."
  [ids table]
  {:database (:db ids) :type "query" :query {:source-table (dataset/tid ids table)}})

(defn- create-card-as!
  "Save a card as `user` through the API. Returns its id."
  [user body]
  (:id (mt/user-http-request user :post 200 "card" (merge {:name "authored" :display "table" :visualization_settings {}}
                                                          body))))

(defn- forge-column
  "`columns`, result metadata, with the column named `col-name` changed by `f`."
  [columns col-name f]
  (mapv #(if (= col-name (:name %)) (f %) %) columns))

(defn model-metadata
  "Result metadata for `query`, computed as an admin in-process, so a model has what a saved one would; in the wire
  shape, as the API would have stored it."
  [query]
  (-> (mt/with-test-user :crowberto (qp/process-query query))
      (get-in [:data :results_metadata :columns])
      json/encode
      json/decode+kw))

(defn- model-as!
  "Save a model over `table` as `user`, with the result metadata of column `col` changed by `f`. Returns `{:card id}`."
  [user ids table col f]
  (let [query (table-query ids table)]
    {:card (create-card-as! user {:type            "model"
                                  :dataset_query   query
                                  :result_metadata (forge-column (model-metadata query) (dataset/fname ids table col) f)})}))

(def authored
  "Probe name -> `{:author (fn [ids user] created), :probe (fn [ids created] probe)}`. `created` maps `:card` or
  `:segment` to the id of what `:author` saved."
  (array-map
   ;; a model over SHOPS whose NAME column claims to be SECRET, queried by SECRET's field id
   :model-forged-field-id {:author (fn [ids user]
                                     (model-as! user ids :shops :name
                                                #(assoc % :id (dataset/fid ids :shops :secret)
                                                        :field_ref ["field" (dataset/fid ids :shops :secret) nil])))
                           :probe  (fn [ids {:keys [card]}]
                                     {:query {:source-table (str "card__" card)
                                              :fields       [["field" (dataset/fid ids :shops :secret) nil]]}})}
   ;; a model over ORDERS whose AMOUNT column claims to be an FK to SHOPS, used for an implicit join
   :model-forged-fk       {:author (fn [ids user]
                                     (model-as! user ids :orders :amount
                                                #(assoc % :semantic_type "type/FK"
                                                        :fk_target_field_id (dataset/fid ids :shops :id))))
                           :probe  (fn [ids {:keys [card]}]
                                     {:query {:source-table (str "card__" card)
                                              :fields       [["field" (dataset/fid ids :orders :id) nil]
                                                             ["field" (dataset/fid ids :shops :secret)
                                                              {:source-field (dataset/fid ids :orders :amount)}]]}})}
   ;; a model over ORDERS whose NAME column is mapped to SHOPS.SECRET, as the model metadata editor does
   :model-mapped-to-shops {:author (fn [ids user]
                                     (model-as! user ids :orders :name
                                                #(assoc % :id (dataset/fid ids :shops :secret)
                                                        :field_ref ["field" (dataset/fid ids :shops :secret) nil]
                                                        :semantic_type "type/Category")))
                           :probe  (fn [ids {:keys [card]}]
                                     {:query {:source-table (str "card__" card)
                                              :breakout     [["field" (dataset/fname ids :orders :name) {:base-type "type/Text"}]]
                                              :aggregation  [["count"]]}})}
   ;; a metric over SHOPS saved by the user, used from SHOPS
   :own-metric            {:author (fn [ids user]
                                     {:card (create-card-as!
                                             user {:type          "metric"
                                                   :display       "scalar"
                                                   :dataset_query (assoc-in (table-query ids :shops) [:query :aggregation]
                                                                            [["sum" ["field" (dataset/fid ids :shops :secret_num) nil]]])})})
                           :probe  (fn [_ids {:keys [card]}]
                                     {:query {:source-table (str "card__" card)
                                              :aggregation  [["metric" card]]}})}
   ;; an admin's segment on SHOPS (sandboxed users cannot create segments), used from SHOPS and from ORDERS
   :admin-segment         {:author (fn [ids _user]
                                     {:segment (:id (mt/user-http-request
                                                     :crowberto :post 200 "segment"
                                                     {:name       "tier 2"
                                                      :table_id   (dataset/tid ids :shops)
                                                      :definition (assoc-in (table-query ids :shops) [:query :filter]
                                                                            ["=" ["field" (dataset/fid ids :shops :tier) nil] 2])}))})
                           :probe  (fn [ids {:keys [segment]}]
                                     {:query {:source-table (dataset/tid ids :shops)
                                              :filter       ["segment" segment]
                                              :fields       [["field" (dataset/fid ids :shops :id) nil] ["field" (dataset/fid ids :shops :secret) nil]]}})}))

(defn delete-authored!
  "Delete what an authored probe's `:author` created."
  [created]
  (doseq [[k id] created]
    (t2/delete! (case k :card :model/Card :segment :model/Segment) id)))

(def saved-sql
  "Probes over a saved SQL question on SHOPS. Sandboxes do not apply to SQL results, by design: see \"You cannot
  secure the rows or columns of SQL results\" in docs/permissions/row-and-column-security.md."
  (array-map
   :native-card-source   (fn [ids] {:query {:source-table (dataset/card-source ids :native/shops)}})
   :native-card-name-ref (fn [ids] {:query {:source-table (dataset/card-source ids :native/shops)
                                            :fields       [[:field (dataset/fname ids :shops :secret) {:base-type :type/Text}]]}})))

;;; ------------------------------------------------ Forged metadata -----------------------------------------------
;;;
;;; A model's result metadata is the user's to write: the card API keeps every key a client sends, the Lib name keys
;;; (`:lib/original-name`, `:lib/deduplicated-name`, `:lib/source-column-alias`, `:lib/desired-column-alias`) and
;;; `:lib/source` included, and no guard applies to a model that no sandbox reads. These models disagree with their
;;; own queries on purpose; the probes ask whether the query processor trusts the metadata over the query in any way
;;; that reaches data the sandbox hides. Reaches model metadata merging (`lib.card`), `lib.field.resolution`,
;;; `add-alias-info` (whose desired aliases become a positional CTE column list) and `add-remaps`.

(defn- format-name [c] (mt/format-name (name c)))

(def ^:private all-shops-columns [:id :name :tier :price :region_id :parent_shop_id :rating :opened_on :secret :secret_num])

(defn- shops-columns-query [ids cols]
  (assoc-in (table-query ids :shops) [:query :fields] (mapv (fn [c] [:field (dataset/fid ids :shops c) nil]) cols)))

(defn- orders-implicit-query [ids]
  (assoc-in (table-query ids :orders) [:query :fields]
            [[:field (dataset/fid ids :orders :id) nil]
             [:field (dataset/fid ids :orders :name) nil]
             [:field (dataset/fid ids :orders :shop_id) nil]
             (implicit ids :orders :shop_id :shops :name)
             (implicit ids :orders :shop_id :shops :tier)]))

(defn- orders-return-query [ids]
  (assoc-in (table-query ids :orders) [:query :fields]
            [[:field (dataset/fid ids :orders :id) nil] [:field (dataset/fid ids :orders :return_shop_id) nil]]))

(defn- column-named [md c] (first (filter #(= (format-name c) (:name %)) md)))

(defn- update-column [md c f] (mapv #(if (= (format-name c) (:name %)) (f %) %) md))

(defn- full-shops-metadata [ids] (model-metadata (shops-columns-query ids all-shops-columns)))

(def forged-metadata
  "Forgery name -> fn of an ids map returning `{:query q, :forge (fn [metadata] forged-metadata)}`: the model's query
  and how its true metadata is rewritten before the user saves it."
  (array-map
   ;; two columns trade names: NAME is called SECRET and SECRET is called NAME
   :swap-names           (fn [ids] {:query (shops-columns-query ids all-shops-columns)
                                    :forge #(-> % (update-column :name (fn [c] (assoc c :name (format-name :secret))))
                                                (update-column :secret (fn [c] (assoc c :name (format-name :name)))))})
   ;; NAME keeps its `:name`, while every Lib name key points at another column, PRICE among them
   :lib-names-disagree   (fn [ids] {:query (shops-columns-query ids all-shops-columns)
                                    :forge #(update-column % :name (fn [c] (assoc c
                                                                                  :lib/original-name (format-name :price)
                                                                                  :lib/deduplicated-name (format-name :secret)
                                                                                  :lib/source-column-alias (format-name :price)
                                                                                  :lib/desired-column-alias (format-name :secret_num))))})
   ;; NAME claims to be the field PRICE
   :id-to-hidden         (fn [ids] {:query (shops-columns-query ids [:id :name :tier])
                                    :forge #(update-column % :name (fn [c] (assoc c
                                                                                  :id (dataset/fid ids :shops :price)
                                                                                  :field_ref [:field (dataset/fid ids :shops :price) nil])))})
   ;; the metadata lists columns the query does not return
   :extra-hidden-columns (fn [ids] {:query (shops-columns-query ids [:id :name :tier])
                                    :forge (fn [md] (into (vec md) (map #(column-named (full-shops-metadata ids) %))
                                                          [:price :secret :secret_num]))})
   ;; ... and says they come from the NAME column
   :extra-columns-aliased (fn [ids] {:query (shops-columns-query ids [:id :name :tier])
                                     :forge (fn [md] (into (vec md)
                                                           (map #(assoc (column-named (full-shops-metadata ids) %)
                                                                        :lib/source-column-alias (format-name :name)
                                                                        :lib/desired-column-alias (format-name :name)))
                                                           [:price :secret]))})
   ;; the implicit-join SHOPS.NAME column takes ORDERS.NAME's name and points its aliases at SHOPS.SECRET
   :implicit-dedup-collision (fn [ids] {:query (orders-implicit-query ids)
                                        :forge #(mapv (fn [c] (if (re-find #"(?i)__via__" (str (:name c)))
                                                                (assoc c
                                                                       :name (format-name :name)
                                                                       :lib/deduplicated-name (format-name :name)
                                                                       :lib/source-column-alias (str (format-name :shops) "__via__"
                                                                                                     (format-name :shop_id) "__"
                                                                                                     (format-name :secret))
                                                                       :lib/desired-column-alias (format-name :secret))
                                                                c))
                                                      %)})
   ;; a column the query does not return, claiming to come straight from the SHOPS table
   :table-defaults-source (fn [ids] {:query (orders-implicit-query ids)
                                     :forge #(conj (vec %) {:name                     (format-name :secret)
                                                            :display_name             "Secret"
                                                            :base_type                :type/Text
                                                            :id                       (dataset/fid ids :shops :secret)
                                                            :table_id                 (dataset/tid ids :shops)
                                                            :field_ref                [:field (dataset/fid ids :shops :secret) nil]
                                                            :lib/source               :source/table-defaults
                                                            :lib/source-column-alias  (format-name :secret)
                                                            :lib/desired-column-alias (format-name :secret)})})
   ;; ORDERS.RETURN_SHOP_ID (hidden shop ids among its values) claims to be ORDERS.SHOP_ID, which a policy may remap
   :remap-via-forged-fk-id (fn [ids] {:query (orders-return-query ids)
                                      :forge #(update-column % :return_shop_id
                                                             (fn [c] (assoc c
                                                                            :id (dataset/fid ids :orders :shop_id)
                                                                            :field_ref [:field (dataset/fid ids :orders :shop_id) nil])))})))

(def forged-metadata-queries
  "Query name -> fn of an ids map and the forged model's id, returning the probe run over the model."
  (array-map
   :all            (fn [_ m] {:query {:source-table (str "card__" m)}})
   :id-refs        (fn [ids m] {:query {:source-table (str "card__" m)
                                        :fields       (mapv (fn [c] [:field (dataset/fid ids :shops c) nil]) [:id :name :price :secret])}})
   :name-refs      (fn [_ m] {:query {:source-table (str "card__" m)
                                      :fields       (mapv (fn [c] [:field (format-name c) {:base-type :type/Text}]) [:name :secret])}})
   :price-by-name  (fn [_ m] {:query {:source-table (str "card__" m)
                                      :fields       [[:field (format-name :price) {:base-type :type/Integer}]]}})
   :breakout-price (fn [ids m] {:query {:source-table (str "card__" m)
                                        :breakout     [[:field (dataset/fid ids :shops :price) nil]]
                                        :aggregation  [[:count]]}})
   :filter-secret  (fn [ids m] {:query {:source-table (str "card__" m)
                                        :filter       [:starts-with [:field (dataset/fid ids :shops :secret) nil] "CANARY-S2"]
                                        :aggregation  [[:count]]}})))

(defn save-forged-model!
  "Save, as `user`, a model with the query and forged metadata of forgery `forgery-k` against the database `ids`
  describes. Returns its id."
  [user ids forgery-k]
  (let [{:keys [query forge]} ((forged-metadata forgery-k) ids)]
    (create-card-as! user {:type            "model"
                           :dataset_query   query
                           :result_metadata (forge (model-metadata query))})))

(defn invalid?
  "Is `probe` invalid on purpose, so that both users failing is in order? The [[invalid]] hand-written probes, and every
  probe over a forged-metadata model: its metadata names columns its query does not return."
  [probe]
  (boolean (or (invalid probe)
               (and (vector? probe) (= :forged-metadata (first probe))))))
