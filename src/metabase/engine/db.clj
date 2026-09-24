(ns metabase.engine.db
  "Application database queries for the engine module: the table and field metadata that becomes
  the engine's catalog snapshot."
  (:require
   [metabase.warehouse-schema-overlay.core :as warehouse-schema-overlay]
   [toucan2.core :as t2]))

(defn active-tables
  "Active tables of a database, with the columns the catalog needs, ordered by schema and name."
  [database-id]
  (t2/select [:model/Table :id :schema :name :updated_at]
             :db_id database-id
             :active true
             {:from     [(warehouse-schema-overlay/table-query)]
              :order-by [[:schema :asc] [:name :asc]]}))

(defn active-top-level-fields
  "Active, top-level (not nested) fields of the given tables, in table and position order."
  [table-ids]
  (when (seq table-ids)
    (t2/select [:model/Field :table_id :name :base_type :database_type :database_required :updated_at]
               :table_id [:in table-ids]
               :active true
               :parent_id nil
               {:from     [(warehouse-schema-overlay/field-query)]
                :order-by [[:table_id :asc] [:position :asc] [:name :asc]]})))

(defn catalog-stamp
  "Counts and latest update times of a database's active tables and fields: two aggregate queries
  instead of loading every row."
  [database-id]
  (let [tables (t2/select-one [:model/Table [:%count.* :n] [:%max.updated_at :latest]]
                              :db_id database-id
                              :active true
                              {:from [(warehouse-schema-overlay/table-query)]})
        fields (t2/select-one [:model/Field [:%count.* :n] [:%max.updated_at :latest]]
                              :active true
                              {:from  [(warehouse-schema-overlay/field-query)]
                               :where [:in :table_id ^:allow-subquery {:select [:id]
                                                                       :from   [(warehouse-schema-overlay/table-query)]
                                                                       :where  [:and [:= :db_id database-id] [:= :active true]]}]})]
    [(:n tables) (:latest tables) (:n fields) (:latest fields)]))
