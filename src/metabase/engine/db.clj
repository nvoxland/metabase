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
