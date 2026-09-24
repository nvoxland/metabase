(ns metabase.engine-bridge.db
  "Application-database access for the JDBC bridge."
  (:require
   [toucan2.core :as t2]))

(defn database
  "The Database a bridge ticket names, or nil."
  [database-id]
  (t2/select-one :model/Database :id database-id))
