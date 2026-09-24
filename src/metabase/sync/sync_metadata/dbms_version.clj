(ns metabase.sync.sync-metadata.dbms-version
  (:require
   [metabase.driver :as driver]
   [metabase.sync.db :as sync.db]
   [metabase.sync.interface :as i]
   [metabase.sync.util :as sync-util]
   [metabase.util.malli :as mu]
   [metabase.util.malli.schema :as ms]))

(def DBMSVersion
  "Schema for the expected output of [[sync-dbms-version!]]."
  [:map
   [:version ms/NonBlankString]])

(mu/defn sync-dbms-version! :- [:maybe DBMSVersion]
  "Get the DBMS version as provided by the driver and save it in the Database."
  [database :- i/DatabaseInstance]
  (let [driver  (sync-util/database->driver database)
        version (driver/dbms-version driver database)]
    (when (not= version (:dbms_version database))
      (sync.db/update-database! (:id database) {:dbms_version version}))
    version))
