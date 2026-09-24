(ns metabase.engine.events
  "Keeps the engine in step with the application database: a deleted database releases its pools
  and catalog. Details changes are picked up on the next query (the driver keys registrations on a
  details hash) so they need no event."
  (:require
   [metabase.engine.core :as engine]
   [metabase.events.core :as events]
   [metabase.util.log :as log]
   [methodical.core :as methodical]))

(derive ::database-delete :metabase/event)
(derive :event/database-delete ::database-delete)

(methodical/defmethod events/publish-event! ::database-delete
  [_topic {:keys [object]}]
  (when-let [id (:id object)]
    (log/debugf "Forgetting deleted database %d in the native query engine" id)
    (engine/forget-database! id)))
