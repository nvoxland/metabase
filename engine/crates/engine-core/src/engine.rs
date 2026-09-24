//! The engine: one tokio runtime, catalog snapshots per database, sessions per request.
//! Both front-ends call `handle` / `handle_async` with the same messages.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::runtime::Runtime;

use crate::api::{DatabaseRegistration, Request, Response, SessionContext};
use crate::catalog::SnapshotCatalog;
use crate::error::{EngineError, ErrorKind, Result};
use crate::planner::Planner;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Worker threads for the internal runtime. Defaults to the number of cores.
    #[serde(default)]
    pub worker_threads: Option<usize>,
    /// Honor the `debug_panic` request. Tests only.
    #[serde(default)]
    pub allow_debug_ops: bool,
}

pub struct Session {
    pub id: u64,
    pub context: SessionContext,
    pub catalog: Arc<SnapshotCatalog>,
    pub planner: Planner,
}

pub struct Engine {
    runtime: Runtime,
    config: EngineConfig,
    catalogs: DashMap<i64, Arc<SnapshotCatalog>>,
    databases: DashMap<i64, Arc<DatabaseRegistration>>,
    sessions: DashMap<u64, Arc<Session>>,
    next_id: AtomicU64,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Arc<Engine>> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all().thread_name("metabase-engine");
        if let Some(n) = config.worker_threads {
            builder.worker_threads(n.max(1));
        }
        let runtime = builder.build().map_err(|e| {
            EngineError::new(ErrorKind::InvalidRequest, format!("starting runtime: {e}"))
        })?;
        Ok(Arc::new(Engine {
            runtime,
            config,
            catalogs: DashMap::new(),
            databases: DashMap::new(),
            sessions: DashMap::new(),
            next_id: AtomicU64::new(1),
        }))
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn version() -> &'static str {
        VERSION
    }

    /// Handle a request from a thread that is not a runtime worker (the C ABI path).
    /// Panics if called from inside the runtime; use `handle_async` there.
    pub fn handle(&self, request: Request) -> Response {
        self.runtime.block_on(self.handle_async(request))
    }

    pub fn handle_json(&self, request_json: &str) -> Response {
        match serde_json::from_str::<Request>(request_json) {
            Ok(request) => self.handle(request),
            Err(e) => Response::err(e.into()),
        }
    }

    pub async fn handle_json_async(&self, request_json: &str) -> Response {
        match serde_json::from_str::<Request>(request_json) {
            Ok(request) => self.handle_async(request).await,
            Err(e) => Response::err(e.into()),
        }
    }

    pub async fn handle_async(&self, request: Request) -> Response {
        self.dispatch(request).await.into()
    }

    async fn dispatch(&self, request: Request) -> Result<Value> {
        match request {
            Request::Version => Ok(json!({ "version": VERSION })),
            Request::RegisterCatalog { snapshot } => {
                let catalog = SnapshotCatalog::from_snapshot(&snapshot)?;
                let tables = catalog.table_count();
                self.catalogs.insert(snapshot.database_id, catalog);
                Ok(json!({
                    "database_id": snapshot.database_id,
                    "catalog_version": snapshot.catalog_version,
                    "tables": tables,
                }))
            }
            Request::InvalidateDatabase { database_id } => {
                let had_catalog = self.catalogs.remove(&database_id).is_some();
                let had_database = self.databases.remove(&database_id).is_some();
                Ok(json!({ "catalog": had_catalog, "database": had_database }))
            }
            Request::RegisterDatabase { registration } => {
                let id = registration.database_id;
                self.databases.insert(id, Arc::new(registration));
                Ok(json!({ "database_id": id }))
            }
            Request::CreateSession { context } => {
                let id = self.create_session(context)?;
                Ok(json!({ "session_id": id }))
            }
            Request::DestroySession { session_id } => {
                Ok(json!({ "destroyed": self.sessions.remove(&session_id).is_some() }))
            }
            Request::PlanAndUnparse {
                session_id,
                sql,
                params,
            } => {
                let session = self.session(session_id)?;
                let result = session.planner.plan_and_unparse(&sql, &params).await?;
                Ok(serde_json::to_value(result)?)
            }
            Request::Cancel { .. } => Err(EngineError::not_implemented("cancel")),
            Request::PoolStats { .. } => Ok(json!({ "pools": [] })),
            Request::DebugPanic => {
                if self.config.allow_debug_ops {
                    panic!("debug_panic requested");
                }
                Err(EngineError::invalid_request("debug ops are disabled"))
            }
        }
    }

    pub fn create_session(&self, context: SessionContext) -> Result<u64> {
        let catalog = self
            .catalogs
            .get(&context.database_id)
            .map(|c| Arc::clone(c.value()))
            .ok_or_else(|| {
                EngineError::new(
                    ErrorKind::CatalogMissing,
                    format!("no catalog registered for database {}", context.database_id),
                )
            })?;
        let planner = Planner::new(Arc::clone(&catalog), context.clone())?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.sessions.insert(
            id,
            Arc::new(Session {
                id,
                context,
                catalog,
                planner,
            }),
        );
        Ok(id)
    }

    pub fn session(&self, id: u64) -> Result<Arc<Session>> {
        self.sessions
            .get(&id)
            .map(|s| Arc::clone(s.value()))
            .ok_or_else(|| EngineError::new(ErrorKind::SessionMissing, format!("no session {id}")))
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Stop accepting work and wait for the runtime to drain, bounded by `timeout`.
    pub fn shutdown(self: Arc<Engine>, timeout: Duration) {
        self.sessions.clear();
        match Arc::try_unwrap(self) {
            Ok(engine) => engine.runtime.shutdown_timeout(timeout),
            Err(shared) => {
                tracing::warn!(
                    refs = Arc::strong_count(&shared),
                    "engine still referenced at shutdown; runtime will stop when dropped"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_json() -> Value {
        json!({
            "op": "register_catalog",
            "snapshot": {
                "database_id": 42, "catalog_version": "v1", "connector": "postgres",
                "tables": [{"schema": "public", "name": "Orders", "columns": [
                    {"name": "ID", "type": "Int64", "nullable": false},
                    {"name": "Total", "type": "Float64"}
                ]}]
            }
        })
    }

    #[test]
    fn version_round_trips_through_json() {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        let response = engine.handle_json(r#"{"op":"version"}"#);
        match response {
            Response::Ok { result } => assert_eq!(result["version"], VERSION),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn register_catalog_then_plan_through_a_session() {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        assert!(engine.handle_json(&snapshot_json().to_string()).is_ok());
        let created = engine.handle_json(r#"{"op":"create_session","context":{"database_id":42}}"#);
        let session_id = match created {
            Response::Ok { result } => result["session_id"].as_u64().unwrap(),
            other => panic!("{other:?}"),
        };
        let planned = engine.handle(Request::PlanAndUnparse {
            session_id,
            sql: r#"SELECT "ID" FROM "public"."Orders" WHERE "Total" > $1"#.into(),
            params: vec![],
        });
        match planned {
            Response::Ok { result } => {
                assert_eq!(result["warehouse"], "postgres");
                assert!(result["warehouse_sql"].as_str().unwrap().contains("$1"));
            }
            other => panic!("{other:?}"),
        }
        assert!(engine
            .handle(Request::DestroySession { session_id })
            .is_ok());
        assert_eq!(engine.session_count(), 0);
        let gone = engine.handle(Request::PlanAndUnparse {
            session_id,
            sql: "SELECT 1".into(),
            params: vec![],
        });
        match gone {
            Response::Err { error } => assert_eq!(error.kind, ErrorKind::SessionMissing),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn session_without_catalog_is_a_configuration_error() {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        let response = engine.handle(Request::CreateSession {
            context: SessionContext {
                database_id: 7,
                ..Default::default()
            },
        });
        match response {
            Response::Err { error } => {
                assert_eq!(error.kind, ErrorKind::CatalogMissing);
                assert_eq!(error.error_type, "bad-configuration");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn invalidate_drops_catalog_and_database() {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        engine.handle_json(&snapshot_json().to_string());
        let response = engine.handle(Request::InvalidateDatabase { database_id: 42 });
        match response {
            Response::Ok { result } => {
                assert_eq!(result["catalog"], true);
                assert_eq!(result["database"], false);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn malformed_json_is_an_invalid_request() {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        match engine.handle_json("{not json") {
            Response::Err { error } => assert_eq!(error.kind, ErrorKind::InvalidRequest),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn debug_panic_is_refused_unless_enabled() {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        match engine.handle(Request::DebugPanic) {
            Response::Err { error } => assert_eq!(error.kind, ErrorKind::InvalidRequest),
            other => panic!("{other:?}"),
        }
    }
}
