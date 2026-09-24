//! The engine: one tokio runtime, catalog snapshots per database, connection pools per database
//! and role, sessions per request, and the statements currently streaming. Both front-ends call
//! `handle` / `handle_async` with the same messages and `execute` / `take_stream` for data.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::runtime::Runtime;

use crate::api::{DatabaseRegistration, ParamValue, Request, Response, SessionContext};
use crate::catalog::SnapshotCatalog;
use crate::connector::{Connector, Query, SessionSettings};
use crate::error::{EngineError, ErrorKind, Result};
use crate::planner::Planner;
use crate::pools::PoolRegistry;
use crate::schema_cast::SchemaCastStream;
use crate::stream::{BlockingReader, StreamEntry, StreamStatsSnapshot};

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
    streams: DashMap<u64, Arc<StreamEntry>>,
    pools: PoolRegistry,
    next_id: AtomicU64,
}

impl Engine {
    /// An engine with no warehouse connectors: it can plan and unparse but not execute.
    pub fn new(config: EngineConfig) -> Result<Arc<Engine>> {
        Engine::with_connectors(config, vec![])
    }

    pub fn with_connectors(
        config: EngineConfig,
        connectors: Vec<Arc<dyn Connector>>,
    ) -> Result<Arc<Engine>> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        // Planning and unparsing recurse deeply on nested plans; JVM threads have small stacks, so all
        // engine work runs on these workers (see `run_on_runtime`) and they get room to recurse.
        builder
            .enable_all()
            .thread_name("metabase-engine")
            .thread_stack_size(16 * 1024 * 1024);
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
            streams: DashMap::new(),
            pools: PoolRegistry::new(connectors),
            next_id: AtomicU64::new(1),
        }))
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn version() -> &'static str {
        VERSION
    }

    /// Run a future on the engine's worker threads and wait for it from the calling (non-runtime)
    /// thread. `block_on` alone would poll the future on the caller's own stack, which for a JVM
    /// thread is far too small for the planner's recursion.
    pub fn run_on_runtime<F, T>(self: &Arc<Self>, make: F) -> T
    where
        F: FnOnce(Arc<Engine>) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>
            + Send
            + 'static,
        T: Send + 'static,
    {
        let engine = Arc::clone(self);
        let task = self.runtime.spawn(async move { make(engine).await });
        self.runtime
            .block_on(async move { task.await.expect("engine task panicked or was cancelled") })
    }

    /// Handle a request from a thread that is not a runtime worker (the C ABI path).
    /// Panics if called from inside the runtime; use `handle_async` there.
    pub fn handle(self: &Arc<Self>, request: Request) -> Response {
        self.run_on_runtime(move |engine| {
            Box::pin(async move { engine.handle_async(request).await })
        })
    }

    pub fn handle_json(self: &Arc<Self>, request_json: &str) -> Response {
        match serde_json::from_str::<Request>(request_json) {
            Ok(request) => self.handle(request),
            Err(e) => Response::err(e.into()),
        }
    }

    /// [`Engine::execute`] for a non-runtime thread: plans on a worker thread and waits.
    pub fn execute_blocking(
        self: &Arc<Self>,
        session_id: u64,
        sql: String,
        params: Vec<ParamValue>,
    ) -> Result<u64> {
        self.run_on_runtime(move |engine| {
            Box::pin(async move { engine.execute(session_id, &sql, params).await })
        })
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
                let pools = self.pools.invalidate(database_id).await;
                Ok(json!({ "catalog": had_catalog, "database": had_database, "pools": pools }))
            }
            Request::RegisterDatabase { registration } => {
                if !self.pools.has_connector(&registration.connector) {
                    return Err(EngineError::new(
                        ErrorKind::CatalogMissing,
                        format!(
                            "no connector for {:?} in this engine build",
                            registration.connector
                        ),
                    ));
                }
                let id = registration.database_id;
                let details_changed = self
                    .databases
                    .get(&id)
                    .map(|old| {
                        old.details != registration.details
                            || old.pool.max_size != registration.pool.max_size
                    })
                    .unwrap_or(false);
                self.databases.insert(id, Arc::new(registration));
                let closed = if details_changed {
                    self.pools.invalidate(id).await
                } else {
                    0
                };
                Ok(json!({ "database_id": id, "pools_closed": closed }))
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
            Request::Cancel { stream_id } => {
                Ok(json!({ "cancelled": self.cancel_stream(stream_id) }))
            }
            Request::PoolStats { database_id } => {
                Ok(json!({ "pools": self.pools.stats(database_id) }))
            }
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

    pub fn destroy_session(&self, id: u64) -> bool {
        self.sessions.remove(&id).is_some()
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Plan `sql` in the session and start it on the warehouse. Returns a stream id; batches are
    /// pulled through [`Engine::take_stream`] or [`Engine::blocking_reader`].
    pub async fn execute(
        &self,
        session_id: u64,
        sql: &str,
        params: Vec<ParamValue>,
    ) -> Result<u64> {
        let session = self.session(session_id)?;
        let planned = session.planner.plan(sql).await?;
        let registration = self
            .databases
            .get(&session.context.database_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| {
                EngineError::new(
                    ErrorKind::CatalogMissing,
                    format!(
                        "database {} is not registered with the engine",
                        session.context.database_id
                    ),
                )
            })?;
        if registration.connector != session.catalog.connector {
            return Err(EngineError::new(
                ErrorKind::CatalogMissing,
                format!(
                    "database {} is registered as {:?} but its catalog says {:?}",
                    registration.database_id, registration.connector, session.catalog.connector
                ),
            ));
        }
        let (connector, pool) = self
            .pools
            .pool_for(&registration, session.context.role.as_deref())
            .await?;
        let settings = SessionSettings {
            time_zone: session.context.report_timezone.clone(),
            statement_timeout_ms: session.context.statement_timeout_ms,
            role: session.context.role.clone(),
            read_only: true,
            application_name: "metabase".into(),
        };
        let handle = connector
            .execute(
                pool.as_ref(),
                Query {
                    sql: planned.warehouse_sql,
                    params,
                    settings,
                },
            )
            .await?;
        let stream = SchemaCastStream::new(handle.stream, &planned.schema);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.streams.insert(
            id,
            Arc::new(StreamEntry::new(
                session_id,
                Box::pin(stream),
                handle.cancel,
            )),
        );
        Ok(id)
    }

    /// Run warehouse-dialect `sql` in the session's database without planning it. For Metabase's own
    /// metadata queries (sync), which are written in the warehouse's dialect; not a user-facing path.
    pub async fn execute_raw(
        &self,
        session_id: u64,
        sql: &str,
        params: Vec<ParamValue>,
    ) -> Result<u64> {
        let session = self.session(session_id)?;
        let registration = self
            .databases
            .get(&session.context.database_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| {
                EngineError::new(
                    ErrorKind::CatalogMissing,
                    format!(
                        "database {} is not registered with the engine",
                        session.context.database_id
                    ),
                )
            })?;
        let (connector, pool) = self
            .pools
            .pool_for(&registration, session.context.role.as_deref())
            .await?;
        let settings = SessionSettings {
            time_zone: session.context.report_timezone.clone(),
            statement_timeout_ms: session.context.statement_timeout_ms,
            role: session.context.role.clone(),
            read_only: true,
            application_name: "metabase-sync".into(),
        };
        let handle = connector
            .execute(
                pool.as_ref(),
                Query {
                    sql: sql.to_string(),
                    params,
                    settings,
                },
            )
            .await?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.streams.insert(
            id,
            Arc::new(StreamEntry::new(session_id, handle.stream, handle.cancel)),
        );
        Ok(id)
    }

    /// [`Engine::execute_raw`] for a non-runtime thread.
    pub fn execute_raw_blocking(
        self: &Arc<Self>,
        session_id: u64,
        sql: String,
        params: Vec<ParamValue>,
    ) -> Result<u64> {
        self.run_on_runtime(move |engine| {
            Box::pin(async move { engine.execute_raw(session_id, &sql, params).await })
        })
    }

    pub fn stream(&self, id: u64) -> Result<Arc<StreamEntry>> {
        self.streams
            .get(&id)
            .map(|s| Arc::clone(s.value()))
            .ok_or_else(|| EngineError::new(ErrorKind::InvalidRequest, format!("no stream {id}")))
    }

    /// Take a stream's batches for the Flight path. Only the first call succeeds.
    pub fn take_stream(
        &self,
        id: u64,
    ) -> Result<(
        Arc<StreamEntry>,
        datafusion::physical_plan::SendableRecordBatchStream,
    )> {
        let entry = self.stream(id)?;
        let stream = entry.take().ok_or_else(|| {
            EngineError::new(
                ErrorKind::InvalidRequest,
                format!("stream {id} was already exported"),
            )
        })?;
        Ok((entry, stream))
    }

    /// A synchronous reader over a stream's batches for the C Data Interface. Only the first call succeeds.
    /// The stream itself is driven on a worker thread; the reader only waits on a channel.
    pub fn blocking_reader(&self, id: u64) -> Result<BlockingReader> {
        let (entry, stream) = self.take_stream(id)?;
        Ok(BlockingReader::spawn(
            self.runtime.handle().clone(),
            stream,
            entry.cancel.clone(),
            Arc::clone(&entry.stats),
        ))
    }

    pub fn cancel_stream(&self, id: u64) -> bool {
        match self.streams.get(&id) {
            Some(entry) => {
                entry.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Forget a stream. Cancels it if it is still running.
    pub fn close_stream(&self, id: u64) -> bool {
        match self.streams.remove(&id) {
            Some((_, entry)) => {
                if !entry.exported() {
                    entry.cancel.cancel();
                }
                true
            }
            None => false,
        }
    }

    pub fn stream_stats(&self, id: u64) -> Result<StreamStatsSnapshot> {
        Ok(self.stream(id)?.snapshot())
    }

    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Stop accepting work and wait for the runtime to drain, bounded by `timeout`.
    pub fn shutdown(self: Arc<Engine>, timeout: Duration) {
        for entry in self.streams.iter() {
            entry.cancel.cancel();
        }
        self.streams.clear();
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
    fn execute_without_a_registered_database_is_a_configuration_error() {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        engine.handle_json(&snapshot_json().to_string());
        let session_id = engine
            .create_session(SessionContext {
                database_id: 42,
                ..Default::default()
            })
            .unwrap();
        let err = engine
            .runtime()
            .block_on(engine.execute(session_id, r#"SELECT "ID" FROM "public"."Orders""#, vec![]))
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::CatalogMissing);
        assert!(err.message.contains("not registered"), "{}", err.message);
    }

    #[test]
    fn registering_a_database_needs_a_connector_in_the_build() {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        let response = engine.handle_json(
            r#"{"op":"register_database","registration":{"database_id":42,"connector":"postgres","details":{}}}"#,
        );
        match response {
            Response::Err { error } => assert_eq!(error.kind, ErrorKind::CatalogMissing),
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
                assert_eq!(result["pools"], 0);
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

    #[test]
    fn unknown_streams_are_invalid_requests() {
        let engine = Engine::new(EngineConfig::default()).unwrap();
        assert!(!engine.cancel_stream(99));
        assert!(!engine.close_stream(99));
        assert_eq!(
            engine.stream_stats(99).unwrap_err().kind,
            ErrorKind::InvalidRequest
        );
    }
}
