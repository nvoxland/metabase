//! Postgres connector: a deadpool of tokio-postgres clients, per-statement session settings
//! (`SET` on checkout, `RESET` on release), bound parameters, and rows decoded to Arrow.
//!
//! Every statement runs with `default_transaction_read_only = on`, so read-only is enforced by the
//! server rather than hinted. Cancellation uses the protocol-level cancel request on a separate
//! connection; a statement timeout is set server-side.

mod config;
mod decode;
mod params;

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use deadpool_postgres::{
    Manager, ManagerConfig, Object, Pool as DeadPool, RecyclingMethod, Runtime,
};
use engine_core::api::PoolConfig;
use engine_core::connector::{Connector, ExecuteHandle, Pool, PoolStats, Query, SessionSettings};
use engine_core::error::{EngineError, ErrorKind, Result};
use engine_dialects::Warehouse;
use futures::Stream;
use futures::StreamExt;
use serde_json::Value;
use tokio_postgres::{NoTls, RowStream, Statement};
use tokio_util::sync::CancellationToken;

use crate::config::Tls;
use crate::decode::RowDecoder;

pub struct PostgresConnector;

pub struct PgPool {
    pool: DeadPool,
    tls: Tls,
}

#[async_trait]
impl Pool for PgPool {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn stats(&self) -> PoolStats {
        let status = self.pool.status();
        PoolStats {
            max_size: status.max_size,
            size: status.size,
            available: status.available,
            waiting: status.waiting,
        }
    }

    fn reap_idle(&self, idle_for: std::time::Duration) {
        let removed = self
            .pool
            .retain(|_, metrics| metrics.last_used() < idle_for);
        if !removed.removed.is_empty() {
            tracing::debug!(
                closed = removed.removed.len(),
                "closed idle Postgres connections"
            );
        }
    }

    async fn close(&self) {
        self.pool.close();
    }
}

fn pg_pool(pool: &dyn Pool) -> Result<&PgPool> {
    pool.as_any()
        .downcast_ref::<PgPool>()
        .ok_or_else(|| EngineError::new(ErrorKind::InvalidRequest, "pool is not a Postgres pool"))
}

/// Map a tokio-postgres error onto the engine's error kinds, keeping the SQLSTATE.
pub fn warehouse_error(e: tokio_postgres::Error, cancelled: bool) -> EngineError {
    if let Some(db) = e.as_db_error() {
        let state = db.code().code();
        let kind = match state {
            "57014" if db.message().contains("statement timeout") => ErrorKind::Timeout,
            "57014" if cancelled => ErrorKind::Cancelled,
            "57014" => ErrorKind::Cancelled,
            "42601" | "42703" | "42P01" | "42883" | "42804" | "22P02" => ErrorKind::Warehouse,
            _ => ErrorKind::Warehouse,
        };
        let mut err = EngineError::new(kind, db.message().to_string());
        err.sql_state = Some(state.to_string());
        err.warehouse_message = Some(db.to_string());
        err.warehouse = Some("postgres".into());
        return err;
    }
    let message = e.to_string();
    let kind = if e.is_closed() {
        ErrorKind::Connect
    } else if message.contains("timed out") || message.contains("timeout") {
        ErrorKind::Timeout
    } else {
        ErrorKind::Warehouse
    };
    let mut err = EngineError::new(kind, message);
    err.warehouse = Some("postgres".into());
    err
}

fn pool_error(e: deadpool_postgres::PoolError) -> EngineError {
    use deadpool_postgres::PoolError;
    match e {
        PoolError::Timeout(_) => EngineError::new(
            ErrorKind::PoolTimeout,
            "timed out waiting for a Postgres connection",
        ),
        PoolError::Backend(e) => {
            let mut err = warehouse_error(e, false);
            if err.kind == ErrorKind::Warehouse {
                err.kind = ErrorKind::Connect;
                err.error_type = ErrorKind::Connect.error_type().to_string();
            }
            err
        }
        PoolError::Closed => EngineError::new(ErrorKind::Connect, "the connection pool is closed"),
        other => EngineError::new(ErrorKind::Connect, format!("connection pool: {other}")),
    }
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// The report time zone `SET`, applied separately so a zone the server lacks can be tolerated.
fn time_zone_sql(tz: &str) -> String {
    format!("SET TIME ZONE {};", quote_literal(tz))
}

/// `SET` statements applied to a checked-out connection for one statement (the time zone is
/// applied separately, see `time_zone_sql`).
fn session_setup_sql(settings: &SessionSettings) -> String {
    let mut sql = String::new();
    if settings.read_only {
        sql.push_str("SET default_transaction_read_only = on;\n");
    }
    if let Some(ms) = settings.statement_timeout_ms {
        sql.push_str(&format!("SET statement_timeout = {ms};\n"));
    }
    if let Some(role) = &settings.role {
        // `NONE` is Postgres' keyword for "the session user", not a role name.
        if role.eq_ignore_ascii_case("none") {
            sql.push_str("SET ROLE NONE;\n");
        } else {
            sql.push_str(&format!("SET ROLE {};\n", quote_ident(role)));
        }
    }
    sql
}

const SESSION_RESET_SQL: &str = "RESET ALL; RESET ROLE;";

/// Wrap a statement so undecodable columns come back as text.
fn text_cast_wrapper(sql: &str, statement: &Statement) -> String {
    let columns: Vec<String> = statement
        .columns()
        .iter()
        .map(|c| {
            let name = quote_ident(c.name());
            if decode::arrow_type_for(c.type_()).is_some() {
                name
            } else {
                format!("{name}::text AS {name}")
            }
        })
        .collect();
    format!(
        "SELECT {} FROM (\n{}\n) AS \"mb_q\"",
        columns.join(", "),
        sql
    )
}

/// Pulls rows from Postgres and emits Arrow batches; holds the connection until finished, then
/// resets its session state before it goes back to the pool.
struct RowBatches {
    /// `None` once handed to the cleanup that drains a capped statement.
    rows: Option<Pin<Box<RowStream>>>,
    decoder: RowDecoder,
    client: Option<Object>,
    cancel: CancellationToken,
    done: bool,
    /// Rows still to return under the query's cap, if it has one.
    remaining: Option<u64>,
    /// The cap was reached: the statement is stopped and the connection drained and kept.
    capped: bool,
    /// The engine runtime, so cleanup can be scheduled from a thread that isn't inside it (the JVM
    /// releases streams from its own threads).
    runtime: tokio::runtime::Handle,
    /// Waits for the cancel token and sends the protocol-level cancel; aborted when the statement
    /// finishes on its own so it doesn't outlive the query.
    cancel_watcher: Option<tokio::task::JoinHandle<()>>,
}

impl RowBatches {
    /// The cap was reached. Like JDBC's `setMaxRows`, stop the statement but keep the connection:
    /// send the protocol cancel, wait until the server has ended the statement (draining whatever
    /// was already in flight), then reset the session and return the connection to the pool. A
    /// cancel that arrives after the statement finished on its own can only hit the reset, which
    /// then fails and discards the connection.
    fn release_capped(&mut self) {
        self.capped = true;
        let watcher = self.cancel_watcher.take();
        let rows = self.rows.take();
        let client = self.client.take();
        self.cancel.cancel();
        self.runtime.spawn(async move {
            if let Some(watcher) = watcher {
                let _ = watcher.await;
            }
            if let Some(mut rows) = rows {
                while let Some(item) = rows.next().await {
                    if item.is_err() {
                        break;
                    }
                }
            }
            if let Some(client) = client {
                match client.batch_execute(SESSION_RESET_SQL).await {
                    Ok(()) => drop(client),
                    Err(e) => {
                        tracing::debug!(error = %e, "reset after a capped statement failed; discarding the connection");
                        drop(Object::take(client));
                    }
                }
            }
        });
    }

    fn release(&mut self) {
        if self.capped {
            return;
        }
        if let Some(watcher) = self.cancel_watcher.take() {
            if !self.cancel.is_cancelled() {
                watcher.abort();
            }
        }
        if let Some(client) = self.client.take() {
            let cancelled = self.cancel.is_cancelled();
            self.runtime.spawn(async move {
                // A cancelled statement's connection is discarded rather than reset: the protocol-level
                // cancel races anything else run on it, and a `RESET ROLE` it killed would hand the
                // next checkout the previous user's role. A reset that fails is discarded the same way.
                if cancelled {
                    drop(Object::take(client));
                    return;
                }
                match client.batch_execute(SESSION_RESET_SQL).await {
                    Ok(()) => drop(client),
                    Err(e) => {
                        tracing::warn!(error = %e, "could not reset the Postgres session; discarding the connection");
                        drop(Object::take(client));
                    }
                }
            });
        }
    }
}

impl Drop for RowBatches {
    fn drop(&mut self) {
        if !self.done {
            // Dropped mid-stream: stop the server-side query so the connection comes back clean.
            self.cancel.cancel();
        }
        self.release();
    }
}

impl Stream for RowBatches {
    type Item =
        std::result::Result<arrow::record_batch::RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.remaining == Some(0) {
            self.done = true;
            self.release_capped();
            return Poll::Ready(None);
        }
        loop {
            let Some(rows) = self.rows.as_mut() else {
                return Poll::Ready(None);
            };
            match rows.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(row))) => {
                    if let Err(e) = self.decoder.push(&row) {
                        self.done = true;
                        self.release();
                        return Poll::Ready(Some(Err(e.into())));
                    }
                    if let Some(remaining) = self.remaining.as_mut() {
                        *remaining -= 1;
                        if *remaining == 0 {
                            self.done = true;
                            self.release_capped();
                            return Poll::Ready(
                                self.decoder
                                    .flush()
                                    .transpose()
                                    .map(|r| r.map_err(Into::into)),
                            );
                        }
                    }
                    if self.decoder.is_full() {
                        return Poll::Ready(
                            self.decoder
                                .flush()
                                .transpose()
                                .map(|r| r.map_err(Into::into)),
                        );
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    self.done = true;
                    let cancelled = self.cancel.is_cancelled();
                    self.release();
                    return Poll::Ready(Some(Err(warehouse_error(e, cancelled).into())));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    self.release();
                    return Poll::Ready(
                        self.decoder
                            .flush()
                            .transpose()
                            .map(|r| r.map_err(Into::into)),
                    );
                }
            }
        }
    }
}

const BATCH_SIZE: usize = 8192;

/// Prepare `sql`, letting Postgres infer placeholder types; when it can't (SQLSTATE 42P18,
/// e.g. `SELECT $1`), declare them from the Arrow types Metabase attached to the values.
async fn prepare_with_inference(
    client: &Object,
    sql: &str,
    params: &[engine_core::api::ParamValue],
) -> std::result::Result<Statement, tokio_postgres::Error> {
    match client.prepare(sql).await {
        Ok(statement) => Ok(statement),
        Err(e)
            if e.as_db_error().map(|d| d.code().code()) == Some("42P18") && !params.is_empty() =>
        {
            let types: Vec<tokio_postgres::types::Type> = params
                .iter()
                .map(|p| params::declared_type(&p.arrow_type))
                .collect();
            client.prepare_typed(sql, &types).await
        }
        Err(e) => Err(e),
    }
}

/// Apply session settings, prepare, bind, and start streaming rows on a checked-out connection.
/// On failure the connection comes back with the error so the caller can reset it.
async fn start_statement(
    pg: &PgPool,
    client: Object,
    query: Query,
) -> std::result::Result<ExecuteHandle, (Object, EngineError)> {
    macro_rules! fail {
        ($e:expr) => {
            return Err((client, $e))
        };
    }
    let setup = session_setup_sql(&query.settings);
    if !setup.is_empty() {
        if let Err(e) = client.batch_execute(&setup).await {
            fail!(warehouse_error(e, false));
        }
    }
    // The report time zone is applied on its own and, as on the JDBC path, a zone the server
    // doesn't know is logged and skipped rather than failing the query.
    if let Some(tz) = &query.settings.time_zone {
        if let Err(e) = client.batch_execute(&time_zone_sql(tz)).await {
            tracing::warn!(time_zone = tz, error = %e, "could not set the session time zone; running without it");
        }
    }
    let mut statement = match prepare_with_inference(&client, &query.sql, &query.params).await {
        Ok(s) => s,
        Err(e) => fail!(warehouse_error(e, false)),
    };
    let mut sql = query.sql.clone();
    if statement
        .columns()
        .iter()
        .any(|c| decode::arrow_type_for(c.type_()).is_none())
    {
        sql = text_cast_wrapper(&sql, &statement);
        statement = match client.prepare(&sql).await {
            Ok(s) => s,
            Err(e) => fail!(warehouse_error(e, false)),
        };
    }
    if query.params.len() != statement.params().len() {
        fail!(EngineError::new(
            ErrorKind::InvalidRequest,
            format!(
                "statement expects {} parameters, got {}",
                statement.params().len(),
                query.params.len()
            ),
        ));
    }
    let mut params: Vec<params::BoundParam> = Vec::with_capacity(query.params.len());
    let session_tz = query.settings.time_zone.as_deref();
    for (i, (p, ty)) in query.params.iter().zip(statement.params()).enumerate() {
        match params::bind(i, p, ty, session_tz) {
            Ok(bound) => params.push(bound),
            Err(e) => fail!(e),
        }
    }
    let decoder = match RowDecoder::new(statement.columns(), BATCH_SIZE) {
        Ok(d) => d,
        Err(e) => fail!(e),
    };
    let schema = decoder.schema();
    let rows = match client.query_raw(&statement, params).await {
        Ok(rows) => rows,
        Err(e) => fail!(warehouse_error(e, false)),
    };

    let cancel = CancellationToken::new();
    let cancel_token = client.cancel_token();
    let tls = pg.tls.connector();
    let watcher = cancel.clone();
    let cancel_watcher = tokio::spawn(async move {
        watcher.cancelled().await;
        let result = match tls {
            None => cancel_token.cancel_query(NoTls).await,
            Some(tls) => cancel_token.cancel_query(tls).await,
        };
        if let Err(e) = result {
            tracing::debug!(error = %e, "sending Postgres cancel request");
        }
    });

    let batches = RowBatches {
        rows: Some(Box::pin(rows)),
        decoder,
        client: Some(client),
        cancel: cancel.clone(),
        done: false,
        remaining: query.max_rows,
        capped: false,
        runtime: tokio::runtime::Handle::current(),
        cancel_watcher: Some(cancel_watcher),
    };
    Ok(ExecuteHandle {
        stream: Box::pin(RecordBatchStreamAdapter::new(schema, batches)),
        cancel,
    })
}

#[async_trait]
impl Connector for PostgresConnector {
    fn kind(&self) -> &'static str {
        "postgres"
    }

    fn warehouse(&self) -> Warehouse {
        Warehouse::Postgres
    }

    async fn connect(&self, details: &Value, config: &PoolConfig) -> Result<Arc<dyn Pool>> {
        let pg = config::from_details(details, Duration::from_millis(config.connect_timeout_ms))?;
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = match pg.tls.connector() {
            None => Manager::from_config(pg.config, NoTls, manager_config),
            Some(tls) => Manager::from_config(pg.config, tls, manager_config),
        };
        let pool = DeadPool::builder(manager)
            .max_size(config.max_size.max(1))
            .wait_timeout(Some(Duration::from_millis(config.checkout_timeout_ms)))
            .create_timeout(Some(Duration::from_millis(config.connect_timeout_ms)))
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| EngineError::new(ErrorKind::Connect, format!("building pool: {e}")))?;
        Ok(Arc::new(PgPool { pool, tls: pg.tls }))
    }

    async fn execute(&self, pool: &dyn Pool, query: Query) -> Result<ExecuteHandle> {
        let pg = pg_pool(pool)?;
        let client = pg.pool.get().await.map_err(pool_error)?;
        // Anything that fails after checkout must reset the session before the connection goes back.
        match start_statement(pg, client, query).await {
            Ok(handle) => Ok(handle),
            Err((client, e)) => {
                let _ = client.batch_execute(SESSION_RESET_SQL).await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_setup_quotes_and_orders_settings() {
        let sql = session_setup_sql(&SessionSettings {
            time_zone: Some("America/New_York".into()),
            statement_timeout_ms: Some(1500),
            role: Some("analyst\"; DROP".into()),
            read_only: true,
            application_name: "metabase".into(),
        });
        assert_eq!(
            sql,
            "SET default_transaction_read_only = on;\nSET statement_timeout = 1500;\nSET ROLE \"analyst\"\"; DROP\";\n"
        );
        assert_eq!(session_setup_sql(&SessionSettings::default()), "");
        assert_eq!(
            time_zone_sql("America/New_York"),
            "SET TIME ZONE 'America/New_York';"
        );
        assert_eq!(time_zone_sql("x'; DROP"), "SET TIME ZONE 'x''; DROP';");
        assert_eq!(
            session_setup_sql(&SessionSettings {
                role: Some("NONE".into()),
                ..Default::default()
            }),
            "SET ROLE NONE;\n"
        );
    }
}
