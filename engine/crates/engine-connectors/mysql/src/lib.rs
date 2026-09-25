//! MySQL and MariaDB connector: a mysql_async pool, per-statement session settings, bound
//! parameters, and rows decoded to Arrow.
//!
//! Every checkout sets the session up explicitly (`NO_BACKSLASH_ESCAPES` so the unparser's
//! standard string literals mean what they say, the time zone, read-only, a statement time
//! limit, the role), so nothing a previous statement left behind is relied on or has to be reset.
//! Cancellation is `KILL QUERY` from a second connection; the time limit is server-side.

mod config;
mod decode;
mod params;
mod placeholders;

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::timezone::Tz;
use async_trait::async_trait;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use engine_core::api::PoolConfig;
use engine_core::connector::{Connector, ExecuteHandle, Pool, PoolStats, Query, SessionSettings};
use engine_core::error::{EngineError, ErrorKind, Result};
use engine_dialects::Warehouse;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params};
use tokio_util::sync::CancellationToken;

use crate::decode::RowDecoder;

pub struct MySqlConnector;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flavor {
    MySql,
    MariaDb,
}

pub struct MyPool {
    pool: mysql_async::Pool,
    max_size: usize,
    checkout_timeout: Duration,
    flavor: Flavor,
    /// Connections (by server id) on which a statement activated a role. A user's default role
    /// is active at login and `SET ROLE NONE` would drop it, so a statement without a role
    /// resets such a connection to its login state instead.
    role_set: dashmap::DashSet<u32>,
}

#[async_trait]
impl Pool for MyPool {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn stats(&self) -> PoolStats {
        use std::sync::atomic::Ordering;
        let m = self.pool.metrics();
        PoolStats {
            max_size: self.max_size,
            size: m.connection_count.load(Ordering::Relaxed),
            available: m.connections_in_pool.load(Ordering::Relaxed),
            waiting: m.active_wait_requests.load(Ordering::Relaxed),
        }
    }

    // mysql_async closes its own idle connections (`config::IDLE_TTL`)

    async fn close(&self) {
        if let Err(e) = self.pool.clone().disconnect().await {
            tracing::debug!(error = %e, "closing the MySQL pool");
        }
    }
}

fn my_pool(pool: &dyn Pool) -> Result<&MyPool> {
    pool.as_any()
        .downcast_ref::<MyPool>()
        .ok_or_else(|| EngineError::new(ErrorKind::InvalidRequest, "pool is not a MySQL pool"))
}

// MySQL 3024 / MariaDB 1969: the statement time limit; 1317: killed
const ER_QUERY_TIMEOUT: u16 = 3024;
const ER_STATEMENT_TIMEOUT: u16 = 1969;
const ER_QUERY_INTERRUPTED: u16 = 1317;

pub fn warehouse_error(e: mysql_async::Error, cancelled: bool) -> EngineError {
    match e {
        mysql_async::Error::Server(server) => {
            let kind = match server.code {
                ER_QUERY_TIMEOUT | ER_STATEMENT_TIMEOUT => ErrorKind::Timeout,
                ER_QUERY_INTERRUPTED if cancelled => ErrorKind::Cancelled,
                ER_QUERY_INTERRUPTED => ErrorKind::Cancelled,
                _ => ErrorKind::Warehouse,
            };
            let mut err = EngineError::new(kind, server.message.clone());
            err.sql_state = Some(server.state.clone());
            err.warehouse_message = Some(format!("({}) {}", server.code, server.message));
            err.warehouse = Some("mysql".into());
            err
        }
        mysql_async::Error::Io(_) | mysql_async::Error::Driver(_) => {
            let mut err = EngineError::new(ErrorKind::Connect, e.to_string());
            err.warehouse = Some("mysql".into());
            err
        }
        other => {
            let mut err = EngineError::new(ErrorKind::Warehouse, other.to_string());
            err.warehouse = Some("mysql".into());
            err
        }
    }
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn quote_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// The session time zone for a statement: the report time zone, else UTC, so `TIMESTAMP` values
/// and `FROM_UNIXTIME` are unambiguous.
fn session_time_zone(settings: &SessionSettings) -> &str {
    settings.time_zone.as_deref().unwrap_or("+00:00")
}

/// The statements that put a checked-out connection into the state one query needs.
fn session_setup_sql(settings: &SessionSettings, flavor: Flavor) -> Vec<String> {
    let mut sql = vec![
        // standard string literals: a quote is doubled, a backslash is a backslash
        "SET SESSION sql_mode = CONCAT_WS(',', @@GLOBAL.sql_mode, 'NO_BACKSLASH_ESCAPES')"
            .to_string(),
        format!(
            "SET SESSION time_zone = {}",
            quote_literal(session_time_zone(settings))
        ),
    ];
    if settings.read_only {
        sql.push("SET SESSION TRANSACTION READ ONLY".to_string());
    }
    let ms = settings.statement_timeout_ms.unwrap_or(0);
    sql.push(match flavor {
        // seconds, fractional
        Flavor::MariaDb => format!("SET SESSION max_statement_time = {}", ms as f64 / 1000.0),
        Flavor::MySql => format!("SET SESSION max_execution_time = {ms}"),
    });
    if let Some(role) = requested_role(settings) {
        sql.push(format!("SET ROLE {}", quote_ident(role)));
    }
    sql
}

/// The role a statement asks for, if any.
fn requested_role(settings: &SessionSettings) -> Option<&str> {
    settings
        .role
        .as_deref()
        .filter(|r| !r.eq_ignore_ascii_case("none"))
}

async fn set_up_session(
    conn: &mut Conn,
    settings: &SessionSettings,
    flavor: Flavor,
    role_set: &dashmap::DashSet<u32>,
) -> Result<()> {
    // Back to the login state (the user's default role, no session variables) when an earlier
    // statement on this connection activated a role and this one wants none; the variables are
    // set again below either way.
    if requested_role(settings).is_none() && role_set.remove(&conn.id()).is_some() {
        conn.reset().await.map_err(|e| warehouse_error(e, false))?;
    }
    for statement in session_setup_sql(settings, flavor) {
        if let Err(e) = conn.query_drop(&statement).await {
            let mut err = warehouse_error(e, false);
            if statement.starts_with("SET SESSION time_zone") {
                err.message = format!(
                    "{} (the MySQL server may lack its time zone tables; load them with mysql_tzinfo_to_sql)",
                    err.message
                );
            }
            return Err(err);
        }
    }
    if requested_role(settings).is_some() {
        role_set.insert(conn.id());
    }
    Ok(())
}

async fn detect_flavor(pool: &mysql_async::Pool) -> Result<Flavor> {
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| warehouse_error(e, false))?;
    let version: Option<String> = conn
        .query_first("SELECT VERSION()")
        .await
        .map_err(|e| warehouse_error(e, false))?;
    Ok(match version {
        Some(v) if v.to_ascii_lowercase().contains("mariadb") => Flavor::MariaDb,
        _ => Flavor::MySql,
    })
}

const BATCH_SIZE: usize = 8192;

/// What the statement task reports back before streaming: the schema, or why it couldn't start.
type Started = std::result::Result<arrow::datatypes::SchemaRef, EngineError>;

#[async_trait]
impl Connector for MySqlConnector {
    fn kind(&self) -> &'static str {
        "mysql"
    }

    fn warehouse(&self) -> Warehouse {
        Warehouse::MySql
    }

    async fn connect(&self, details: &Value, config: &PoolConfig) -> Result<Arc<dyn Pool>> {
        let opts = config::from_details(details, config)?;
        let pool = mysql_async::Pool::new(opts);
        let flavor = tokio::time::timeout(
            Duration::from_millis(config.connect_timeout_ms),
            detect_flavor(&pool),
        )
        .await
        .map_err(|_| EngineError::new(ErrorKind::Connect, "timed out connecting to MySQL"))?
        .map_err(|e| {
            let mut e = e;
            if e.kind == ErrorKind::Warehouse {
                e.kind = ErrorKind::Connect;
                e.error_type = ErrorKind::Connect.error_type().to_string();
            }
            e
        })?;
        Ok(Arc::new(MyPool {
            pool,
            max_size: config.max_size.max(1),
            checkout_timeout: Duration::from_millis(
                config.checkout_timeout_ms + config.connect_timeout_ms,
            ),
            flavor,
            role_set: dashmap::DashSet::new(),
        }))
    }

    async fn execute(&self, pool: &dyn Pool, query: Query) -> Result<ExecuteHandle> {
        let my = my_pool(pool)?;
        let session_tz: Tz = session_time_zone(&query.settings).parse().map_err(|e| {
            EngineError::new(
                ErrorKind::InvalidRequest,
                format!(
                    "unknown report time zone {:?}: {e}",
                    query.settings.time_zone
                ),
            )
        })?;
        let mut conn = tokio::time::timeout(my.checkout_timeout, my.pool.get_conn())
            .await
            .map_err(|_| {
                EngineError::new(
                    ErrorKind::PoolTimeout,
                    "timed out waiting for a MySQL connection",
                )
            })?
            .map_err(|e| {
                let mut e = warehouse_error(e, false);
                if e.kind == ErrorKind::Warehouse {
                    e.kind = ErrorKind::Connect;
                    e.error_type = ErrorKind::Connect.error_type().to_string();
                }
                e
            })?;
        set_up_session(&mut conn, &query.settings, my.flavor, &my.role_set).await?;

        let (sql, order) = placeholders::to_question_marks(&query.sql);
        let mut bound = Vec::with_capacity(order.len());
        for i in order {
            let param = query.params.get(i).ok_or_else(|| {
                EngineError::new(
                    ErrorKind::InvalidRequest,
                    format!(
                        "statement refers to ${} but {} parameters were given",
                        i + 1,
                        query.params.len()
                    ),
                )
            })?;
            bound.push(params::bind(i, param, &session_tz)?);
        }
        let params = if bound.is_empty() {
            Params::Empty
        } else {
            Params::Positional(bound)
        };

        let cancel = CancellationToken::new();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<Started>();
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<
            std::result::Result<
                arrow::record_batch::RecordBatch,
                datafusion::error::DataFusionError,
            >,
        >(2);
        let killer = my.pool.clone();
        let task_cancel = cancel.clone();
        // The statement runs on the engine's runtime whatever thread drops the stream; the
        // connection goes back to the pool when the task ends.
        tokio::spawn(async move {
            let conn_id = conn.id();
            let mut result = match conn.exec_iter(sql, params).await {
                Ok(result) => result,
                Err(e) => {
                    let _ = started_tx.send(Err(warehouse_error(e, false)));
                    return;
                }
            };
            let columns = result.columns_ref().to_vec();
            let mut decoder = RowDecoder::new(&columns, BATCH_SIZE, session_tz);
            if started_tx.send(Ok(decoder.schema())).is_err() {
                return;
            }
            let mut cancelled = false;
            loop {
                let next = tokio::select! {
                    biased;
                    _ = task_cancel.cancelled() => {
                        cancelled = true;
                        break;
                    }
                    next = result.next() => next,
                };
                match next {
                    Ok(Some(row)) => {
                        if let Err(e) = decoder.push(&row) {
                            let _ = batch_tx.send(Err(e.into())).await;
                            cancelled = true;
                            break;
                        }
                        if decoder.is_full() {
                            match decoder.flush() {
                                Ok(Some(batch)) => {
                                    if batch_tx.send(Ok(batch)).await.is_err() {
                                        // the reader went away: stop the query too
                                        cancelled = true;
                                        break;
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    let _ = batch_tx.send(Err(e.into())).await;
                                    cancelled = true;
                                    break;
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        match decoder.flush() {
                            Ok(Some(batch)) => {
                                let _ = batch_tx.send(Ok(batch)).await;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                let _ = batch_tx.send(Err(e.into())).await;
                            }
                        }
                        break;
                    }
                    Err(e) => {
                        let _ = batch_tx
                            .send(Err(warehouse_error(e, task_cancel.is_cancelled()).into()))
                            .await;
                        break;
                    }
                }
            }
            if cancelled {
                // stop the server-side query so the connection comes back promptly
                if let Ok(mut other) = killer.get_conn().await {
                    if let Err(e) = other.query_drop(format!("KILL QUERY {conn_id}")).await {
                        tracing::debug!(error = %e, "KILL QUERY");
                    }
                }
                let _ = batch_tx
                    .send(Err(EngineError::new(
                        ErrorKind::Cancelled,
                        "query cancelled",
                    )
                    .into()))
                    .await;
            }
            drop(result);
            drop(conn);
        });

        let schema = started_rx.await.map_err(|_| {
            EngineError::new(ErrorKind::Warehouse, "the MySQL statement task ended early")
        })??;
        let stream = tokio_stream::wrappers::ReceiverStream::new(batch_rx);
        Ok(ExecuteHandle {
            stream: Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            cancel,
        })
    }
}

use serde_json::Value;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_setup_spells_every_setting_for_each_flavor() {
        let settings = SessionSettings {
            time_zone: Some("America/New_York".into()),
            statement_timeout_ms: Some(1500),
            role: Some("ana`lyst".into()),
            read_only: true,
            application_name: "metabase".into(),
        };
        assert_eq!(
            session_setup_sql(&settings, Flavor::MariaDb),
            vec![
                "SET SESSION sql_mode = CONCAT_WS(',', @@GLOBAL.sql_mode, 'NO_BACKSLASH_ESCAPES')",
                "SET SESSION time_zone = 'America/New_York'",
                "SET SESSION TRANSACTION READ ONLY",
                "SET SESSION max_statement_time = 1.5",
                "SET ROLE `ana``lyst`",
            ]
        );
        let none = session_setup_sql(&SessionSettings::default(), Flavor::MySql);
        assert_eq!(none[1], "SET SESSION time_zone = '+00:00'");
        assert_eq!(none[2], "SET SESSION max_execution_time = 0");
        // no role asked for: the login's default role stays active (nothing is unset here)
        assert_eq!(none.len(), 3);
    }
}
