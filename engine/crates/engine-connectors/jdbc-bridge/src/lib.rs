//! The JDBC bridge connector: warehouses without a good Rust driver are reached through a small
//! Arrow Flight service Metabase itself hosts over its existing JDBC connection pools
//! (`metabase.engine-bridge.core`). The engine plans and unparses as usual; this connector only
//! carries the warehouse SQL over and the Arrow batches back. Details:
//!
//! ```json
//! {"url": "http://127.0.0.1:50052", "token": "...", "database_id": 7, "warehouse": "postgres"}
//! ```

use std::any::Any;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, Ticket};
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use engine_core::api::PoolConfig;
use engine_core::connector::{Connector, ExecuteHandle, Pool, PoolStats, Query, TableDesc};
use engine_core::error::{EngineError, ErrorKind, Result};
use engine_dialects::Warehouse;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

#[derive(Debug, Clone, Deserialize)]
struct BridgeDetails {
    url: String,
    token: String,
    database_id: i64,
    #[serde(default)]
    warehouse: Option<String>,
}

/// One bridge "pool": a lazily connected gRPC channel to the bridge and what to put in tickets.
/// The real connection pool is the JDBC one on the Metabase side.
pub struct BridgePool {
    channel: Channel,
    details: BridgeDetails,
    max_size: usize,
    in_flight: AtomicUsize,
}

#[async_trait]
impl Pool for BridgePool {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn stats(&self) -> PoolStats {
        let in_flight = self.in_flight.load(Ordering::Relaxed);
        PoolStats {
            max_size: self.max_size,
            size: in_flight,
            available: self.max_size.saturating_sub(in_flight),
            waiting: 0,
        }
    }

    async fn close(&self) {}
}

/// What the bridge reads from a `DoGet` ticket.
#[derive(Debug, Serialize)]
struct BridgeTicket<'a> {
    token: &'a str,
    database_id: i64,
    sql: &'a str,
    params: &'a [engine_core::api::ParamValue],
    settings: TicketSettings<'a>,
}

#[derive(Debug, Serialize)]
struct TicketSettings<'a> {
    time_zone: Option<&'a str>,
    statement_timeout_ms: Option<u64>,
    role: Option<&'a str>,
    read_only: bool,
}

#[derive(Default)]
pub struct JdbcBridgeConnector;

fn bridge_pool(pool: &dyn Pool) -> Result<&BridgePool> {
    pool.as_any().downcast_ref::<BridgePool>().ok_or_else(|| {
        EngineError::new(ErrorKind::InvalidRequest, "pool is not a JDBC bridge pool")
    })
}

/// The bridge reports statement failures as a gRPC status whose message is the engine's error
/// JSON; anything else is a transport failure.
fn flight_error(e: FlightError) -> EngineError {
    match e {
        FlightError::Tonic(status) => {
            let message = status.message();
            if let Ok(value) = serde_json::from_str::<Value>(message) {
                if let Some(text) = value.get("message").and_then(Value::as_str) {
                    let kind = match value.get("kind").and_then(Value::as_str) {
                        Some("warehouse") => ErrorKind::Warehouse,
                        Some("unsupported") => ErrorKind::Unsupported,
                        Some("cancelled") => ErrorKind::Cancelled,
                        _ => ErrorKind::Warehouse,
                    };
                    return EngineError::new(kind, text);
                }
            }
            EngineError::new(
                ErrorKind::Connect,
                format!("JDBC bridge call failed: {} {}", status.code(), message),
            )
        }
        other => EngineError::new(
            ErrorKind::Warehouse,
            format!("JDBC bridge stream failed: {other}"),
        ),
    }
}

/// Pull messages until the schema arrives, then hand back the decoder positioned at the batches.
async fn schema_and_batches(
    mut decoder: FlightDataDecoder,
) -> Result<(SchemaRef, FlightDataDecoder)> {
    while let Some(message) = decoder.next().await {
        let message = message.map_err(flight_error)?;
        match message.payload {
            DecodedPayload::Schema(schema) => return Ok((schema, decoder)),
            DecodedPayload::None => continue,
            DecodedPayload::RecordBatch(_) => {
                return Err(EngineError::new(
                    ErrorKind::Warehouse,
                    "JDBC bridge sent a batch before its schema",
                ))
            }
        }
    }
    Err(EngineError::new(
        ErrorKind::Warehouse,
        "JDBC bridge closed the stream before sending a schema",
    ))
}

#[async_trait]
impl Connector for JdbcBridgeConnector {
    fn kind(&self) -> &'static str {
        "jdbc-bridge"
    }

    fn warehouse(&self) -> Warehouse {
        // The dialect comes from the catalog snapshot, not from this connector; see `Planner::new`.
        Warehouse::Postgres
    }

    async fn connect(&self, details: &Value, config: &PoolConfig) -> Result<Arc<dyn Pool>> {
        let details: BridgeDetails = serde_json::from_value(details.clone())
            .map_err(|e| EngineError::invalid_request(format!("JDBC bridge details: {e}")))?;
        let channel = Channel::from_shared(details.url.clone())
            .map_err(|e| EngineError::invalid_request(format!("JDBC bridge url: {e}")))?
            .connect_lazy();
        Ok(Arc::new(BridgePool {
            channel,
            details,
            max_size: config.max_size,
            in_flight: AtomicUsize::new(0),
        }))
    }

    async fn execute(&self, pool: &dyn Pool, query: Query) -> Result<ExecuteHandle> {
        let bridge = bridge_pool(pool)?;
        let ticket = BridgeTicket {
            token: &bridge.details.token,
            database_id: bridge.details.database_id,
            sql: &query.sql,
            params: &query.params,
            settings: TicketSettings {
                time_zone: query.settings.time_zone.as_deref(),
                statement_timeout_ms: query.settings.statement_timeout_ms,
                role: query.settings.role.as_deref(),
                read_only: query.settings.read_only,
            },
        };
        let ticket = serde_json::to_vec(&ticket)?;
        let mut client = FlightClient::new(bridge.channel.clone());
        let stream = client
            .do_get(Ticket::new(ticket))
            .await
            .map_err(flight_error)?;
        let (schema, decoder) = schema_and_batches(stream.into_inner()).await?;
        let cancel = CancellationToken::new();
        let batches = decoder
            .try_filter_map(|message| async move {
                Ok(match message.payload {
                    DecodedPayload::RecordBatch(batch) => Some(batch),
                    _ => None,
                })
            })
            .map_err(|e| DataFusionError::External(Box::new(flight_error(e))))
            .take_until(cancel.clone().cancelled_owned());
        let batches: Pin<
            Box<
                dyn Stream<Item = datafusion::error::Result<arrow::record_batch::RecordBatch>>
                    + Send,
            >,
        > = Box::pin(batches);
        tracing::debug!(
            database_id = bridge.details.database_id,
            warehouse = ?bridge.details.warehouse,
            "statement started through the JDBC bridge"
        );
        Ok(ExecuteHandle {
            stream: Box::pin(RecordBatchStreamAdapter::new(schema, batches)),
            cancel,
        })
    }

    async fn execute_statement(&self, _pool: &dyn Pool, _query: Query) -> Result<u64> {
        Err(EngineError::unsupported(
            "statements without a result set",
            "jdbc-bridge",
        ))
    }

    async fn describe(&self, _pool: &dyn Pool, _schema: Option<&str>) -> Result<Vec<TableDesc>> {
        Err(EngineError::unsupported("describe", "jdbc-bridge"))
    }

    async fn ping(&self, pool: &dyn Pool) -> Result<()> {
        bridge_pool(pool).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_error_json_in_a_status_becomes_a_typed_error() {
        let status = tonic::Status::internal(
            r#"{"kind":"warehouse","error_type":"db","message":"relation x does not exist"}"#,
        );
        let err = flight_error(FlightError::Tonic(Box::new(status)));
        assert_eq!(err.kind, ErrorKind::Warehouse);
        assert_eq!(err.message, "relation x does not exist");
        let err = flight_error(FlightError::Tonic(Box::new(tonic::Status::unavailable(
            "no route",
        ))));
        assert_eq!(err.kind, ErrorKind::Connect);
    }
}
