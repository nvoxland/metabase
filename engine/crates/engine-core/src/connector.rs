//! Warehouse connectors. A connector owns its connection pool and turns SQL into an Arrow
//! stream. Transport-agnostic: the same trait serves in-process and sidecar modes.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use engine_dialects::Warehouse;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::api::{ParamValue, PoolConfig};
use crate::error::Result;

/// Per-statement session settings, applied on checkout and undone on release.
#[derive(Debug, Clone, Default)]
pub struct SessionSettings {
    pub time_zone: Option<String>,
    pub statement_timeout_ms: Option<u64>,
    pub role: Option<String>,
    pub read_only: bool,
    pub application_name: String,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub sql: String,
    pub params: Vec<ParamValue>,
    pub settings: SessionSettings,
}

/// A running statement: the batches and the handle to stop it.
pub struct ExecuteHandle {
    pub stream: SendableRecordBatchStream,
    pub cancel: CancellationToken,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolStats {
    pub max_size: usize,
    pub size: usize,
    pub available: usize,
    pub waiting: usize,
}

#[async_trait]
pub trait Pool: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn stats(&self) -> PoolStats;
    async fn close(&self);
}

#[async_trait]
pub trait Connector: Send + Sync {
    fn kind(&self) -> &'static str;
    fn warehouse(&self) -> Warehouse;
    async fn connect(&self, details: &Value, config: &PoolConfig) -> Result<Arc<dyn Pool>>;
    async fn execute(&self, pool: &dyn Pool, query: Query) -> Result<ExecuteHandle>;
}
