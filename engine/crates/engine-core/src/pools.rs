//! Connection pools, one per (database, role, connection details). Details are hashed so a change
//! in credentials or host produces a new pool and the old one drains.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::api::DatabaseRegistration;
use crate::connector::{Connector, Pool, PoolStats};
use crate::error::{EngineError, ErrorKind, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub database_id: i64,
    pub role: Option<String>,
    pub details_hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PoolStatsEntry {
    pub database_id: i64,
    pub role: Option<String>,
    pub details_hash: String,
    pub connector: &'static str,
    #[serde(flatten)]
    pub stats: PoolStats,
}

struct Entry {
    connector: Arc<dyn Connector>,
    pool: Arc<dyn Pool>,
}

pub struct PoolRegistry {
    connectors: HashMap<&'static str, Arc<dyn Connector>>,
    pools: DashMap<PoolKey, Arc<Entry>>,
    /// Serializes pool creation so two concurrent first queries don't both open a pool.
    create_lock: Mutex<()>,
}

/// Rewrite a JSON value with object keys sorted, so the same details always hash the same.
fn canonical(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: std::collections::BTreeMap<&String, serde_json::Value> =
                map.iter().map(|(k, v)| (k, canonical(v))).collect();
            serde_json::Value::Object(sorted.into_iter().map(|(k, v)| (k.clone(), v)).collect())
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonical).collect())
        }
        other => other.clone(),
    }
}

pub fn details_hash(details: &serde_json::Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical(details).to_string().as_bytes());
    hex::encode(&hasher.finalize()[..8])
}

impl PoolRegistry {
    pub fn new(connectors: Vec<Arc<dyn Connector>>) -> PoolRegistry {
        PoolRegistry {
            connectors: connectors.into_iter().map(|c| (c.kind(), c)).collect(),
            pools: DashMap::new(),
            create_lock: Mutex::new(()),
        }
    }

    pub fn connector(&self, kind: &str) -> Result<Arc<dyn Connector>> {
        self.connectors.get(kind).cloned().ok_or_else(|| {
            EngineError::new(
                ErrorKind::CatalogMissing,
                format!("no connector for {kind:?} in this engine build"),
            )
        })
    }

    pub fn has_connector(&self, kind: &str) -> bool {
        self.connectors.contains_key(kind)
    }

    /// The pool for a registration and role, created on first use.
    pub async fn pool_for(
        &self,
        registration: &DatabaseRegistration,
        role: Option<&str>,
    ) -> Result<(Arc<dyn Connector>, Arc<dyn Pool>)> {
        let key = PoolKey {
            database_id: registration.database_id,
            role: role.map(str::to_string),
            details_hash: details_hash(&registration.details),
        };
        if let Some(entry) = self.pools.get(&key) {
            return Ok((Arc::clone(&entry.connector), Arc::clone(&entry.pool)));
        }
        let _guard = self.create_lock.lock().await;
        if let Some(entry) = self.pools.get(&key) {
            return Ok((Arc::clone(&entry.connector), Arc::clone(&entry.pool)));
        }
        let connector = self.connector(&registration.connector)?;
        let pool = connector
            .connect(&registration.details, &registration.pool)
            .await?;
        tracing::info!(database_id = key.database_id, role = ?key.role, connector = connector.kind(), "opened connection pool");
        self.pools.insert(
            key,
            Arc::new(Entry {
                connector: Arc::clone(&connector),
                pool: Arc::clone(&pool),
            }),
        );
        Ok((connector, pool))
    }

    /// Close every pool for a database.
    pub async fn invalidate(&self, database_id: i64) -> usize {
        let keys: Vec<PoolKey> = self
            .pools
            .iter()
            .filter(|e| e.key().database_id == database_id)
            .map(|e| e.key().clone())
            .collect();
        let mut closed = 0;
        for key in keys {
            if let Some((_, entry)) = self.pools.remove(&key) {
                entry.pool.close().await;
                closed += 1;
            }
        }
        closed
    }

    pub fn stats(&self, database_id: Option<i64>) -> Vec<PoolStatsEntry> {
        self.pools
            .iter()
            .filter(|e| database_id.is_none_or(|id| e.key().database_id == id))
            .map(|e| PoolStatsEntry {
                database_id: e.key().database_id,
                role: e.key().role.clone(),
                details_hash: e.key().details_hash.clone(),
                connector: e.value().connector.kind(),
                stats: e.value().pool.stats(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn details_hash_is_stable_and_order_independent() {
        let a = serde_json::json!({"host": "h", "port": 1, "password": "x"});
        let b = serde_json::json!({"password": "x", "port": 1, "host": "h"});
        let c = serde_json::json!({"host": "h", "port": 1, "password": "y"});
        assert_eq!(details_hash(&a), details_hash(&b));
        assert_ne!(details_hash(&a), details_hash(&c));
        assert_eq!(details_hash(&a).len(), 16);
    }

    #[tokio::test]
    async fn unknown_connector_is_a_configuration_error() {
        let registry = PoolRegistry::new(vec![]);
        let registration = DatabaseRegistration {
            database_id: 1,
            connector: "snowflake".into(),
            details: serde_json::json!({}),
            pool: Default::default(),
        };
        let err = registry
            .pool_for(&registration, None)
            .await
            .err()
            .expect("an error");
        assert_eq!(err.kind, ErrorKind::CatalogMissing);
        assert!(registry.stats(None).is_empty());
    }
}
