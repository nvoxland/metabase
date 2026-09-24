//! A DataFusion catalog backed by a snapshot Metabase pushes. Tables resolve by their exact
//! names (identifier normalization is off), and scans are never executed locally: the whole
//! plan is unparsed and pushed to the warehouse.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, SchemaProvider, Session, TableProvider,
};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::api::{CatalogSnapshot, ColumnSnapshot, TableSnapshot};
use crate::error::{EngineError, ErrorKind, Result};

/// The catalog name every session sees. Warehouse schemas sit underneath it.
/// Field metadata key carrying the warehouse's own type name for a column.
pub const DB_TYPE_METADATA: &str = "metabase.db_type";

pub const CATALOG_NAME: &str = "warehouse";
pub const DEFAULT_SCHEMA: &str = "public";

/// A resolved snapshot: Arrow schemas built once, shared by every session on that version.
pub struct SnapshotCatalog {
    pub database_id: i64,
    pub catalog_version: String,
    pub connector: String,
    pub default_schema: String,
    schemas: HashMap<String, Arc<SnapshotSchema>>,
}

impl SnapshotCatalog {
    pub fn from_snapshot(snapshot: &CatalogSnapshot) -> Result<Arc<SnapshotCatalog>> {
        let default_schema = snapshot
            .default_schema
            .clone()
            .unwrap_or_else(|| DEFAULT_SCHEMA.to_string());
        let mut schemas: HashMap<String, HashMap<String, Arc<SnapshotTable>>> = HashMap::new();
        for table in &snapshot.tables {
            let schema_name = table
                .schema
                .clone()
                .unwrap_or_else(|| default_schema.clone());
            let provider = SnapshotTable::from_snapshot(&schema_name, table)?;
            schemas
                .entry(schema_name)
                .or_default()
                .insert(table.name.clone(), Arc::new(provider));
        }
        let schemas = schemas
            .into_iter()
            .map(|(name, tables)| (name.clone(), Arc::new(SnapshotSchema { name, tables })))
            .collect();
        Ok(Arc::new(SnapshotCatalog {
            database_id: snapshot.database_id,
            catalog_version: snapshot.catalog_version.clone(),
            connector: snapshot.connector.clone(),
            default_schema,
            schemas,
        }))
    }

    pub fn table_count(&self) -> usize {
        self.schemas.values().map(|s| s.tables.len()).sum()
    }
}

impl std::fmt::Debug for SnapshotCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotCatalog")
            .field("database_id", &self.database_id)
            .field("catalog_version", &self.catalog_version)
            .field("tables", &self.table_count())
            .finish()
    }
}

impl CatalogProviderList for SnapshotCatalog {
    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![CATALOG_NAME.to_string()]
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == CATALOG_NAME {
            Some(Arc::new(SnapshotCatalogProvider {
                schemas: self.schemas.clone(),
            }))
        } else {
            None
        }
    }
}

/// The single catalog, exposing the warehouse's schemas.
struct SnapshotCatalogProvider {
    schemas: HashMap<String, Arc<SnapshotSchema>>,
}

impl std::fmt::Debug for SnapshotCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotCatalogProvider")
            .field("schemas", &self.schemas.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CatalogProvider for SnapshotCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

pub struct SnapshotSchema {
    name: String,
    tables: HashMap<String, Arc<SnapshotTable>>,
}

impl std::fmt::Debug for SnapshotSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotSchema")
            .field("name", &self.name)
            .field("tables", &self.tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for SnapshotSchema {
    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .tables
            .get(name)
            .map(|t| Arc::clone(t) as Arc<dyn TableProvider>))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

/// One warehouse table. Carries the remote name so the unparser emits it as-is.
pub struct SnapshotTable {
    pub schema_name: String,
    pub table_name: String,
    schema: SchemaRef,
    pub db_types: Vec<Option<String>>,
}

impl std::fmt::Debug for SnapshotTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotTable")
            .field("schema_name", &self.schema_name)
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl SnapshotTable {
    fn from_snapshot(schema_name: &str, table: &TableSnapshot) -> Result<SnapshotTable> {
        let mut fields = Vec::with_capacity(table.columns.len());
        let mut db_types = Vec::with_capacity(table.columns.len());
        for column in &table.columns {
            fields.push(column_to_field(column).map_err(|e| {
                EngineError::new(
                    ErrorKind::InvalidRequest,
                    format!(
                        "table {}.{} column {}: {}",
                        schema_name, table.name, column.name, e.message
                    ),
                )
            })?);
            db_types.push(column.db_type.clone());
        }
        Ok(SnapshotTable {
            schema_name: schema_name.to_string(),
            table_name: table.name.clone(),
            schema: Arc::new(Schema::new(fields)),
            db_types,
        })
    }
}

fn column_to_field(column: &ColumnSnapshot) -> Result<Field> {
    let data_type = parse_arrow_type(&column.arrow_type)?;
    let mut field = Field::new(&column.name, data_type, column.nullable);
    if let Some(db_type) = &column.db_type {
        // Rides along to the result schema for column references, so the JVM can restore types
        // Arrow has no equivalent for (uuid, timetz) from the warehouse type name.
        field.set_metadata(std::collections::HashMap::from([(
            DB_TYPE_METADATA.to_string(),
            db_type.clone(),
        )]));
    }
    Ok(field)
}

/// Parse an Arrow type from its `Display` form (`Int64`, `Utf8`, `Timestamp(Microsecond, None)`).
pub fn parse_arrow_type(s: &str) -> Result<DataType> {
    DataType::from_str(s).map_err(|e| {
        EngineError::new(
            ErrorKind::InvalidRequest,
            format!("unknown Arrow type {s:?}: {e}"),
        )
    })
}

#[async_trait]
impl TableProvider for SnapshotTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // v1 never executes warehouse tables locally: the whole plan is pushed down.
        Err(DataFusionError::NotImplemented(format!(
            "local scan of warehouse table {}.{}; whole-query pushdown is the only execution path",
            self.schema_name, self.table_name
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> CatalogSnapshot {
        serde_json::from_value(serde_json::json!({
            "database_id": 42,
            "catalog_version": "v1",
            "connector": "postgres",
            "tables": [
                {"schema": "public", "name": "Orders", "columns": [
                    {"name": "ID", "type": "Int64", "nullable": false},
                    {"name": "Total", "type": "Float64"},
                    {"name": "Created At", "type": "Timestamp(Microsecond, Some(\"UTC\"))"}
                ]},
                {"schema": "sales", "name": "regions", "columns": [
                    {"name": "id", "type": "Int32", "nullable": false},
                    {"name": "name", "type": "Utf8"}
                ]}
            ]
        }))
        .unwrap()
    }

    #[test]
    fn snapshot_builds_schemas_and_tables_with_exact_names() {
        let catalog = SnapshotCatalog::from_snapshot(&snapshot()).unwrap();
        assert_eq!(catalog.table_count(), 2);
        assert_eq!(catalog.default_schema, "public");
        let provider = catalog.catalog(CATALOG_NAME).unwrap();
        let public = provider.schema("public").unwrap();
        assert!(public.table_exist("Orders"));
        assert!(!public.table_exist("orders"));
        assert!(provider.schema("missing").is_none());
    }

    #[test]
    fn arrow_types_parse_from_display_form() {
        assert_eq!(parse_arrow_type("Int64").unwrap(), DataType::Int64);
        assert_eq!(
            parse_arrow_type("Timestamp(Microsecond, Some(\"UTC\"))").unwrap(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        let err = parse_arrow_type("Bogus").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidRequest);
    }

    #[tokio::test]
    async fn scans_are_refused() {
        let catalog = SnapshotCatalog::from_snapshot(&snapshot()).unwrap();
        let provider = catalog.catalog(CATALOG_NAME).unwrap();
        let table = provider
            .schema("public")
            .unwrap()
            .table("Orders")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(table.schema().fields().len(), 3);
        let state = datafusion::execution::SessionStateBuilder::new().build();
        let err = table.scan(&state, None, &[], None).await.unwrap_err();
        assert!(matches!(err, DataFusionError::NotImplemented(_)));
    }
}
