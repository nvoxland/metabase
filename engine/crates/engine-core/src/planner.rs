//! Parse, analyze, reject what the warehouse can't run, unparse. DataFusion's optimizer never
//! runs on the pushed plan: the warehouse has its own optimizer, and optimizer rewrites produce
//! shapes the unparser handles badly (see apache/datafusion#21066).

use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::context::SessionState;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan, WindowFunctionDefinition};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use datafusion::sql::unparser::Unparser;
use engine_dialects::{DialectPolicy, Warehouse};

use crate::api::{FieldInfo, ParamValue, PlanResult, SessionContext};
use crate::catalog::{SnapshotCatalog, CATALOG_NAME};
use crate::error::{EngineError, ErrorKind, Result};
use crate::policy::PolicyInjectionRule;

/// Everything needed to plan for one session.
pub struct Planner {
    state: SessionState,
    policy: DialectPolicy,
    context: SessionContext,
}

impl Planner {
    pub fn new(catalog: Arc<SnapshotCatalog>, context: SessionContext) -> Result<Planner> {
        let warehouse = Warehouse::parse(&catalog.connector).ok_or_else(|| {
            EngineError::new(
                ErrorKind::CatalogMissing,
                format!("no dialect for connector {:?}", catalog.connector),
            )
        })?;
        let policy = DialectPolicy::for_warehouse(warehouse);
        let state = build_state(catalog, &context);
        Ok(Planner {
            state,
            policy,
            context,
        })
    }

    pub fn warehouse(&self) -> Warehouse {
        self.policy.warehouse()
    }

    /// Parse and analyze `sql` into a logical plan, without optimizing it.
    pub async fn analyze(&self, sql: &str) -> Result<LogicalPlan> {
        let dialect = &self.state.config_options().sql_parser.dialect;
        let statement = self.state.sql_to_statement(sql, dialect)?;
        ensure_query(&statement)?;
        let plan = self.state.statement_to_plan(statement).await?;
        let plan = self.state.analyzer().execute_and_check(
            plan,
            self.state.config_options(),
            |_, _| {},
        )?;
        Ok(plan)
    }

    /// Analyze, reject unsupported constructs, and unparse to the warehouse dialect, keeping the
    /// plan's Arrow schema for execution.
    pub async fn plan(&self, sql: &str) -> Result<Planned> {
        let plan = self.analyze(sql).await?;
        self.reject_unsupported(&plan)?;
        let statement = Unparser::new(self.policy.dialect()).plan_to_sql(&plan)?;
        let mut warehouse_sql = statement.to_string();
        if let Some(remark) = &self.context.remark {
            warehouse_sql = format!("-- {}\n{}", single_line(remark), warehouse_sql);
        }
        Ok(Planned {
            warehouse_sql,
            warehouse: self.policy.warehouse(),
            schema: Arc::new(plan.schema().as_arrow().clone()),
        })
    }

    /// [`Planner::plan`] in the JSON-friendly shape.
    pub async fn plan_and_unparse(&self, sql: &str, _params: &[ParamValue]) -> Result<PlanResult> {
        let planned = self.plan(sql).await?;
        Ok(PlanResult {
            warehouse_sql: planned.warehouse_sql,
            warehouse: planned.warehouse.name().to_string(),
            schema: planned
                .schema
                .fields()
                .iter()
                .map(|f| FieldInfo {
                    name: f.name().clone(),
                    arrow_type: f.data_type().to_string(),
                    nullable: f.is_nullable(),
                })
                .collect(),
            warnings: vec![],
        })
    }

    /// Walk the plan and refuse anything the target warehouse can't express.
    fn reject_unsupported(&self, plan: &LogicalPlan) -> Result<()> {
        let warehouse = self.policy.warehouse().name();
        let mut rejected: Option<String> = None;
        plan.apply_with_subqueries(|node| {
            let construct = match node {
                LogicalPlan::Unnest(_) if !self.policy.supports_unnest() => Some("UNNEST"),
                LogicalPlan::RecursiveQuery(_) if !self.policy.supports_recursive_cte() => {
                    Some("WITH RECURSIVE")
                }
                LogicalPlan::Extension(_) => Some("extension plan node"),
                LogicalPlan::Ddl(_) | LogicalPlan::Dml(_) | LogicalPlan::Copy(_) => {
                    Some("data modification")
                }
                LogicalPlan::Statement(_) => Some("non-query statement"),
                LogicalPlan::Explain(_) | LogicalPlan::Analyze(_) => Some("EXPLAIN"),
                LogicalPlan::DescribeTable(_) => Some("DESCRIBE"),
                _ => None,
            };
            if let Some(c) = construct {
                rejected = Some(c.to_string());
                return Ok(TreeNodeRecursion::Stop);
            }
            node.apply_expressions(|expr| {
                expr.apply(|e| {
                    let name = match e {
                        Expr::ScalarFunction(f) => Some(f.name().to_string()),
                        Expr::AggregateFunction(a) => Some(a.func.name().to_string()),
                        Expr::WindowFunction(w) => Some(match &w.fun {
                            WindowFunctionDefinition::AggregateUDF(u) => u.name().to_string(),
                            WindowFunctionDefinition::WindowUDF(u) => u.name().to_string(),
                        }),
                        _ => None,
                    };
                    if let Some(name) = name {
                        if !self.policy.supports_function(&name) {
                            rejected = Some(format!("function {name}"));
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })?;
        match rejected {
            Some(construct) => Err(EngineError::unsupported(construct, warehouse)),
            None => Ok(()),
        }
    }
}

fn ensure_query(statement: &DFStatement) -> Result<()> {
    let ok = match statement {
        DFStatement::Statement(s) => matches!(**s, SqlStatement::Query(_)),
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(EngineError::unsupported("non-query statement", "engine"))
    }
}

fn single_line(s: &str) -> String {
    s.replace(['\r', '\n'], " ")
}

/// What execution needs from planning.
pub struct Planned {
    pub warehouse_sql: String,
    pub warehouse: Warehouse,
    pub schema: arrow::datatypes::SchemaRef,
}

fn build_state(catalog: Arc<SnapshotCatalog>, context: &SessionContext) -> SessionState {
    let mut config = datafusion::prelude::SessionConfig::new();
    {
        let options = config.options_mut();
        options.sql_parser.enable_ident_normalization = false;
        options.sql_parser.dialect = datafusion::common::config::Dialect::PostgreSQL;
        options.sql_parser.map_string_types_to_utf8view = false;
        options.catalog.create_default_catalog_and_schema = false;
        options.catalog.default_catalog = CATALOG_NAME.to_string();
        options.catalog.default_schema = catalog.default_schema.clone();
        options.execution.batch_size =
            datafusion::common::config::ConfigNonZeroUsize::try_new(8192).expect("non-zero");
        if let Some(tz) = &context.report_timezone {
            options.execution.time_zone = Some(tz.clone());
        }
    }
    SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_catalog_list(catalog)
        .with_analyzer_rule(Arc::new(PolicyInjectionRule::new(context.clone())))
        .with_optimizer_rules(vec![])
        .with_physical_optimizer_rules(vec![])
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::CatalogSnapshot;

    fn planner() -> Planner {
        let snapshot: CatalogSnapshot = serde_json::from_value(serde_json::json!({
            "database_id": 42,
            "catalog_version": "v1",
            "connector": "postgres",
            "tables": [
                {"schema": "public", "name": "Orders", "columns": [
                    {"name": "ID", "type": "Int64", "nullable": false},
                    {"name": "Total", "type": "Float64"},
                    {"name": "Created At", "type": "Timestamp(Microsecond, Some(\"UTC\"))"},
                    {"name": "Product ID", "type": "Int64"}
                ]},
                {"schema": "public", "name": "Products", "columns": [
                    {"name": "ID", "type": "Int64", "nullable": false},
                    {"name": "Title", "type": "Utf8"}
                ]}
            ]
        }))
        .unwrap();
        let catalog = SnapshotCatalog::from_snapshot(&snapshot).unwrap();
        Planner::new(
            catalog,
            SessionContext {
                database_id: 42,
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn simple_select_unparses_to_postgres_with_placeholder_and_schema() {
        let result = planner()
            .plan_and_unparse(
                r#"SELECT "ID", "Total" FROM "public"."Orders" WHERE "Total" > $1 ORDER BY "ID" LIMIT 10"#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(result.warehouse, "postgres");
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT "Orders"."ID", "Orders"."Total" FROM "public"."Orders" WHERE ("Orders"."Total" > $1) ORDER BY "Orders"."ID" ASC NULLS LAST LIMIT 10"#
        );
        assert_eq!(
            result.schema,
            vec![
                FieldInfo {
                    name: "ID".into(),
                    arrow_type: "Int64".into(),
                    nullable: false
                },
                FieldInfo {
                    name: "Total".into(),
                    arrow_type: "Float64".into(),
                    nullable: true
                },
            ]
        );
    }

    #[tokio::test]
    async fn aggregation_with_join_and_date_trunc_unparses() {
        let result = planner()
            .plan_and_unparse(
                r#"SELECT date_trunc('month', o."Created At") AS "month", p."Title", count(*) AS "count"
                   FROM "public"."Orders" o LEFT JOIN "public"."Products" p ON o."Product ID" = p."ID"
                   GROUP BY date_trunc('month', o."Created At"), p."Title"
                   ORDER BY "month" ASC"#,
                &[],
            )
            .await
            .unwrap();
        assert!(
            result.warehouse_sql.contains("date_trunc('month'"),
            "{}",
            result.warehouse_sql
        );
        assert!(
            result.warehouse_sql.contains("LEFT OUTER JOIN"),
            "{}",
            result.warehouse_sql
        );
        assert!(
            result.warehouse_sql.contains("GROUP BY"),
            "{}",
            result.warehouse_sql
        );
        assert_eq!(result.schema.len(), 3);
        assert_eq!(result.schema[2].name, "count");
        assert_eq!(result.schema[2].arrow_type, "Int64");
    }

    #[tokio::test]
    async fn array_subscript_round_trips_for_regex_capture_groups() {
        let result = planner()
            .plan_and_unparse(
                r#"SELECT regexp_match("Title", '^(\w+)')[1] AS "m" FROM "public"."Products" LIMIT 2"#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT regexp_match("Products"."Title", '^(\w+)')[1] AS "m" FROM "public"."Products" LIMIT 2"#
        );
    }

    #[tokio::test]
    async fn substr_positions_are_cast_to_integer_for_postgres() {
        let result = planner()
            .plan_and_unparse(
                r#"SELECT substr("Title", 1, 3) AS "s" FROM "public"."Products""#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT substr("Products"."Title", CAST(1 AS INTEGER), CAST(3 AS INTEGER)) AS "s" FROM "public"."Products""#
        );
    }

    #[tokio::test]
    async fn remark_is_prepended_as_a_single_line_comment() {
        let snapshot: CatalogSnapshot = serde_json::from_value(serde_json::json!({
            "database_id": 1, "catalog_version": "v1", "connector": "postgres",
            "tables": [{"name": "t", "columns": [{"name": "x", "type": "Int64"}]}]
        }))
        .unwrap();
        let catalog = SnapshotCatalog::from_snapshot(&snapshot).unwrap();
        let planner = Planner::new(
            catalog,
            SessionContext {
                database_id: 1,
                remark: Some("Metabase:: userID: 1\nqueryHash: abc".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let result = planner
            .plan_and_unparse(r#"SELECT "x" FROM "t""#, &[])
            .await
            .unwrap();
        assert!(result
            .warehouse_sql
            .starts_with("-- Metabase:: userID: 1 queryHash: abc\nSELECT"));
    }

    #[tokio::test]
    async fn unknown_table_is_a_plan_error() {
        let err = planner()
            .plan_and_unparse(r#"SELECT * FROM "public"."Nope""#, &[])
            .await
            .unwrap_err();
        assert_eq!(err.error_type, "invalid-query");
        assert!(err.message.contains("Nope"), "{}", err.message);
    }

    #[tokio::test]
    async fn case_matters_because_normalization_is_off() {
        let err = planner()
            .plan_and_unparse(r#"SELECT id FROM "public"."Orders""#, &[])
            .await
            .unwrap_err();
        assert_eq!(err.error_type, "invalid-query");
    }

    #[tokio::test]
    async fn datafusion_only_functions_are_rejected_by_name() {
        let err = planner()
            .plan_and_unparse(
                r#"SELECT arrow_cast("Total", 'Utf8') FROM "public"."Orders""#,
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert_eq!(err.construct.as_deref(), Some("function arrow_cast"));
        assert_eq!(err.warehouse.as_deref(), Some("postgres"));
    }

    #[tokio::test]
    async fn dml_and_ddl_are_rejected() {
        for sql in [
            r#"DELETE FROM "public"."Orders""#,
            r#"CREATE TABLE t (x INT)"#,
            r#"SET datafusion.execution.batch_size = 1"#,
            r#"EXPLAIN SELECT 1"#,
        ] {
            let err = planner().plan_and_unparse(sql, &[]).await.unwrap_err();
            assert_eq!(err.kind, ErrorKind::Unsupported, "{sql}");
        }
    }

    #[tokio::test]
    async fn syntax_errors_are_invalid_query() {
        let err = planner()
            .plan_and_unparse("SELEC 1", &[])
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.error_type, "invalid-query");
    }
}
