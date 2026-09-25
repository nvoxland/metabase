//! Parse, analyze, reject what the warehouse can't run, unparse. DataFusion's optimizer never
//! runs on the pushed plan: the warehouse has its own optimizer, and optimizer rewrites produce
//! shapes the unparser handles badly (see apache/datafusion#21066).

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, TableReference};
use datafusion::execution::context::SessionState;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::expr::{Cast, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{BinaryExpr, ExprSchemable, Operator, SortExpr};
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, WindowFunctionDefinition};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use datafusion::sql::unparser::Unparser;
use engine_dialects::udfs;
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
        let plan = qualify_table_scans(plan, &self.state.config_options().catalog.default_schema)?;
        let plan = resolve_engine_udfs(plan, &self.state)?;
        let plan = rename_aggregates(plan, self.policy.aggregate_renames())?;
        let plan = merge_nested_projections(plan)?;
        let plan = alias_columns_over_derived_sort(plan)?;
        let plan = if self.policy.supports_nulls_ordering() {
            plan
        } else {
            emulate_nulls_ordering(plan)?
        };
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
                LogicalPlan::Limit(l)
                    if l.skip.is_some()
                        && l.fetch.is_none()
                        && !self.policy.supports_offset_without_limit() =>
                {
                    Some("OFFSET without LIMIT")
                }
                _ => None,
            };
            if let Some(c) = construct {
                rejected = Some(c.to_string());
                return Ok(TreeNodeRecursion::Stop);
            }
            // sqlparser can't tell an identifier that contains a backslash before a quote from one
            // that is already escaped, and prints it unescaped; the SQL it would produce is not the
            // identifier we were given, so refuse it rather than send a string we can't vouch for.
            if let Some(name) = unprintable_identifier(node) {
                rejected = Some(format!("identifier {name:?} (backslash before a quote)"));
                return Ok(TreeNodeRecursion::Stop);
            }
            node.apply_expressions(|expr| {
                expr.apply(|e| {
                    if let Expr::AggregateFunction(a) = e {
                        if a.params.filter.is_some() && !self.policy.supports_aggregate_filter() {
                            rejected = Some("aggregate FILTER (WHERE ...)".to_string());
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                    if matches!(e, Expr::GroupingSet(_)) && !self.policy.supports_grouping_sets() {
                        rejected = Some("GROUPING SETS".to_string());
                        return Ok(TreeNodeRecursion::Stop);
                    }
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

/// The first identifier in this node's output schema, table reference, or alias that the unparser
/// would print incorrectly (see `reject_unsupported`).
fn unprintable_identifier(node: &LogicalPlan) -> Option<String> {
    fn bad(name: &str) -> bool {
        name.contains("\\\"") || name.contains("\\'") || name.contains("\\`")
    }
    if let LogicalPlan::SubqueryAlias(a) = node {
        if bad(a.alias.table()) {
            return Some(a.alias.table().to_string());
        }
    }
    for (qualifier, field) in node.schema().iter() {
        if bad(field.name()) {
            return Some(field.name().to_string());
        }
        if let Some(q) = qualifier {
            for part in [Some(q.table()), q.schema(), q.catalog()]
                .into_iter()
                .flatten()
            {
                if bad(part) {
                    return Some(part.to_string());
                }
            }
        }
    }
    None
}

/// Collapse a projection that only picks columns of the projection directly beneath it into one
/// projection. The unparser wraps the inner one as a derived table (`derived_projection`) but keeps
/// the outer columns qualified by the base table, which the warehouse then can't resolve. This is
/// the one optimizer-like rewrite the pushed plan gets; it never changes what the query computes.
fn is_fraction(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn is_integer(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// Say in the plan what the SQL surface leaves to the types: integer `/` becomes `mb_int_div`
/// (DataFusion truncates; MySQL's `/` yields a decimal), and `mb_at_time_zone` becomes
/// `mb_zoned_to_wall` or `mb_wall_to_zoned` from its argument's type (Postgres' `AT TIME ZONE`
/// flips on the type by itself; MySQL's `CONVERT_TZ` has to be told). Names are preserved as the
/// analyzer's own rewrites preserve them, so downstream references still resolve.
fn resolve_engine_udfs(plan: LogicalPlan, state: &SessionState) -> Result<LogicalPlan> {
    let int_div = state.udf(udfs::MB_INT_DIV)?;
    let trunc = state.udf(udfs::MB_TRUNC)?;
    let zoned_to_wall = state.udf(udfs::MB_ZONED_TO_WALL)?;
    let wall_to_zoned = state.udf(udfs::MB_WALL_TO_ZONED)?;
    Ok(plan
        .transform_down_with_subqueries(|node| {
            let mut schema = DFSchema::empty();
            for input in node.inputs() {
                schema.merge(input.schema());
            }
            let name_preserver = NamePreserver::new(&node);
            node.map_expressions(|expr| {
                let original = name_preserver.save(&expr);
                expr.transform_up(|e| {
                    Ok(match e {
                        Expr::BinaryExpr(BinaryExpr {
                            left,
                            op: Operator::Divide,
                            right,
                        }) if left.get_type(&schema).is_ok_and(|t| is_integer(&t))
                            && right.get_type(&schema).is_ok_and(|t| is_integer(&t)) =>
                        {
                            Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                                Arc::clone(&int_div),
                                vec![*left, *right],
                            )))
                        }
                        // a fraction cast to an integer: DataFusion truncates, the warehouses would round
                        Expr::Cast(Cast { expr, field })
                            if is_integer(field.data_type())
                                && expr.get_type(&schema).is_ok_and(|t| is_fraction(&t)) =>
                        {
                            Transformed::yes(Expr::Cast(Cast::new_from_field(
                                Box::new(Expr::ScalarFunction(ScalarFunction::new_udf(
                                    Arc::clone(&trunc),
                                    vec![*expr],
                                ))),
                                field,
                            )))
                        }
                        Expr::ScalarFunction(f)
                            if f.name() == udfs::MB_CAST
                                && f.args.len() == 2
                                && matches!(&f.args[1], Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(t)), _) if t.eq_ignore_ascii_case("BIGINT"))
                                && f.args[0].get_type(&schema).is_ok_and(|t| is_fraction(&t)) =>
                        {
                            let mut args = f.args;
                            let value = args.remove(0);
                            args.insert(
                                0,
                                Expr::ScalarFunction(ScalarFunction::new_udf(
                                    Arc::clone(&trunc),
                                    vec![value],
                                )),
                            );
                            Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                                f.func,
                                args,
                            )))
                        }
                        Expr::ScalarFunction(f)
                            if f.name() == udfs::MB_AT_TIME_ZONE && f.args.len() == 2 =>
                        {
                            match f.args[0].get_type(&schema) {
                                Ok(DataType::Timestamp(_, Some(_))) => {
                                    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                                        Arc::clone(&zoned_to_wall),
                                        f.args,
                                    )))
                                }
                                Ok(_) => Transformed::yes(Expr::ScalarFunction(
                                    ScalarFunction::new_udf(Arc::clone(&wall_to_zoned), f.args),
                                )),
                                Err(_) => Transformed::no(Expr::ScalarFunction(f)),
                            }
                        }
                        other => Transformed::no(other),
                    })
                })
                .map(|t| t.update_data(|e| original.restore(e)))
            })?
            .map_data(|p| p.recompute_schema())
        })?
        .data)
}

/// Call aggregate functions by the name the warehouse knows them under (see
/// [`DialectPolicy::aggregate_renames`]), keeping the plan's own names for the output columns.
fn rename_aggregates(
    plan: LogicalPlan,
    renames: &[(&'static str, &'static str)],
) -> Result<LogicalPlan> {
    if renames.is_empty() {
        return Ok(plan);
    }
    Ok(plan
        .transform_down_with_subqueries(|node| {
            let name_preserver = NamePreserver::new(&node);
            node.map_expressions(|expr| {
                let original = name_preserver.save(&expr);
                expr.transform_up(|e| {
                    Ok(match e {
                        Expr::AggregateFunction(mut a)
                            if renames.iter().any(|(from, _)| *from == a.func.name()) =>
                        {
                            let to = renames
                                .iter()
                                .find(|(from, _)| *from == a.func.name())
                                .map(|(_, to)| *to)
                                .unwrap_or_default();
                            a.func = udfs::renamed_aggregate(Arc::clone(&a.func), to);
                            Transformed::yes(Expr::AggregateFunction(a))
                        }
                        other => Transformed::no(other),
                    })
                })
                .map(|t| t.update_data(|e| original.restore(e)))
            })?
            .map_data(|p| p.recompute_schema())
        })?
        .data)
}

/// Spell `NULLS FIRST` / `NULLS LAST` out for a warehouse without the syntax: an `IS NULL` sort
/// key ahead of any sort whose null placement differs from MySQL's own (nulls first ascending,
/// last descending).
fn emulate_nulls_ordering(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(plan
        .transform_down_with_subqueries(|node| match node {
            LogicalPlan::Sort(mut sort) => {
                let mut exprs = Vec::with_capacity(sort.expr.len() * 2);
                let mut changed = false;
                for s in sort.expr {
                    let native_nulls_first = s.asc;
                    if s.nulls_first != native_nulls_first {
                        // MySQL takes an aggregate's alias as a sort key but not inside an
                        // expression, so the key names what the alias stands for
                        let key = resolve_sort_key(&s.expr, &sort.input);
                        exprs.push(SortExpr::new(key.is_null(), !s.nulls_first, false));
                        changed = true;
                    }
                    exprs.push(s);
                }
                sort.expr = exprs;
                if changed {
                    Ok(Transformed::yes(LogicalPlan::Sort(sort)))
                } else {
                    Ok(Transformed::no(LogicalPlan::Sort(sort)))
                }
            }
            other => Ok(Transformed::no(other)),
        })?
        .data)
}

/// The expression a sort key column stands for, looking through the projection (and the
/// aggregate under it) it sorts over; the column itself when nothing resolves.
fn resolve_sort_key(expr: &Expr, input: &LogicalPlan) -> Expr {
    let Expr::Column(column) = expr else {
        return expr.clone();
    };
    let mut resolved = expr.clone();
    let mut plan = input;
    if let LogicalPlan::Projection(projection) = plan {
        if let Some(e) = projection
            .expr
            .iter()
            .find(|e| e.schema_name().to_string() == column.name)
        {
            resolved = e.clone().unalias();
        }
        plan = projection.input.as_ref();
    }
    if let (Expr::Column(inner), LogicalPlan::Aggregate(aggregate)) = (&resolved, plan) {
        if let Some(e) = aggregate
            .aggr_expr
            .iter()
            .chain(aggregate.group_expr.iter())
            .find(|e| e.schema_name().to_string() == inner.name)
        {
            resolved = e.clone().unalias();
        }
    }
    resolved
}

/// Name every scanned table with the schema the catalog resolved it in. A bare name in the SQL
/// resolved against the catalog's default schema; sent bare to the warehouse it would resolve
/// against the connection's search path instead, which need not agree.
fn qualify_table_scans(plan: LogicalPlan, default_schema: &str) -> Result<LogicalPlan> {
    Ok(plan
        .transform_down_with_subqueries(|node| match node {
            LogicalPlan::TableScan(mut scan)
                if matches!(scan.table_name, TableReference::Bare { .. }) =>
            {
                scan.table_name = TableReference::partial(default_schema, scan.table_name.table());
                Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
            }
            other => Ok(Transformed::no(other)),
        })?
        .data)
}

fn merge_nested_projections(plan: LogicalPlan) -> Result<LogicalPlan> {
    let merged = plan.transform_up(|node| {
        let LogicalPlan::Projection(outer) = &node else {
            return Ok(Transformed::no(node));
        };
        let LogicalPlan::Projection(inner) = outer.input.as_ref() else {
            return Ok(Transformed::no(node));
        };
        let mut exprs = Vec::with_capacity(outer.expr.len());
        for expr in &outer.expr {
            let (column, out_name) = match expr {
                Expr::Column(c) => (c, c.name.clone()),
                Expr::Alias(a) => match a.expr.as_ref() {
                    Expr::Column(c) => (c, a.name.clone()),
                    _ => return Ok(Transformed::no(node)),
                },
                _ => return Ok(Transformed::no(node)),
            };
            let Some(index) = inner.schema.index_of_column(column).ok() else {
                return Ok(Transformed::no(node));
            };
            let source = inner.expr[index].clone();
            let same_name = match &source {
                Expr::Column(c) => c.name == out_name,
                Expr::Alias(a) => a.name == out_name,
                _ => false,
            };
            exprs.push(if same_name {
                source
            } else {
                source.unalias().alias(out_name)
            });
        }
        let projection = Projection::try_new(exprs, Arc::clone(&inner.input))?;
        Ok(Transformed::yes(LogicalPlan::Projection(projection)))
    })?;
    Ok(merged.data)
}

/// `Projection(Sort(Projection))` where the outer projection drops columns the sort needs (an
/// `ORDER BY` on something not selected). The unparser flattens only the simplest of these and
/// otherwise wraps the sort in a `derived_sort` subquery while leaving the outer columns
/// qualified by their base table, which the warehouse can't resolve from outside the subquery.
/// Giving the inner projection uniquely named copies of the columns the outer one wants, and
/// referring to those by name, makes the wrapped SQL valid. Sort keys keep referring to the
/// inner projection's own columns.
fn alias_columns_over_derived_sort(plan: LogicalPlan) -> Result<LogicalPlan> {
    let rewritten = plan.transform_up(|node| {
        let LogicalPlan::Projection(outer) = &node else {
            return Ok(Transformed::no(node));
        };
        let LogicalPlan::Sort(sort) = outer.input.as_ref() else {
            return Ok(Transformed::no(node));
        };
        let LogicalPlan::Projection(inner) = sort.input.as_ref() else {
            return Ok(Transformed::no(node));
        };
        // The unparser also wraps plain-column sorts when a join or aggregate sits below, so any
        // projection that drops inner columns gets the treatment; the result is valid either way.
        if outer.expr.len() >= inner.expr.len() {
            return Ok(Transformed::no(node));
        }
        let mut inner_exprs = inner.expr.clone();
        let mut outer_exprs = Vec::with_capacity(outer.expr.len());
        for (i, expr) in outer.expr.iter().enumerate() {
            let (column, out_name) = match expr {
                Expr::Column(c) => (c, c.name.clone()),
                Expr::Alias(a) => match a.expr.as_ref() {
                    Expr::Column(c) => (c, a.name.clone()),
                    _ => return Ok(Transformed::no(node)),
                },
                _ => return Ok(Transformed::no(node)),
            };
            let Ok(index) = inner.schema.index_of_column(column) else {
                return Ok(Transformed::no(node));
            };
            let copy_name = format!("__mb_c{i}");
            inner_exprs.push(inner.expr[index].clone().unalias().alias(&copy_name));
            outer_exprs.push(Expr::Column(Column::new_unqualified(&copy_name)).alias(out_name));
        }
        let inner =
            LogicalPlan::Projection(Projection::try_new(inner_exprs, Arc::clone(&inner.input))?);
        let sorted = LogicalPlan::Sort(datafusion::logical_expr::Sort {
            expr: sort.expr.clone(),
            input: Arc::new(inner),
            fetch: sort.fetch,
        });
        let projection = Projection::try_new(outer_exprs, Arc::new(sorted))?;
        Ok(Transformed::yes(LogicalPlan::Projection(projection)))
    })?;
    Ok(rewritten.data)
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
    let mut state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_catalog_list(catalog)
        // DataFusion's defaults minus the rule that rewrites `grouping(...)` into bit operations over an
        // internal grouping-id column: the warehouse has `GROUPING(...)` itself, and the rewritten plan
        // can't be unparsed back to it.
        .with_analyzer_rules(
            datafusion::optimizer::Analyzer::default()
                .rules
                .into_iter()
                .filter(|rule| rule.name() != "resolve_grouping_function")
                .collect(),
        )
        .with_analyzer_rule(Arc::new(PolicyInjectionRule::new(context.clone())))
        .with_optimizer_rules(vec![])
        .with_physical_optimizer_rules(vec![])
        .build();
    for udf in engine_dialects::udfs::metabase_udfs() {
        state.register_udf(udf).expect("engine UDF registration");
    }
    state
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
                    {"name": "Title", "type": "Utf8", "db_type": "text"}
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

    fn mysql_planner() -> Planner {
        let snapshot: CatalogSnapshot = serde_json::from_value(serde_json::json!({
            "database_id": 43,
            "catalog_version": "v1",
            "connector": "mysql",
            "default_schema": "lakes",
            "tables": [
                {"name": "orders", "columns": [
                    {"name": "id", "type": "Int64", "nullable": false},
                    {"name": "quantity", "type": "Int64"},
                    {"name": "total", "type": "Float64"},
                    {"name": "note", "type": "Utf8"},
                    {"name": "created_at", "type": "Timestamp(Microsecond, Some(\"UTC\"))"},
                    {"name": "shipped_at", "type": "Timestamp(Microsecond, None)"}
                ]}
            ]
        }))
        .unwrap();
        let catalog = SnapshotCatalog::from_snapshot(&snapshot).unwrap();
        Planner::new(
            catalog,
            SessionContext {
                database_id: 43,
                ..Default::default()
            },
        )
        .unwrap()
    }

    async fn mysql_sql(sql: &str) -> String {
        mysql_planner()
            .plan_and_unparse(sql, &[])
            .await
            .unwrap()
            .warehouse_sql
    }

    #[tokio::test]
    async fn mysql_quotes_with_backticks_and_qualifies_with_the_database() {
        assert_eq!(
            mysql_sql(r#"SELECT "id", "total" FROM "orders" WHERE "total" > $1 ORDER BY "id" LIMIT 10"#).await,
            "SELECT `orders`.`id`, `orders`.`total` FROM `lakes`.`orders` WHERE (`orders`.`total` > $1) ORDER BY `orders`.`id` IS NULL ASC, `orders`.`id` ASC LIMIT 10"
        );
    }

    #[tokio::test]
    async fn mysql_spells_null_ordering_out_only_where_it_differs_from_its_own() {
        // ascending nulls-first and descending nulls-last are what MySQL does by itself; DataFusion's
        // own default for DESC is nulls first, as Postgres'
        assert_eq!(
            mysql_sql(r#"SELECT "id", "note", "total" FROM "orders" ORDER BY "note" ASC NULLS FIRST, "total" DESC NULLS LAST, "id" DESC"#).await,
            "SELECT `orders`.`id`, `orders`.`note`, `orders`.`total` FROM `lakes`.`orders` ORDER BY `orders`.`note` ASC, `orders`.`total` DESC, `orders`.`id` IS NULL DESC, `orders`.`id` DESC"
        );
    }

    #[tokio::test]
    async fn integer_division_is_div_on_mysql_and_slash_on_postgres() {
        assert_eq!(
            mysql_sql(r#"SELECT "quantity" / 3 AS q, "total" / 3 AS t FROM "orders""#).await,
            "SELECT (`orders`.`quantity` DIV 3) AS `q`, (`orders`.`total` / CAST(3 AS DOUBLE)) AS `t` FROM `lakes`.`orders`"
        );
        let postgres = planner()
            .plan_and_unparse(
                r#"SELECT "ID" / 3, "Total" / 3 FROM "public"."Orders""#,
                &[],
            )
            .await
            .unwrap();
        assert!(
            postgres
                .warehouse_sql
                .contains(r#"("Orders"."ID" / 3) AS "public.Orders.ID / Int64(3)""#),
            "{}",
            postgres.warehouse_sql
        );
        assert!(
            postgres
                .warehouse_sql
                .contains(r#"("Orders"."Total" / CAST(3 AS DOUBLE PRECISION))"#),
            "{}",
            postgres.warehouse_sql
        );
        assert_eq!(postgres.schema[0].arrow_type, "Int64");
    }

    #[tokio::test]
    async fn mysql_time_zone_conversion_goes_the_way_the_type_says() {
        assert_eq!(
            mysql_sql(r#"SELECT mb_at_time_zone("created_at", 'Asia/Tokyo') AS a, mb_at_time_zone("shipped_at", 'Asia/Tokyo') AS b FROM "orders""#).await,
            "SELECT CONVERT_TZ(`orders`.`created_at`, @@session.time_zone, 'Asia/Tokyo') AS `a`, CONVERT_TZ(`orders`.`shipped_at`, 'Asia/Tokyo', @@session.time_zone) AS `b` FROM `lakes`.`orders`"
        );
        let result = mysql_planner()
            .plan_and_unparse(r#"SELECT mb_at_time_zone("created_at", 'Asia/Tokyo') AS a, mb_at_time_zone("shipped_at", 'Asia/Tokyo') AS b FROM "orders""#, &[])
            .await
            .unwrap();
        assert_eq!(result.schema[0].arrow_type, "Timestamp(µs)");
        assert_eq!(result.schema[1].arrow_type, "Timestamp(µs, \"UTC\")");
    }

    #[tokio::test]
    async fn mysql_temporal_functions_render_their_own_spellings() {
        assert_eq!(
            mysql_sql(r#"SELECT date_trunc('month', "created_at") AS m, date_trunc('week', "created_at") AS w, date_trunc('year', "created_at") AS y FROM "orders""#).await,
            "SELECT CAST(DATE_FORMAT(`orders`.`created_at`, '%Y-%m-01') AS DATETIME) AS `m`, CAST((DATE(`orders`.`created_at`) - INTERVAL WEEKDAY(`orders`.`created_at`) DAY) AS DATETIME) AS `w`, CAST(MAKEDATE(YEAR(`orders`.`created_at`), 1) AS DATETIME) AS `y` FROM `lakes`.`orders`"
        );
        assert_eq!(
            mysql_sql(r#"SELECT extract(year FROM "created_at") AS y, extract(dow FROM "created_at") AS d, date_part('week', "created_at") AS w FROM "orders""#).await,
            "SELECT EXTRACT(YEAR FROM `orders`.`created_at`) AS `y`, (DAYOFWEEK(`orders`.`created_at`) - 1) AS `d`, WEEK(`orders`.`created_at`, 3) AS `w` FROM `lakes`.`orders`"
        );
        assert_eq!(
            mysql_sql(r#"SELECT to_timestamp("quantity") AS t, to_timestamp("note", '%Y-%m-%d %H:%M:%S') AS p FROM "orders""#).await,
            "SELECT CONVERT_TZ(FROM_UNIXTIME(CAST(`orders`.`quantity` AS DECIMAL(20,6))), @@session.time_zone, '+00:00') AS `t`, STR_TO_DATE(`orders`.`note`, '%Y-%m-%d %H:%i:%s') AS `p` FROM `lakes`.`orders`"
        );
    }

    #[tokio::test]
    async fn mysql_null_ordering_key_over_an_aggregate_alias_names_the_aggregate() {
        assert_eq!(
            mysql_sql(r#"SELECT "note", sum("quantity") AS q FROM "orders" GROUP BY "note" ORDER BY q ASC, "note" DESC NULLS LAST"#).await,
            "SELECT `orders`.`note`, sum(`orders`.`quantity`) AS `q` FROM `lakes`.`orders` GROUP BY `orders`.`note` ORDER BY sum(`orders`.`quantity`) IS NULL ASC, `q` ASC, `orders`.`note` DESC"
        );
    }

    #[tokio::test]
    async fn sample_statistics_are_called_what_each_warehouse_calls_them() {
        assert_eq!(
            mysql_sql(r#"SELECT stddev("total") AS s, var_samp("total") AS v, stddev_pop("total") AS p FROM "orders""#).await,
            "SELECT stddev_samp(`orders`.`total`) AS `s`, var_samp(`orders`.`total`) AS `v`, stddev_pop(`orders`.`total`) AS `p` FROM `lakes`.`orders`"
        );
        let postgres = planner()
            .plan_and_unparse(
                r#"SELECT stddev("Total"), var_samp("Total") FROM "public"."Orders""#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            postgres.warehouse_sql,
            r#"SELECT stddev("Orders"."Total"), var_samp("Orders"."Total") AS "var_samp(public.Orders.Total)" FROM "public"."Orders""#
        );
        assert_eq!(postgres.schema[1].name, "var_samp(public.Orders.Total)");
    }

    #[tokio::test]
    async fn a_fraction_cast_to_an_integer_truncates_on_every_warehouse() {
        assert_eq!(
            mysql_sql(r#"SELECT CAST("total" AS BIGINT) AS a, mb_cast("total", 'BIGINT') AS b, CAST("quantity" AS BIGINT) AS c, CAST("note" AS BIGINT) AS d FROM "orders""#).await,
            "SELECT CAST(TRUNCATE(`orders`.`total`, 0) AS SIGNED) AS `a`, CAST(TRUNCATE(`orders`.`total`, 0) AS SIGNED) AS `b`, CAST(`orders`.`quantity` AS SIGNED) AS `c`, CAST(`orders`.`note` AS SIGNED) AS `d` FROM `lakes`.`orders`"
        );
        let postgres = planner()
            .plan_and_unparse(
                r#"SELECT CAST("Total" AS BIGINT) AS a FROM "public"."Orders""#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            postgres.warehouse_sql,
            r#"SELECT CAST(trunc("Orders"."Total") AS BIGINT) AS "a" FROM "public"."Orders""#
        );
    }

    #[tokio::test]
    async fn mysql_concat_skips_nulls_as_datafusion_does() {
        assert_eq!(
            mysql_sql(r#"SELECT concat("note", '/', "id") AS c FROM "orders""#).await,
            "SELECT CONCAT_WS('', `orders`.`note`, '/', CAST(`orders`.`id` AS CHAR)) AS `c` FROM `lakes`.`orders`"
        );
    }

    #[tokio::test]
    async fn mysql_string_functions_render_their_own_spellings() {
        assert_eq!(
            mysql_sql(r#"SELECT strpos("note", 'x') AS s, starts_with("note", 'ab') AS b, length("note") AS l, regexp_like("note", '^a') AS r, regexp_like("note", '^a', 'i') AS ri, regexp_match("note", '[0-9]+')[1] AS m, mb_cast("total", 'TEXT') AS c, mb_cast("note", 'BIGINT') AS i FROM "orders""#).await,
            "SELECT LOCATE('x', `orders`.`note`) AS `s`, (LEFT(`orders`.`note`, CHAR_LENGTH('ab')) = 'ab') AS `b`, CHAR_LENGTH(`orders`.`note`) AS `l`, (`orders`.`note` REGEXP '(?-i)^a') AS `r`, (`orders`.`note` REGEXP '(?i)^a') AS `ri`, REGEXP_SUBSTR(`orders`.`note`, '(?-i)[0-9]+') AS `m`, CAST(`orders`.`total` AS CHAR) AS `c`, CAST(`orders`.`note` AS SIGNED) AS `i` FROM `lakes`.`orders`"
        );
    }

    #[tokio::test]
    async fn mysql_refuses_what_it_cannot_say() {
        for (sql, construct) in [
            (
                r#"SELECT count(*) FILTER (WHERE "total" > 1) FROM "orders""#,
                "aggregate FILTER",
            ),
            (
                r#"SELECT "id" FROM "orders" OFFSET 2"#,
                "OFFSET without LIMIT",
            ),
            (
                r#"SELECT approx_percentile_cont("total", 0.5) FROM "orders""#,
                "function approx_percentile_cont",
            ),
            (
                r#"SELECT "note", count(*) FROM "orders" GROUP BY GROUPING SETS (("note"), ())"#,
                "GROUPING SETS",
            ),
        ] {
            let err = mysql_planner()
                .plan_and_unparse(sql, &[])
                .await
                .unwrap_err();
            assert_eq!(err.kind, ErrorKind::Unsupported, "{sql}");
            assert!(err.message.contains(construct), "{sql}: {}", err.message);
        }
    }

    #[tokio::test]
    async fn bare_table_names_reach_the_warehouse_qualified_with_the_default_schema() {
        // The catalog resolved the table in its default schema; the warehouse must query that same
        // table whatever its own search path says.
        let result = planner()
            .plan_and_unparse(
                r#"SELECT o."ID", p."Title" FROM "Orders" o JOIN "Products" p ON o."Product ID" = p."ID" WHERE o."ID" IN (SELECT "ID" FROM "Orders" WHERE "Total" > 1)"#,
                &[],
            )
            .await
            .unwrap();
        assert!(
            result
                .warehouse_sql
                .contains(r#"FROM "public"."Orders" AS "o" INNER JOIN "public"."Products" AS "p""#),
            "{}",
            result.warehouse_sql
        );
        assert!(
            result
                .warehouse_sql
                .contains(r#"FROM "public"."Orders" WHERE"#),
            "{}",
            result.warehouse_sql
        );
    }

    #[tokio::test]
    async fn regexp_like_renders_as_the_postgres_match_operators() {
        let result = planner()
            .plan_and_unparse(
                r#"SELECT regexp_like("Title", '^A') AS a, regexp_like("Title", '^a', 'i') AS b FROM "public"."Products""#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT ("Products"."Title" ~ '^A') AS "a", ("Products"."Title" ~* '^a') AS "b" FROM "public"."Products""#
        );
        let err = planner()
            .plan_and_unparse(
                r#"SELECT regexp_like("Title", 'a', 'x') FROM "public"."Products""#,
                &[],
            )
            .await
            .unwrap_err();
        assert!(
            err.message.contains("no Postgres operator"),
            "{}",
            err.message
        );
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
            r#"SELECT (regexp_match("Products"."Title", '^(\w+)'))[1] AS "m" FROM "public"."Products" LIMIT 2"#
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
    async fn identifiers_with_a_backslash_before_a_quote_are_rejected() {
        // `""` is a quote inside a quoted identifier; the backslash before it is what sqlparser mishandles
        let err = planner()
            .plan_and_unparse(r#"SELECT "Title" AS "a\""b" FROM "public"."Products""#, &[])
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("identifier"), "{}", err.message);
        // a quote on its own is fine
        let ok = planner()
            .plan_and_unparse(r#"SELECT "Title" AS "a""b" FROM "public"."Products""#, &[])
            .await
            .unwrap();
        assert!(
            ok.warehouse_sql.contains(r#"AS "a""b""#),
            "{}",
            ok.warehouse_sql
        );
    }

    #[tokio::test]
    async fn nested_projections_merge_instead_of_becoming_a_derived_table() {
        // a Metabase stage that selects from an expressions stage: the unparser alone emits
        // `"Products"."Title"` inside a `derived_projection` subquery, which Postgres can't resolve
        let result = planner()
            .plan_and_unparse(
                r#"SELECT "s"."Title" AS "t", "s"."one" FROM (SELECT "Title", "one" FROM (SELECT "Title", "ID", 1 AS "one" FROM "public"."Products" WHERE "ID" > 1)) AS "s""#,
                &[],
            )
            .await
            .unwrap();
        assert!(
            !result.warehouse_sql.contains("derived_projection"),
            "{}",
            result.warehouse_sql
        );
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT "s"."Title" AS "t", "s"."one" FROM (SELECT "Products"."Title", 1 AS "one" FROM "public"."Products" WHERE ("Products"."ID" > 1)) AS "s""#
        );
    }

    #[tokio::test]
    async fn to_timestamp_formats_are_translated_for_postgres() {
        let result = planner()
            .plan_and_unparse(
                r#"SELECT to_timestamp("Title", '%Y%m%d%H%M%S') AS "t" FROM "public"."Products""#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT to_timestamp("Products"."Title", 'YYYYMMDDHH24MISS') AS "t" FROM "public"."Products""#
        );
    }

    #[tokio::test]
    async fn mb_cast_names_the_cast_so_aggregates_over_it_do_not_collide() {
        // `max(x)` and `max(CAST(x AS TEXT))` share a schema name in DataFusion; the UDF gives the cast its own
        let result = planner()
            .plan_and_unparse(
                r#"SELECT max("Title") AS "m", max(mb_cast("Title", 'TEXT')) AS "t" FROM "public"."Products""#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT max("Products"."Title") AS "m", max(CAST("Products"."Title" AS TEXT)) AS "t" FROM "public"."Products""#
        );
        assert_eq!(result.schema[1].arrow_type, "Utf8");
        let time = planner()
            .plan_and_unparse(
                r#"SELECT mb_cast("Title", 'TIME') AS "tm" FROM "public"."Products""#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            time.warehouse_sql,
            r#"SELECT CAST("Products"."Title" AS TIME) AS "tm" FROM "public"."Products""#
        );
        assert_eq!(time.schema[0].arrow_type, "Time64(µs)");
    }

    #[tokio::test]
    async fn mb_bytes_to_text_unparses_as_convert_from() {
        let result = planner()
            .plan_and_unparse(
                r#"SELECT mb_bytes_to_text("Title") AS "s" FROM "public"."Products""#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT convert_from("Products"."Title", 'UTF8') AS "s" FROM "public"."Products""#
        );
    }

    #[tokio::test]
    async fn mb_at_time_zone_unparses_as_at_time_zone_and_flips_zonedness() {
        let result = planner()
            .plan_and_unparse(
                r#"SELECT mb_at_time_zone("Created At", 'Asia/Seoul') AS "wall", mb_at_time_zone(mb_at_time_zone("Created At", 'Asia/Seoul'), 'UTC') AS "back" FROM "public"."Orders""#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT ("Orders"."Created At" AT TIME ZONE 'Asia/Seoul') AS "wall", (("Orders"."Created At" AT TIME ZONE 'Asia/Seoul') AT TIME ZONE 'UTC') AS "back" FROM "public"."Orders""#
        );
        assert_eq!(result.schema[0].arrow_type, "Timestamp(µs)");
        assert_eq!(result.schema[1].arrow_type, "Timestamp(µs, \"UTC\")");
    }

    #[tokio::test]
    async fn warehouse_type_names_ride_along_as_field_metadata() {
        // the test catalog gives "Title" a db_type (see `planner()`); a bare column reference keeps it
        let plan = planner()
            .analyze(r#"SELECT "Title", upper("Title") AS "u" FROM "public"."Products""#)
            .await
            .unwrap();
        let fields = plan.schema().fields();
        assert_eq!(
            fields[0]
                .metadata()
                .get(crate::catalog::DB_TYPE_METADATA)
                .map(String::as_str),
            Some("text")
        );
        assert!(fields[1]
            .metadata()
            .get(crate::catalog::DB_TYPE_METADATA)
            .is_none());
    }

    #[tokio::test]
    async fn sorting_by_an_expression_over_a_dropped_column_unparses_to_valid_sql() {
        // the unparser wraps this in a `derived_sort` subquery; the outer columns must not be qualified by the base table
        let result = planner()
            .plan_and_unparse(
                r#"SELECT o."ID", p."Title" AS "title" FROM "public"."Orders" o LEFT JOIN "public"."Products" p ON o."Product ID" = p."ID" ORDER BY date_trunc('day', o."Created At") DESC LIMIT 2"#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.warehouse_sql,
            r#"SELECT "__mb_c0" AS "ID", "__mb_c1" AS "title" FROM (SELECT "o"."ID", "p"."Title" AS "title", "o"."Created At", "o"."ID" AS "__mb_c0", "p"."Title" AS "__mb_c1" FROM "public"."Orders" AS "o" LEFT OUTER JOIN "public"."Products" AS "p" ON ("o"."Product ID" = "p"."ID") ORDER BY date_trunc('day', "o"."Created At") DESC NULLS FIRST) AS "derived_sort" LIMIT 2"#
        );
        assert_eq!(
            result
                .schema
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            vec!["ID", "title"]
        );
        // a plain-column key over a join gets the same shape, and no base-table qualifier survives outside
        let plain = planner()
            .plan_and_unparse(
                r#"SELECT o."ID" AS "id", p."Title" AS "title" FROM "public"."Orders" o LEFT JOIN "public"."Products" p ON o."Product ID" = p."ID" ORDER BY o."Created At" DESC LIMIT 2"#,
                &[],
            )
            .await
            .unwrap();
        assert!(
            plain
                .warehouse_sql
                .starts_with(r#"SELECT "__mb_c0" AS "id", "__mb_c1" AS "title" FROM ("#),
            "{}",
            plain.warehouse_sql
        );
    }

    #[tokio::test]
    async fn grouping_sets_and_grouping_unparse_for_pivots() {
        let result = planner()
            .plan_and_unparse(
                r#"SELECT "Title", grouping("Title") AS "pivot-grouping", count(*) AS "count" FROM "public"."Products" GROUP BY GROUPING SETS (("Title"), ()) ORDER BY grouping("Title") ASC"#,
                &[],
            )
            .await
            .unwrap();
        assert!(
            result.warehouse_sql.contains("GROUPING SETS"),
            "{}",
            result.warehouse_sql
        );
        assert!(
            result
                .warehouse_sql
                .contains(r#"grouping("Products"."Title")"#),
            "{}",
            result.warehouse_sql
        );
        assert!(
            !result.warehouse_sql.contains("__grouping_id"),
            "{}",
            result.warehouse_sql
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
