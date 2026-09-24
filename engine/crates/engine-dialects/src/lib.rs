//! Warehouse dialects for the DataFusion unparser, plus the per-dialect capability policy
//! that decides which plan nodes and functions a warehouse can run.
//!
//! The unparser emits most DataFusion function names verbatim, so a dialect's function
//! allowlist is "DataFusion functions that exist with compatible semantics in the warehouse".
//! Anything outside the list is rejected before unparsing rather than sent to the warehouse
//! to fail (or worse, to run with different semantics).

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::unparser::dialect::{
    DateFieldExtractStyle, Dialect, DistinctFromStyle, IntervalStyle, PostgreSqlDialect,
};
use datafusion::sql::unparser::Unparser;

/// A warehouse we can unparse to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Warehouse {
    Postgres,
}

impl Warehouse {
    pub fn parse(name: &str) -> Option<Warehouse> {
        match name {
            "postgres" | "postgresql" => Some(Warehouse::Postgres),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Warehouse::Postgres => "postgres",
        }
    }
}

/// The unparser dialect for a warehouse plus what that warehouse can express.
pub struct DialectPolicy {
    warehouse: Warehouse,
    dialect: Arc<dyn Dialect>,
    functions: HashSet<&'static str>,
}

impl DialectPolicy {
    pub fn for_warehouse(warehouse: Warehouse) -> DialectPolicy {
        match warehouse {
            Warehouse::Postgres => DialectPolicy {
                warehouse,
                dialect: Arc::new(MetabasePostgresDialect::default()),
                functions: POSTGRES_FUNCTIONS.iter().copied().collect(),
            },
        }
    }

    pub fn warehouse(&self) -> Warehouse {
        self.warehouse
    }

    pub fn dialect(&self) -> &dyn Dialect {
        self.dialect.as_ref()
    }

    /// Whether a DataFusion scalar, aggregate, or window function can be sent to this
    /// warehouse by name.
    pub fn supports_function(&self, name: &str) -> bool {
        self.functions.contains(name.to_ascii_lowercase().as_str())
    }

    pub fn supports_unnest(&self) -> bool {
        false
    }

    pub fn supports_recursive_cte(&self) -> bool {
        false
    }
}

/// DataFusion's Postgres dialect plus the rewrites Metabase's queries need. Each override exists
/// because DataFusion and Postgres spell or type something differently; keep them small and tested.
pub struct MetabasePostgresDialect {
    inner: PostgreSqlDialect,
}

impl Default for MetabasePostgresDialect {
    fn default() -> Self {
        MetabasePostgresDialect {
            inner: PostgreSqlDialect {},
        }
    }
}

impl MetabasePostgresDialect {
    /// DataFusion coerces `substr` positions to BIGINT; Postgres only has `substr(text, int, int)`.
    fn substr_with_int_positions(
        &self,
        unparser: &Unparser,
        args: &[Expr],
    ) -> DFResult<Option<ast::Expr>> {
        if args.len() < 2 || args.len() > 3 {
            return Ok(None);
        }
        let mut sql_args = Vec::with_capacity(args.len());
        for (i, arg) in args.iter().enumerate() {
            let expr = unparser.expr_to_sql(arg)?;
            let expr = if i == 0 {
                expr
            } else {
                ast::Expr::Cast {
                    kind: ast::CastKind::Cast,
                    expr: Box::new(expr),
                    data_type: ast::DataType::Integer(None),
                    format: None,
                    array: false,
                }
            };
            sql_args.push(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)));
        }
        Ok(Some(ast::Expr::Function(ast::Function {
            name: ast::ObjectName::from(vec![ast::Ident::new("substr")]),
            uses_odbc_syntax: false,
            parameters: ast::FunctionArguments::None,
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: sql_args,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })))
    }
}

impl Dialect for MetabasePostgresDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        self.inner.supports_nulls_first_in_sort()
    }

    fn use_array_keyword_for_array_literals(&self) -> bool {
        self.inner.use_array_keyword_for_array_literals()
    }

    fn supports_qualify(&self) -> bool {
        self.inner.supports_qualify()
    }

    fn requires_derived_table_alias(&self) -> bool {
        self.inner.requires_derived_table_alias()
    }

    fn supports_empty_select_list(&self) -> bool {
        self.inner.supports_empty_select_list()
    }

    fn interval_style(&self) -> IntervalStyle {
        self.inner.interval_style()
    }

    fn float64_ast_dtype(&self) -> ast::DataType {
        self.inner.float64_ast_dtype()
    }

    fn int8_cast_dtype(&self) -> ast::DataType {
        self.inner.int8_cast_dtype()
    }

    fn distinct_from_style(&self) -> DistinctFromStyle {
        self.inner.distinct_from_style()
    }

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        self.inner.date_field_extract_style()
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> DFResult<Option<ast::Expr>> {
        match func_name {
            "substr" | "substring" => self.substr_with_int_positions(unparser, args),
            _ => self
                .inner
                .scalar_function_to_sql_overrides(unparser, func_name, args),
        }
    }
}

/// DataFusion function names that unparse to valid Postgres with matching semantics.
/// Refined by the golden corpus and the federated sqllogictest harness as gaps appear.
const POSTGRES_FUNCTIONS: &[&str] = &[
    // math
    "abs",
    "ceil",
    "ceiling",
    "floor",
    "round",
    "trunc",
    "sqrt",
    "cbrt",
    "power",
    "pow",
    "exp",
    "ln",
    "log",
    "log10",
    "sign",
    "pi",
    "random",
    "mod",
    "gcd",
    "lcm",
    "factorial",
    "degrees",
    "radians",
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "atan2",
    "sinh",
    "cosh",
    "tanh",
    // null handling / conditionals
    "coalesce",
    "nullif",
    "greatest",
    "least",
    // strings
    "upper",
    "lower",
    "length",
    "char_length",
    "character_length",
    "octet_length",
    "bit_length",
    "substr",
    "substring",
    "trim",
    "ltrim",
    "rtrim",
    "btrim",
    "concat",
    "concat_ws",
    "replace",
    "repeat",
    "reverse",
    "left",
    "right",
    "lpad",
    "rpad",
    "position",
    "strpos",
    "split_part",
    "starts_with",
    "initcap",
    "ascii",
    "chr",
    "md5",
    "sha256",
    "translate",
    "overlay",
    "array_element",
    "regexp_match",
    "regexp_replace",
    "regexp_count",
    "to_hex",
    "encode",
    "decode",
    "uuid",
    // date/time
    "now",
    "current_date",
    "current_time",
    "current_timestamp",
    "date_trunc",
    "date_part",
    "extract",
    "to_char",
    "to_timestamp",
    "to_date",
    "make_date",
    "date_bin",
    // casting helpers the unparser renders as CAST
    "cast",
    // aggregates
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "var_pop",
    "var_samp",
    "variance",
    "array_agg",
    "string_agg",
    "bool_and",
    "bool_or",
    "bit_and",
    "bit_or",
    "bit_xor",
    "corr",
    "covar_pop",
    "covar_samp",
    "regr_slope",
    "regr_intercept",
    "regr_count",
    "regr_r2",
    "regr_avgx",
    "regr_avgy",
    "regr_sxx",
    "regr_syy",
    "regr_sxy",
    "percentile_cont",
    "percentile_disc",
    // window
    "row_number",
    "rank",
    "dense_rank",
    "lag",
    "lead",
    "ntile",
    "nth_value",
    "first_value",
    "last_value",
    "cume_dist",
    "percent_rank",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_policy_allows_common_functions_and_rejects_datafusion_only_ones() {
        let policy = DialectPolicy::for_warehouse(Warehouse::Postgres);
        assert!(policy.supports_function("date_trunc"));
        assert!(policy.supports_function("COUNT"));
        assert!(policy.supports_function("regexp_match"));
        assert!(!policy.supports_function("arrow_cast"));
        assert!(!policy.supports_function("regexp_like"));
        assert!(!policy.supports_function("approx_percentile_cont"));
    }

    #[test]
    fn warehouse_names_round_trip() {
        assert_eq!(Warehouse::parse("postgres"), Some(Warehouse::Postgres));
        assert_eq!(Warehouse::parse("postgresql"), Some(Warehouse::Postgres));
        assert_eq!(Warehouse::parse("snowflake"), None);
        assert_eq!(Warehouse::Postgres.name(), "postgres");
    }
}
