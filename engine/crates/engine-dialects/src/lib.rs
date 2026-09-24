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

pub mod mysql;
pub mod udfs;

pub use mysql::MetabaseMySqlDialect;

/// A warehouse we can unparse to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Warehouse {
    Postgres,
    /// MySQL and MariaDB.
    MySql,
}

impl Warehouse {
    pub fn parse(name: &str) -> Option<Warehouse> {
        match name {
            "postgres" | "postgresql" => Some(Warehouse::Postgres),
            "mysql" | "mariadb" => Some(Warehouse::MySql),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Warehouse::Postgres => "postgres",
            Warehouse::MySql => "mysql",
        }
    }
}

/// The unparser dialect for a warehouse plus what that warehouse can express.
pub struct DialectPolicy {
    warehouse: Warehouse,
    dialect: Arc<dyn Dialect>,
    functions: HashSet<&'static str>,
    /// `NULLS FIRST` / `NULLS LAST` in `ORDER BY`. Without it the planner spells the ordering
    /// out with an `IS NULL` sort key.
    nulls_ordering: bool,
    /// `agg(x) FILTER (WHERE ...)`.
    aggregate_filter: bool,
}

impl DialectPolicy {
    pub fn for_warehouse(warehouse: Warehouse) -> DialectPolicy {
        match warehouse {
            Warehouse::Postgres => DialectPolicy {
                warehouse,
                dialect: Arc::new(MetabasePostgresDialect::default()),
                functions: POSTGRES_FUNCTIONS.iter().copied().collect(),
                nulls_ordering: true,
                aggregate_filter: true,
            },
            Warehouse::MySql => DialectPolicy {
                warehouse,
                dialect: Arc::new(MetabaseMySqlDialect::default()),
                functions: mysql::MYSQL_FUNCTIONS.iter().copied().collect(),
                nulls_ordering: false,
                aggregate_filter: false,
            },
        }
    }

    pub fn supports_nulls_ordering(&self) -> bool {
        self.nulls_ordering
    }

    pub fn supports_aggregate_filter(&self) -> bool {
        self.aggregate_filter
    }

    /// `OFFSET n` with no `LIMIT` (MySQL has no syntax for it).
    pub fn supports_offset_without_limit(&self) -> bool {
        self.warehouse != Warehouse::MySql
    }

    /// `GROUP BY GROUPING SETS / CUBE / ROLLUP (...)` (MySQL has only `WITH ROLLUP`).
    pub fn supports_grouping_sets(&self) -> bool {
        self.warehouse != Warehouse::MySql
    }

    /// Aggregate functions the warehouse knows under another name than DataFusion's, or whose
    /// DataFusion name means something else there: `(datafusion name, warehouse name)`.
    pub fn aggregate_renames(&self) -> &'static [(&'static str, &'static str)] {
        match self.warehouse {
            // Postgres has `var_samp`, not `var`
            Warehouse::Postgres => &[("var", "var_samp")],
            // MySQL's `STDDEV` is the population one
            Warehouse::MySql => &[("stddev", "stddev_samp"), ("var", "var_samp")],
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

impl MetabasePostgresDialect {
    /// DataFusion unparses `array_element(f(x), 1)` as `f(x)[1]`, which Postgres only accepts with
    /// the array expression in parentheses: `(f(x))[1]`.
    fn parenthesized_subscript(
        &self,
        unparser: &Unparser,
        args: &[Expr],
    ) -> DFResult<Option<ast::Expr>> {
        if args.len() != 2 {
            return Ok(None);
        }
        let array = unparser.expr_to_sql(&args[0])?;
        let index = unparser.expr_to_sql(&args[1])?;
        Ok(Some(ast::Expr::CompoundFieldAccess {
            root: Box::new(ast::Expr::Nested(Box::new(array))),
            access_chain: vec![ast::AccessExpr::Subscript(ast::Subscript::Index { index })],
        }))
    }
}

/// `mb_int_div(a, b)` as `(a / b)`, for dialects whose `/` already is integer division on integers.
pub(crate) fn plain_division(unparser: &Unparser, args: &[Expr]) -> DFResult<Option<ast::Expr>> {
    if args.len() != 2 {
        return Ok(None);
    }
    Ok(Some(ast::Expr::Nested(Box::new(ast::Expr::BinaryOp {
        left: Box::new(unparser.expr_to_sql(&args[0])?),
        op: ast::BinaryOperator::Divide,
        right: Box::new(unparser.expr_to_sql(&args[1])?),
    }))))
}

/// A function call in the unparser's AST.
pub(crate) fn call(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::Function(ast::Function {
        name: ast::ObjectName::from(vec![ast::Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// A single-quoted string literal in the unparser's AST.
pub(crate) fn string_literal(s: &str) -> ast::Expr {
    ast::Expr::Value(ast::Value::SingleQuotedString(s.to_string()).into())
}

/// The string literal `args[i]` holds, if it is one.
pub(crate) fn literal_str(args: &[Expr], i: usize) -> Option<&str> {
    match args.get(i) {
        Some(Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(s)), _)) => Some(s),
        _ => None,
    }
}

/// Translate a Chrono strftime pattern (what DataFusion's `to_timestamp` takes) into a Postgres
/// `to_timestamp` template. Returns None for a specifier with no Postgres equivalent.
pub fn chrono_to_postgres_format(chrono: &str) -> Option<String> {
    let mut out = String::with_capacity(chrono.len() * 2);
    let mut chars = chrono.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let spec = chars.next()?;
        out.push_str(match spec {
            'Y' => "YYYY",
            'y' => "YY",
            'm' => "MM",
            'd' => "DD",
            'e' => "DD",
            'H' => "HH24",
            'I' => "HH12",
            'M' => "MI",
            'S' => "SS",
            'j' => "DDD",
            'b' | 'h' => "Mon",
            'B' => "Month",
            'a' => "Dy",
            'A' => "Day",
            'p' => "AM",
            'z' => "TZHTZM",
            'f' => "US",
            '%' => "%",
            '.' => match chars.next()? {
                'f' => ".US",
                _ => return None,
            },
            ':' => match chars.next()? {
                'z' => "TZH:TZM",
                _ => return None,
            },
            _ => return None,
        });
    }
    Some(out)
}

impl MetabasePostgresDialect {
    /// `to_timestamp(text, chrono_format)` with the pattern rewritten for Postgres; one-argument
    /// `to_timestamp(epoch)` and patterns with no translation fall through to the default.
    fn to_timestamp_with_postgres_format(
        &self,
        unparser: &Unparser,
        args: &[Expr],
    ) -> DFResult<Option<ast::Expr>> {
        if args.len() != 2 {
            return Ok(None);
        }
        let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(format)), _) = &args[1] else {
            return Ok(None);
        };
        let Some(translated) = chrono_to_postgres_format(format) else {
            return Ok(None);
        };
        let value = unparser.expr_to_sql(&args[0])?;
        let format_literal = ast::Expr::Value(ast::Value::SingleQuotedString(translated).into());
        Ok(Some(ast::Expr::Function(ast::Function {
            name: ast::ObjectName::from(vec![ast::Ident::new("to_timestamp")]),
            uses_odbc_syntax: false,
            parameters: ast::FunctionArguments::None,
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(value)),
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(format_literal)),
                ],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })))
    }
}

impl MetabasePostgresDialect {
    /// `mb_cast(x, 'TYPE')` as `CAST(x AS <postgres type>)`.
    fn named_cast(&self, unparser: &Unparser, args: &[Expr]) -> DFResult<Option<ast::Expr>> {
        if args.len() != 2 {
            return Ok(None);
        }
        let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(type_name)), _) = &args[1]
        else {
            return Ok(None);
        };
        let data_type = match type_name.to_ascii_uppercase().as_str() {
            "TEXT" => ast::DataType::Text,
            "BIGINT" => ast::DataType::BigInt(None),
            "DOUBLE" => ast::DataType::DoublePrecision,
            "NUMERIC" => ast::DataType::Numeric(ast::ExactNumberInfo::None),
            "DATE" => ast::DataType::Date,
            "TIMESTAMP" => ast::DataType::Timestamp(None, ast::TimezoneInfo::None),
            "TIMESTAMPTZ" => ast::DataType::Timestamp(None, ast::TimezoneInfo::WithTimeZone),
            "TIME" => ast::DataType::Time(None, ast::TimezoneInfo::None),
            _ => return Ok(None),
        };
        Ok(Some(ast::Expr::Cast {
            kind: ast::CastKind::Cast,
            expr: Box::new(unparser.expr_to_sql(&args[0])?),
            data_type,
            format: None,
            array: false,
        }))
    }

    /// `mb_bytes_to_text(x)` as `convert_from(x, 'UTF8')`.
    fn convert_from_utf8(&self, unparser: &Unparser, args: &[Expr]) -> DFResult<Option<ast::Expr>> {
        if args.len() != 1 {
            return Ok(None);
        }
        let value = unparser.expr_to_sql(&args[0])?;
        let encoding = ast::Expr::Value(ast::Value::SingleQuotedString("UTF8".into()).into());
        Ok(Some(ast::Expr::Function(ast::Function {
            name: ast::ObjectName::from(vec![ast::Ident::new("convert_from")]),
            uses_odbc_syntax: false,
            parameters: ast::FunctionArguments::None,
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(value)),
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(encoding)),
                ],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })))
    }
}

impl MetabasePostgresDialect {
    /// `mb_at_time_zone(x, 'zone')` as `(x AT TIME ZONE 'zone')`, parenthesized so nesting reads
    /// unambiguously.
    /// `regexp_like(x, re)` is Postgres' `x ~ re`; the only flag Postgres spells as an operator is
    /// `i` (`~*`). Other flags have no operator form and are refused rather than sent as a
    /// function Postgres doesn't have.
    fn regexp_like_as_operator(
        &self,
        unparser: &Unparser,
        args: &[Expr],
    ) -> DFResult<Option<ast::Expr>> {
        let op = match args {
            [_, _] => ast::BinaryOperator::PGRegexMatch,
            [_, _, Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(flags)), _)] => {
                match flags.as_str() {
                    "" => ast::BinaryOperator::PGRegexMatch,
                    "i" => ast::BinaryOperator::PGRegexIMatch,
                    other => {
                        return Err(datafusion::error::DataFusionError::NotImplemented(format!(
                            "regexp_like flags {other:?} have no Postgres operator"
                        )))
                    }
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(ast::Expr::Nested(Box::new(ast::Expr::BinaryOp {
            left: Box::new(unparser.expr_to_sql(&args[0])?),
            op,
            right: Box::new(unparser.expr_to_sql(&args[1])?),
        }))))
    }

    fn at_time_zone(&self, unparser: &Unparser, args: &[Expr]) -> DFResult<Option<ast::Expr>> {
        if args.len() != 2 {
            return Ok(None);
        }
        let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(zone)), _) = &args[1] else {
            return Ok(None);
        };
        let timestamp = unparser.expr_to_sql(&args[0])?;
        Ok(Some(ast::Expr::Nested(Box::new(ast::Expr::AtTimeZone {
            timestamp: Box::new(timestamp),
            time_zone: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(zone.clone()).into(),
            )),
        }))))
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
            "array_element" => self.parenthesized_subscript(unparser, args),
            "to_timestamp" => self.to_timestamp_with_postgres_format(unparser, args),
            "regexp_like" => self.regexp_like_as_operator(unparser, args),
            udfs::MB_CAST => self.named_cast(unparser, args),
            udfs::MB_BYTES_TO_TEXT => self.convert_from_utf8(unparser, args),
            udfs::MB_AT_TIME_ZONE | udfs::MB_ZONED_TO_WALL | udfs::MB_WALL_TO_ZONED => {
                self.at_time_zone(unparser, args)
            }
            udfs::MB_INT_DIV => plain_division(unparser, args),
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
    "regexp_like",
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
    "grouping",
    udfs::MB_CAST,
    udfs::MB_BYTES_TO_TEXT,
    udfs::MB_AT_TIME_ZONE,
    udfs::MB_ZONED_TO_WALL,
    udfs::MB_WALL_TO_ZONED,
    udfs::MB_INT_DIV,
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
        assert!(policy.supports_function("regexp_like"));
        assert!(!policy.supports_function("regexp_instr"));
        assert!(!policy.supports_function("approx_percentile_cont"));
    }

    #[test]
    fn warehouse_names_round_trip() {
        assert_eq!(Warehouse::parse("postgres"), Some(Warehouse::Postgres));
        assert_eq!(Warehouse::parse("postgresql"), Some(Warehouse::Postgres));
        assert_eq!(Warehouse::parse("snowflake"), None);
        assert_eq!(Warehouse::parse("mysql"), Some(Warehouse::MySql));
        assert_eq!(Warehouse::parse("mariadb"), Some(Warehouse::MySql));
        assert_eq!(Warehouse::Postgres.name(), "postgres");
        assert_eq!(Warehouse::MySql.name(), "mysql");
    }
}

#[cfg(test)]
mod format_tests {
    use super::chrono_to_postgres_format;

    #[test]
    fn translates_common_specifiers() {
        assert_eq!(
            chrono_to_postgres_format("%Y%m%d%H%M%S").as_deref(),
            Some("YYYYMMDDHH24MISS")
        );
        assert_eq!(
            chrono_to_postgres_format("%Y-%m-%d %H:%M:%S%.f").as_deref(),
            Some("YYYY-MM-DD HH24:MI:SS.US")
        );
        assert_eq!(
            chrono_to_postgres_format("%d/%b/%Y %I:%M %p").as_deref(),
            Some("DD/Mon/YYYY HH12:MI AM")
        );
        assert_eq!(chrono_to_postgres_format("%Q"), None);
    }
}
