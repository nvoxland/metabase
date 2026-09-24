//! Warehouse dialects for the DataFusion unparser, plus the per-dialect capability policy
//! that decides which plan nodes and functions a warehouse can run.
//!
//! The unparser emits most DataFusion function names verbatim, so a dialect's function
//! allowlist is "DataFusion functions that exist with compatible semantics in the warehouse".
//! Anything outside the list is rejected before unparsing rather than sent to the warehouse
//! to fail (or worse, to run with different semantics).

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::sql::unparser::dialect::{Dialect, PostgreSqlDialect};

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
                dialect: Arc::new(PostgreSqlDialect {}),
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
