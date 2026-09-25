//! MySQL and MariaDB: DataFusion's MySQL unparser dialect plus the spellings it lacks.
//!
//! The connector runs every session with `NO_BACKSLASH_ESCAPES` (so the unparser's standard
//! `'it''s'` literals mean what they say and a backslash is a backslash) and with the session
//! time zone set explicitly, which is what `@@session.time_zone` refers to below.

use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::unparser::dialect::{
    DateFieldExtractStyle, Dialect, DistinctFromStyle, IntervalStyle, MySqlDialect,
};
use datafusion::sql::unparser::Unparser;

use crate::{call, literal_str, string_literal, udfs};

pub struct MetabaseMySqlDialect {
    inner: MySqlDialect,
}

impl Default for MetabaseMySqlDialect {
    fn default() -> Self {
        MetabaseMySqlDialect {
            inner: MySqlDialect {},
        }
    }
}

fn not_impl(what: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::NotImplemented(format!("{what} on MySQL"))
}

fn cast(expr: ast::Expr, data_type: ast::DataType) -> ast::Expr {
    ast::Expr::Cast {
        kind: ast::CastKind::Cast,
        expr: Box::new(expr),
        data_type,
        format: None,
        array: false,
    }
}

fn datetime() -> ast::DataType {
    ast::DataType::Datetime(None)
}

fn number(n: i64) -> ast::Expr {
    ast::Expr::Value(ast::Value::Number(n.to_string(), false).into())
}

fn binary(left: ast::Expr, op: ast::BinaryOperator, right: ast::Expr) -> ast::Expr {
    ast::Expr::Nested(Box::new(ast::Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}

fn interval(value: ast::Expr, field: ast::DateTimeField) -> ast::Expr {
    ast::Expr::Interval(ast::Interval {
        value: Box::new(value),
        leading_field: Some(field),
        leading_precision: None,
        last_field: None,
        fractional_seconds_precision: None,
    })
}

fn extract(field: ast::DateTimeField, expr: ast::Expr) -> ast::Expr {
    ast::Expr::Extract {
        field,
        syntax: ast::ExtractSyntax::From,
        expr: Box::new(expr),
    }
}

/// The connection's own time zone, as set by the connector.
fn session_time_zone() -> ast::Expr {
    ast::Expr::Identifier(ast::Ident::new("@@session.time_zone"))
}

/// A regex pattern with its case sensitivity spelled out: MySQL's `REGEXP` is case-insensitive by
/// default, DataFusion's functions are not. Both engines' regex flavors honor an inline `(?i)`.
fn regex_pattern(unparser: &Unparser, args: &[Expr], flags_index: usize) -> DFResult<ast::Expr> {
    let prefix = match args.get(flags_index) {
        None => "(?-i)",
        Some(Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(flags)), _)) => {
            match flags.as_str() {
                "" => "(?-i)",
                "i" => "(?i)",
                other => return Err(not_impl(format!("regex flags {other:?}"))),
            }
        }
        Some(_) => return Err(not_impl("non-literal regex flags")),
    };
    Ok(match literal_str(args, 1) {
        Some(pattern) => string_literal(&format!("{prefix}{pattern}")),
        None => call(
            "CONCAT",
            vec![string_literal(prefix), unparser.expr_to_sql(&args[1])?],
        ),
    })
}

impl MetabaseMySqlDialect {
    fn date_trunc(&self, unparser: &Unparser, args: &[Expr]) -> DFResult<Option<ast::Expr>> {
        let (Some(unit), Some(arg)) = (literal_str(args, 0), args.get(1)) else {
            return Ok(None);
        };
        let x = unparser.expr_to_sql(arg)?;
        let format = |f: &str| call("DATE_FORMAT", vec![x.clone(), string_literal(f)]);
        let truncated = match unit.to_ascii_lowercase().as_str() {
            "second" => format("%Y-%m-%d %H:%i:%s"),
            "minute" => format("%Y-%m-%d %H:%i:00"),
            "hour" => format("%Y-%m-%d %H:00:00"),
            "day" => call("DATE", vec![x]),
            // ISO weeks start on Monday, and so does WEEKDAY() (Monday = 0)
            "week" => binary(
                call("DATE", vec![x.clone()]),
                ast::BinaryOperator::Minus,
                interval(call("WEEKDAY", vec![x]), ast::DateTimeField::Day),
            ),
            "month" => format("%Y-%m-01"),
            "quarter" => binary(
                call("MAKEDATE", vec![call("YEAR", vec![x.clone()]), number(1)]),
                ast::BinaryOperator::Plus,
                interval(
                    binary(
                        binary(
                            call("QUARTER", vec![x]),
                            ast::BinaryOperator::Minus,
                            number(1),
                        ),
                        ast::BinaryOperator::Multiply,
                        number(3),
                    ),
                    ast::DateTimeField::Month,
                ),
            ),
            "year" => call("MAKEDATE", vec![call("YEAR", vec![x]), number(1)]),
            other => return Err(not_impl(format!("date_trunc to {other:?}"))),
        };
        Ok(Some(cast(truncated, datetime())))
    }

    fn date_part(&self, unparser: &Unparser, args: &[Expr]) -> DFResult<Option<ast::Expr>> {
        let (Some(part), Some(arg)) = (literal_str(args, 0), args.get(1)) else {
            return Ok(None);
        };
        let x = unparser.expr_to_sql(arg)?;
        Ok(Some(match part.to_ascii_lowercase().as_str() {
            "year" => extract(ast::DateTimeField::Year, x),
            "quarter" => extract(ast::DateTimeField::Quarter, x),
            "month" => extract(ast::DateTimeField::Month, x),
            "day" => extract(ast::DateTimeField::Day, x),
            "hour" => extract(ast::DateTimeField::Hour, x),
            "minute" => extract(ast::DateTimeField::Minute, x),
            "second" => extract(ast::DateTimeField::Second, x),
            // ISO week, as DataFusion counts them
            "week" => call("WEEK", vec![x, number(3)]),
            // Sunday = 0 in DataFusion; DAYOFWEEK() has Sunday = 1
            "dow" => binary(
                call("DAYOFWEEK", vec![x]),
                ast::BinaryOperator::Minus,
                number(1),
            ),
            "doy" => call("DAYOFYEAR", vec![x]),
            "epoch" => call("UNIX_TIMESTAMP", vec![x]),
            other => return Err(not_impl(format!("date_part {other:?}"))),
        }))
    }

    /// `to_timestamp(x)` and its `_seconds`/`_millis`/`_micros`/`_nanos` variants read epoch numbers;
    /// `to_timestamp(x, format)` parses text. DataFusion types both as a zone-less UTC timestamp, so
    /// `FROM_UNIXTIME` (which answers in the session's zone) is converted back to UTC.
    fn to_timestamp(
        &self,
        unparser: &Unparser,
        name: &str,
        args: &[Expr],
    ) -> DFResult<Option<ast::Expr>> {
        let Some(arg) = args.first() else {
            return Ok(None);
        };
        if args.len() == 2 && name == "to_timestamp" {
            let Some(format) = literal_str(args, 1) else {
                return Err(not_impl("to_timestamp with a non-literal format"));
            };
            let Some(mysql_format) = chrono_to_mysql_format(format) else {
                return Err(not_impl(format!("the timestamp format {format:?}")));
            };
            return Ok(Some(call(
                "STR_TO_DATE",
                vec![unparser.expr_to_sql(arg)?, string_literal(&mysql_format)],
            )));
        }
        if args.len() != 1 {
            return Ok(None);
        }
        let divisor: i64 = match name {
            "to_timestamp" | "to_timestamp_seconds" => 1,
            "to_timestamp_millis" => 1_000,
            "to_timestamp_micros" => 1_000_000,
            "to_timestamp_nanos" => 1_000_000_000,
            _ => return Ok(None),
        };
        let seconds = if divisor == 1 {
            unparser.expr_to_sql(arg)?
        } else {
            binary(
                unparser.expr_to_sql(arg)?,
                ast::BinaryOperator::Divide,
                number(divisor),
            )
        };
        // a double's fraction (1742913282.314 is ...313999... in binary) would land in the
        // microseconds; a decimal keeps the digits that were meant
        let seconds = cast(
            seconds,
            ast::DataType::Decimal(ast::ExactNumberInfo::PrecisionAndScale(20, 6)),
        );
        Ok(Some(call(
            "CONVERT_TZ",
            vec![
                call("FROM_UNIXTIME", vec![seconds]),
                session_time_zone(),
                string_literal("+00:00"),
            ],
        )))
    }

    fn named_cast(&self, unparser: &Unparser, args: &[Expr]) -> DFResult<Option<ast::Expr>> {
        let Some(type_name) = literal_str(args, 1) else {
            return Ok(None);
        };
        let data_type = match type_name.to_ascii_uppercase().as_str() {
            "TEXT" => ast::DataType::Char(None),
            "BIGINT" => ast::DataType::Custom(
                ast::ObjectName::from(vec![ast::Ident::new("SIGNED")]),
                vec![],
            ),
            "DOUBLE" => ast::DataType::Double(ast::ExactNumberInfo::None),
            "NUMERIC" => ast::DataType::Decimal(ast::ExactNumberInfo::PrecisionAndScale(38, 10)),
            "DATE" => ast::DataType::Date,
            "TIMESTAMP" | "TIMESTAMPTZ" => datetime(),
            "TIME" => ast::DataType::Time(None, ast::TimezoneInfo::None),
            _ => return Ok(None),
        };
        Ok(Some(cast(unparser.expr_to_sql(&args[0])?, data_type)))
    }

    /// `regexp_match(x, re)[1]`, the only shape DataFusion's array-valued `regexp_match` can take
    /// here, as `REGEXP_SUBSTR`. MySQL returns the whole match where DataFusion returns the first
    /// capture group; the two agree for patterns without groups.
    fn first_regex_match(&self, unparser: &Unparser, args: &[Expr]) -> DFResult<Option<ast::Expr>> {
        let [Expr::ScalarFunction(f), Expr::Literal(datafusion::common::ScalarValue::Int64(Some(1)), _)] =
            args
        else {
            return Err(not_impl("array subscripts"));
        };
        if f.name() != "regexp_match" || f.args.len() < 2 {
            return Err(not_impl("array subscripts"));
        }
        Ok(Some(call(
            "REGEXP_SUBSTR",
            vec![
                unparser.expr_to_sql(&f.args[0])?,
                regex_pattern(unparser, &f.args, 2)?,
            ],
        )))
    }
}

impl Dialect for MetabaseMySqlDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        false
    }

    fn supports_qualify(&self) -> bool {
        false
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::MySQL
    }

    fn utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.utf8_cast_dtype()
    }

    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.large_utf8_cast_dtype()
    }

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        DateFieldExtractStyle::Extract
    }

    fn distinct_from_style(&self) -> DistinctFromStyle {
        DistinctFromStyle::Spaceship
    }

    fn int64_cast_dtype(&self) -> ast::DataType {
        self.inner.int64_cast_dtype()
    }

    fn int32_cast_dtype(&self) -> ast::DataType {
        self.inner.int32_cast_dtype()
    }

    fn timestamp_cast_dtype(
        &self,
        time_unit: &datafusion::arrow::datatypes::TimeUnit,
        tz: &Option<std::sync::Arc<str>>,
    ) -> ast::DataType {
        self.inner.timestamp_cast_dtype(time_unit, tz)
    }

    fn requires_derived_table_alias(&self) -> bool {
        true
    }

    /// MySQL refuses a frame on the ranking and offset functions; the unparser spells DataFusion's
    /// default frame out unless told not to.
    fn window_func_support_window_frame(
        &self,
        func_name: &str,
        _start_bound: &ast::WindowFrameBound,
        _end_bound: &ast::WindowFrameBound,
    ) -> bool {
        !matches!(
            func_name,
            "row_number"
                | "rank"
                | "dense_rank"
                | "percent_rank"
                | "cume_dist"
                | "ntile"
                | "lag"
                | "lead"
        )
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> DFResult<Option<ast::Expr>> {
        let sql = |i: usize| unparser.expr_to_sql(&args[i]);
        match func_name {
            "date_trunc" => self.date_trunc(unparser, args),
            "date_part" => self.date_part(unparser, args),
            "to_timestamp"
            | "to_timestamp_seconds"
            | "to_timestamp_millis"
            | "to_timestamp_micros"
            | "to_timestamp_nanos" => self.to_timestamp(unparser, func_name, args),
            "strpos" | "instr" if args.len() == 2 => {
                Ok(Some(call("LOCATE", vec![sql(1)?, sql(0)?])))
            }
            "starts_with" if args.len() == 2 => Ok(Some(binary(
                call("LEFT", vec![sql(0)?, call("CHAR_LENGTH", vec![sql(1)?])]),
                ast::BinaryOperator::Eq,
                sql(1)?,
            ))),
            "ends_with" if args.len() == 2 => Ok(Some(binary(
                call("RIGHT", vec![sql(0)?, call("CHAR_LENGTH", vec![sql(1)?])]),
                ast::BinaryOperator::Eq,
                sql(1)?,
            ))),
            "length" | "char_length" | "character_length" if args.len() == 1 => {
                Ok(Some(call("CHAR_LENGTH", vec![sql(0)?])))
            }
            "ln" if args.len() == 1 => Ok(Some(call("LOG", vec![sql(0)?]))),
            // DataFusion's one-argument log is base 10; MySQL's is natural
            "log" if args.len() == 1 => Ok(Some(call("LOG10", vec![sql(0)?]))),
            "trunc" if args.len() == 1 => Ok(Some(call("TRUNCATE", vec![sql(0)?, number(0)]))),
            "trunc" if args.len() == 2 => Ok(Some(call("TRUNCATE", vec![sql(0)?, sql(1)?]))),
            "random" => Ok(Some(call("RAND", vec![]))),
            // DataFusion names `trim` `btrim`; MySQL's one-argument `ltrim`/`rtrim` mean the same
            // and pass through, and characters to strip go through `TRIM(side chars FROM x)`
            "btrim" | "trim" if args.len() == 1 => Ok(Some(call("TRIM", vec![sql(0)?]))),
            // the unparser's own rendering of a one-sided trim drops the side
            "ltrim" if args.len() == 1 => Ok(Some(call("LTRIM", vec![sql(0)?]))),
            "rtrim" if args.len() == 1 => Ok(Some(call("RTRIM", vec![sql(0)?]))),
            "btrim" | "trim" | "ltrim" | "rtrim" if args.len() == 2 => {
                let trim_where = match func_name {
                    "ltrim" => ast::TrimWhereField::Leading,
                    "rtrim" => ast::TrimWhereField::Trailing,
                    _ => ast::TrimWhereField::Both,
                };
                Ok(Some(ast::Expr::Trim {
                    expr: Box::new(sql(0)?),
                    trim_where: Some(trim_where),
                    trim_what: Some(Box::new(sql(1)?)),
                    trim_characters: None,
                }))
            }
            // DataFusion names `trim` `btrim`, and takes the characters as a second argument
            "btrim" | "trim" | "ltrim" | "rtrim" if !args.is_empty() && args.len() <= 2 => {
                let trim_where = match func_name {
                    "ltrim" => ast::TrimWhereField::Leading,
                    "rtrim" => ast::TrimWhereField::Trailing,
                    _ => ast::TrimWhereField::Both,
                };
                Ok(Some(ast::Expr::Trim {
                    expr: Box::new(sql(0)?),
                    trim_where: if args.len() == 2 {
                        Some(trim_where)
                    } else {
                        None
                    },
                    trim_what: if args.len() == 2 {
                        Some(Box::new(sql(1)?))
                    } else {
                        None
                    },
                    trim_characters: None,
                }))
            }
            // DataFusion's concat skips NULL arguments; MySQL's CONCAT returns NULL for them
            "concat" => {
                let mut sql_args = vec![string_literal("")];
                for arg in args {
                    sql_args.push(unparser.expr_to_sql(arg)?);
                }
                Ok(Some(call("CONCAT_WS", sql_args)))
            }
            "regexp_like" if args.len() == 2 || args.len() == 3 => {
                Ok(Some(ast::Expr::Nested(Box::new(ast::Expr::RLike {
                    negated: false,
                    expr: Box::new(sql(0)?),
                    pattern: Box::new(regex_pattern(unparser, args, 2)?),
                    regexp: true,
                }))))
            }
            // MySQL replaces every match; DataFusion only with the `g` flag
            "regexp_replace" if args.len() == 4 && literal_str(args, 3) == Some("g") => {
                Ok(Some(call(
                    "REGEXP_REPLACE",
                    vec![sql(0)?, regex_pattern(unparser, args, 4)?, sql(2)?],
                )))
            }
            "regexp_replace" => Err(not_impl("regexp_replace of the first match only")),
            "regexp_match" => Err(not_impl("regexp_match outside a `[1]` subscript")),
            "array_element" => self.first_regex_match(unparser, args),
            // `SUBSTRING_INDEX` hands back the whole remainder for a position past the last part,
            // where DataFusion gives ''; the part count is spelled out to tell the two apart
            "split_part" if args.len() == 3 => {
                let parts = binary(
                    binary(
                        binary(
                            call("CHAR_LENGTH", vec![sql(0)?]),
                            ast::BinaryOperator::Minus,
                            call(
                                "CHAR_LENGTH",
                                vec![call("REPLACE", vec![sql(0)?, sql(1)?, string_literal("")])],
                            ),
                        ),
                        ast::BinaryOperator::Divide,
                        call("CHAR_LENGTH", vec![sql(1)?]),
                    ),
                    ast::BinaryOperator::Plus,
                    number(1),
                );
                let part = call(
                    "SUBSTRING_INDEX",
                    vec![
                        call("SUBSTRING_INDEX", vec![sql(0)?, sql(1)?, sql(2)?]),
                        sql(1)?,
                        number(-1),
                    ],
                );
                Ok(Some(ast::Expr::Case {
                    operand: None,
                    conditions: vec![ast::CaseWhen {
                        condition: binary(sql(2)?, ast::BinaryOperator::Gt, parts),
                        result: string_literal(""),
                    }],
                    else_result: Some(Box::new(part)),
                    case_token: ast::helpers::attached_token::AttachedToken::empty(),
                    end_token: ast::helpers::attached_token::AttachedToken::empty(),
                }))
            }
            udfs::MB_CAST => self.named_cast(unparser, args),
            udfs::MB_BYTES_TO_TEXT if args.len() == 1 => Ok(Some(ast::Expr::Convert {
                is_try: false,
                expr: Box::new(sql(0)?),
                data_type: None,
                charset: Some(ast::ObjectName::from(vec![ast::Ident::new("utf8mb4")])),
                target_before_value: false,
                styles: vec![],
            })),
            udfs::MB_ZONED_TO_WALL if args.len() == 2 => Ok(Some(call(
                "CONVERT_TZ",
                vec![sql(0)?, session_time_zone(), sql(1)?],
            ))),
            udfs::MB_WALL_TO_ZONED if args.len() == 2 => Ok(Some(call(
                "CONVERT_TZ",
                vec![sql(0)?, sql(1)?, session_time_zone()],
            ))),
            udfs::MB_AT_TIME_ZONE => Err(not_impl(
                "AT TIME ZONE on a value of unknown type (the planner resolves the direction)",
            )),
            // shown in the session zone as a DATETIME; the plan's zoned type reads it back from
            // that zone into the instant
            udfs::MB_FROM_UNIX if args.len() == 1 => Ok(Some(call(
                "FROM_UNIXTIME",
                vec![cast(
                    sql(0)?,
                    ast::DataType::Decimal(ast::ExactNumberInfo::PrecisionAndScale(20, 6)),
                )],
            ))),
            udfs::MB_TRUNC if args.len() == 1 => {
                Ok(Some(call("TRUNCATE", vec![sql(0)?, number(0)])))
            }
            udfs::MB_INT_DIV if args.len() == 2 => Ok(Some(binary(
                sql(0)?,
                ast::BinaryOperator::MyIntegerDivide,
                sql(1)?,
            ))),
            _ => self
                .inner
                .scalar_function_to_sql_overrides(unparser, func_name, args),
        }
    }
}

/// Translate a Chrono strftime pattern (what DataFusion's `to_timestamp` takes) into a MySQL
/// `STR_TO_DATE` format. Returns None for a specifier with no MySQL equivalent.
pub fn chrono_to_mysql_format(chrono: &str) -> Option<String> {
    let mut out = String::with_capacity(chrono.len() * 2);
    let mut chars = chrono.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let spec = match chars.next()? {
            'Y' => "%Y",
            'y' => "%y",
            'm' => "%m",
            'd' => "%d",
            'e' => "%e",
            'H' => "%H",
            'I' => "%h",
            'M' => "%i",
            'S' => "%s",
            'j' => "%j",
            'b' | 'h' => "%b",
            'B' => "%M",
            'a' => "%a",
            'A' => "%W",
            'p' => "%p",
            'f' => "%f",
            '%' => "%%",
            '.' => match chars.next()? {
                'f' => ".%f",
                _ => return None,
            },
            _ => return None,
        };
        out.push_str(spec);
    }
    Some(out)
}

/// DataFusion function names that unparse to valid MySQL/MariaDB with matching semantics, either
/// verbatim or through the overrides above.
pub const MYSQL_FUNCTIONS: &[&str] = &[
    // math
    "abs",
    "ceil",
    "ceiling",
    "floor",
    "round",
    "trunc",
    "sqrt",
    "power",
    "pow",
    "exp",
    "ln",
    "log",
    "log10",
    "log2",
    "mod",
    "sign",
    "pi",
    "random",
    "greatest",
    "least",
    "mb_int_div",
    "mb_trunc",
    "mb_from_unix",
    // strings
    "lower",
    "upper",
    "length",
    "char_length",
    "character_length",
    "trim",
    "ltrim",
    "rtrim",
    "btrim",
    "concat",
    "concat_ws",
    "replace",
    "substr",
    "substring",
    "strpos",
    "instr",
    "starts_with",
    "ends_with",
    "left",
    "right",
    "lpad",
    "rpad",
    "repeat",
    "reverse",
    "ascii",
    "md5",
    "split_part",
    "regexp_like",
    "regexp_replace",
    "regexp_match",
    "array_element",
    "mb_bytes_to_text",
    // temporal
    "date_trunc",
    "date_part",
    "now",
    "current_date",
    "current_time",
    "current_timestamp",
    "to_timestamp",
    "to_timestamp_seconds",
    "to_timestamp_millis",
    "to_timestamp_micros",
    "to_timestamp_nanos",
    "mb_zoned_to_wall",
    "mb_wall_to_zoned",
    "mb_at_time_zone",
    // generic
    "coalesce",
    "nullif",
    "ifnull",
    "nvl",
    "mb_cast",
    // aggregates
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "var_samp",
    "var_pop",
    "variance",
    "grouping",
    // window
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value",
];

#[cfg(test)]
mod tests {
    use super::chrono_to_mysql_format;

    #[test]
    fn translates_common_specifiers() {
        assert_eq!(
            chrono_to_mysql_format("%Y%m%d%H%M%S").as_deref(),
            Some("%Y%m%d%H%i%s")
        );
        assert_eq!(
            chrono_to_mysql_format("%Y-%m-%d %H:%M:%S%.f").as_deref(),
            Some("%Y-%m-%d %H:%i:%s.%f")
        );
        assert_eq!(
            chrono_to_mysql_format("%d/%b/%Y %I:%M %p").as_deref(),
            Some("%d/%b/%Y %h:%i %p")
        );
        assert_eq!(chrono_to_mysql_format("%z"), None);
    }
}
