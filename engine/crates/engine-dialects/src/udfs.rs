//! Engine-side scalar functions. They exist so the planner can type an expression the SQL surface
//! can't express in a way the unparser renders for every dialect; each dialect spells them out.
//! None of them ever executes: the plan is pushed to the warehouse whole.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{not_impl_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// `mb_cast(x, 'TYPE')`: a cast the plan names differently from its argument. DataFusion gives
/// `CAST(x AS t)` the same schema name as `x`, so `max(x)` and `max(CAST(x AS TEXT))` in one
/// query collide and the query is refused; a function call has its own name. It also types
/// `TIME` as microseconds, which the unparser can spell (a plain cast plans as nanoseconds).
/// The type names are SQL's: TEXT, BIGINT, DOUBLE, NUMERIC, DATE, TIMESTAMP, TIMESTAMPTZ, TIME.
pub const MB_CAST: &str = "mb_cast";

/// The Arrow type `mb_cast` produces for a type name, if it is one we know.
pub fn cast_target(type_name: &str) -> Option<DataType> {
    Some(match type_name.to_ascii_uppercase().as_str() {
        "TEXT" => DataType::Utf8,
        "BIGINT" => DataType::Int64,
        "DOUBLE" => DataType::Float64,
        "NUMERIC" => DataType::Decimal128(38, 10),
        "DATE" => DataType::Date32,
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMPTZ" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "TIME" => DataType::Time64(TimeUnit::Microsecond),
        _ => return None,
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MbCast {
    signature: Signature,
}

impl ScalarUDFImpl for MbCast {
    fn name(&self) -> &str {
        MB_CAST
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!("{MB_CAST} needs its type name to decide its type")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let Some(Some(ScalarValue::Utf8(Some(type_name)))) = args.scalar_arguments.get(1) else {
            return plan_err!("{MB_CAST}: the second argument must be a type name literal");
        };
        let Some(data_type) = cast_target(type_name) else {
            return plan_err!("{MB_CAST}: unknown type name {type_name:?}");
        };
        Ok(Arc::new(Field::new(MB_CAST, data_type, true)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("{MB_CAST} is unparsed for the warehouse, never executed by the engine")
    }
}

/// `mb_bytes_to_text(x)`: a byte string read as UTF-8 text (Postgres' `convert_from(x, 'UTF8')`),
/// which DataFusion has no function for.
pub const MB_BYTES_TO_TEXT: &str = "mb_bytes_to_text";

#[derive(Debug, PartialEq, Eq, Hash)]
struct MbBytesToText {
    signature: Signature,
}

impl ScalarUDFImpl for MbBytesToText {
    fn name(&self) -> &str {
        MB_BYTES_TO_TEXT
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!(
            "{MB_BYTES_TO_TEXT} is unparsed for the warehouse, never executed by the engine"
        )
    }
}

/// `mb_at_time_zone(x, 'zone')`: SQL's `x AT TIME ZONE 'zone'`. DataFusion plans that syntax as a
/// bare cast that the unparser can't reproduce; this keeps the zone as an argument. As in Postgres, a
/// zoned timestamp becomes the wall-clock time in that zone (no zone), and an unzoned one is read
/// as wall-clock time in that zone and becomes an instant.
pub const MB_AT_TIME_ZONE: &str = "mb_at_time_zone";

#[derive(Debug, PartialEq, Eq, Hash)]
struct MbAtTimeZone {
    signature: Signature,
}

impl ScalarUDFImpl for MbAtTimeZone {
    fn name(&self) -> &str {
        MB_AT_TIME_ZONE
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types.first() {
            Some(DataType::Timestamp(_, Some(_))) => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            _ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        })
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!(
            "{MB_AT_TIME_ZONE} is unparsed for the warehouse, never executed by the engine"
        )
    }
}

/// `mb_int_div(a, b)`: integer division, truncating toward zero, which is what DataFusion's `/`
/// means for two integers. The planner rewrites integer `/` into this so a dialect whose `/`
/// is not integer division (MySQL's yields a decimal) can spell it (`DIV`).
pub const MB_INT_DIV: &str = "mb_int_div";

#[derive(Debug, PartialEq, Eq, Hash)]
struct MbIntDiv {
    signature: Signature,
}

impl ScalarUDFImpl for MbIntDiv {
    fn name(&self) -> &str {
        MB_INT_DIV
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Int64))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("{MB_INT_DIV} is unparsed for the warehouse, never executed by the engine")
    }
}

/// `mb_zoned_to_wall(x, 'zone')`: the wall-clock time of instant `x` in `zone` (no zone), and
/// `mb_wall_to_zoned(x, 'zone')`: the instant at which wall-clock time `x` happens in `zone`.
/// The planner rewrites [`MB_AT_TIME_ZONE`] into one of these from its argument's type, so a
/// dialect without a type-directed `AT TIME ZONE` (MySQL's `CONVERT_TZ` needs to be told which
/// way) knows the direction.
pub const MB_ZONED_TO_WALL: &str = "mb_zoned_to_wall";
pub const MB_WALL_TO_ZONED: &str = "mb_wall_to_zoned";

#[derive(Debug, PartialEq, Eq, Hash)]
struct MbTimeZoneDirection {
    signature: Signature,
    name: &'static str,
    zoned: bool,
}

impl ScalarUDFImpl for MbTimeZoneDirection {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(if self.zoned {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        } else {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        })
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!(
            "{} is unparsed for the warehouse, never executed by the engine",
            self.name
        )
    }
}

/// `mb_trunc(x)`: `x` with its fraction dropped, in `x`'s own type. DataFusion's cast of a float or
/// a decimal to an integer truncates, Postgres' and MySQL's round, so the planner puts this in
/// front of such a cast; each dialect spells it (`trunc`, `TRUNCATE(x, 0)`).
pub const MB_TRUNC: &str = "mb_trunc";

#[derive(Debug, PartialEq, Eq, Hash)]
struct MbTrunc {
    signature: Signature,
}

impl ScalarUDFImpl for MbTrunc {
    fn name(&self) -> &str {
        MB_TRUNC
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Float64))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("{MB_TRUNC} is unparsed for the warehouse, never executed by the engine")
    }
}

/// `mb_from_unix(seconds)`: the instant `seconds` (possibly fractional) after the epoch, typed
/// zoned. DataFusion's `to_timestamp` types its result zone-less (a UTC wall clock), which
/// Metabase would then read in the report time zone; an epoch is an instant.
pub const MB_FROM_UNIX: &str = "mb_from_unix";

#[derive(Debug, PartialEq, Eq, Hash)]
struct MbFromUnix {
    signature: Signature,
}

impl ScalarUDFImpl for MbFromUnix {
    fn name(&self) -> &str {
        MB_FROM_UNIX
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("{MB_FROM_UNIX} is unparsed for the warehouse, never executed by the engine")
    }
}

/// An aggregate function under the name a dialect knows it by. DataFusion calls its sample
/// standard deviation `stddev` and its sample variance `var`; MySQL's `STDDEV` is the population
/// one and it has no `VAR`, Postgres has no `var`. The planner swaps the function for this
/// wrapper, which is never executed either.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RenamedAggregate {
    inner: Arc<AggregateUDF>,
    name: String,
}

impl AggregateUDFImpl for RenamedAggregate {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        not_impl_err!(
            "{} is unparsed for the warehouse, never executed by the engine",
            self.name
        )
    }
}

/// `inner` under `name`.
pub fn renamed_aggregate(inner: Arc<AggregateUDF>, name: &str) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(RenamedAggregate {
        inner,
        name: name.to_string(),
    }))
}

/// Every engine UDF, for registration in a session.
pub fn metabase_udfs() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(MbCast {
            signature: Signature::any(2, Volatility::Immutable),
        })),
        Arc::new(ScalarUDF::new_from_impl(MbBytesToText {
            signature: Signature::any(1, Volatility::Immutable),
        })),
        Arc::new(ScalarUDF::new_from_impl(MbAtTimeZone {
            signature: Signature::any(2, Volatility::Immutable),
        })),
        Arc::new(ScalarUDF::new_from_impl(MbIntDiv {
            signature: Signature::any(2, Volatility::Immutable),
        })),
        Arc::new(ScalarUDF::new_from_impl(MbTrunc {
            signature: Signature::any(1, Volatility::Immutable),
        })),
        Arc::new(ScalarUDF::new_from_impl(MbFromUnix {
            signature: Signature::any(1, Volatility::Immutable),
        })),
        Arc::new(ScalarUDF::new_from_impl(MbTimeZoneDirection {
            signature: Signature::any(2, Volatility::Immutable),
            name: MB_ZONED_TO_WALL,
            zoned: false,
        })),
        Arc::new(ScalarUDF::new_from_impl(MbTimeZoneDirection {
            signature: Signature::any(2, Volatility::Immutable),
            name: MB_WALL_TO_ZONED,
            zoned: true,
        })),
    ]
}
