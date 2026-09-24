//! Engine-side scalar functions. They exist so the planner can type an expression the SQL surface
//! can't express in a way the unparser renders for every dialect; each dialect spells them out.
//! None of them ever executes: the plan is pushed to the warehouse whole.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{not_impl_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// `mb_to_time(x)`: a cast to `TIME`. `CAST(x AS TIME)` itself plans as `Time64(Nanosecond)`,
/// which the unparser has no SQL spelling for; this types as microseconds, what warehouses send.
pub const MB_TO_TIME: &str = "mb_to_time";

#[derive(Debug, PartialEq, Eq, Hash)]
struct MbToTime {
    signature: Signature,
}

impl ScalarUDFImpl for MbToTime {
    fn name(&self) -> &str {
        MB_TO_TIME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Microsecond))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("{MB_TO_TIME} is unparsed for the warehouse, never executed by the engine")
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

/// Every engine UDF, for registration in a session.
pub fn metabase_udfs() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(MbToTime {
            signature: Signature::any(1, Volatility::Immutable),
        })),
        Arc::new(ScalarUDF::new_from_impl(MbAtTimeZone {
            signature: Signature::any(2, Volatility::Immutable),
        })),
    ]
}
