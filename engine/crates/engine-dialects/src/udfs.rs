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

/// Every engine UDF, for registration in a session.
pub fn metabase_udfs() -> Vec<Arc<ScalarUDF>> {
    vec![Arc::new(ScalarUDF::new_from_impl(MbToTime {
        signature: Signature::any(1, Volatility::Immutable),
    }))]
}
