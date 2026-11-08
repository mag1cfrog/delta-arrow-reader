// Local Spark TIMESTAMP subtraction. See the host crate's UPSTREAM.md.
//! Spark's TIMESTAMP subtraction, as `SubtractTimestamps` gives it: the DAY TO SECOND
//! interval between the two local date-times in their time zone, which differs from the
//! elapsed time Arrow gives across a DST change or a change of local mean time. Against a
//! TIMESTAMP_NTZ, Spark first converts the TIMESTAMP to its local date-time. TRY_SUBTRACT
//! gives NULL where the subtraction overflows.

use std::sync::Arc;

use datafusion::arrow::array::types::TimestampMicrosecondType;
use datafusion::arrow::array::{AsArray, DurationMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::scalar::datetime::local_time::TimestampZone;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSubtractTimestamps {
    /// TRY_SUBTRACT: NULL where the subtraction overflows.
    null_on_error: bool,
    /// Spark's cast of a DATE to TIMESTAMP_NTZ is nullable, so a difference with one is too.
    nullable_result: bool,
    signature: Signature,
}

impl Default for SparkSubtractTimestamps {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSubtractTimestamps {
    /// The `-` operator.
    pub fn new() -> Self {
        Self::with_null_on_error(false)
    }

    /// TRY_SUBTRACT.
    pub fn try_subtract() -> Self {
        Self::with_null_on_error(true)
    }

    /// The same subtraction, declared nullable whatever its arguments declare.
    pub fn with_nullable_result(self) -> Self {
        Self {
            nullable_result: true,
            ..self
        }
    }

    fn with_null_on_error(null_on_error: bool) -> Self {
        Self {
            null_on_error,
            nullable_result: false,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    /// Whether the kernel subtracts values of these types: TIMESTAMP or TIMESTAMP_NTZ values.
    pub fn accepts(end: &DataType, start: &DataType) -> bool {
        [end, start]
            .iter()
            .all(|data_type| matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, _)))
    }
}

/// How an operand reads its local date-time.
enum Operand {
    /// A TIMESTAMP, in the zone its type names.
    Zoned(TimestampZone),
    /// A TIMESTAMP_NTZ, which is its local date-time.
    Local,
}

impl Operand {
    fn local(&self, micros: i64) -> i128 {
        match self {
            Self::Zoned(zone) => i128::from(micros) + zone.offset_micros(micros),
            Self::Local => i128::from(micros),
        }
    }
}

impl ScalarUDFImpl for SparkSubtractTimestamps {
    fn name(&self) -> &str {
        if self.null_on_error {
            "spark_try_subtract_timestamps"
        } else {
            "spark_subtract_timestamps"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [end, start] if Self::accepts(end, start) => {
                Ok(DataType::Duration(TimeUnit::Microsecond))
            }
            _ => internal_err!("{} requires two TIMESTAMP arguments", self.name()),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            self.null_on_error
                || self.nullable_result
                || args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    // Spark folds a constant subtraction during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        !self.null_on_error
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [end, start] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let operand = |value: &ColumnarValue| match value.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) => {
                match TimestampZone::parse(&zone) {
                    Some(parsed) => Ok(Operand::Zoned(parsed)),
                    None => exec_err!("invalid time zone {zone}"),
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(Operand::Local),
            other => internal_err!("{} requires a TIMESTAMP, got {other}", self.name()),
        };
        let (end_operand, start_operand) = (operand(end)?, operand(start)?);
        // Spark converts the TIMESTAMP against a TIMESTAMP_NTZ to a TIMESTAMP_NTZ, which fails
        // beyond the BIGINT range.
        let local_bounded =
            matches!(end_operand, Operand::Local) || matches!(start_operand, Operand::Local);
        let scalar = matches!(
            (end, start),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let ends = end.to_array(args.number_rows)?;
        let starts = start.to_array(args.number_rows)?;
        let result = ends
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .zip(starts.as_primitive::<TimestampMicrosecondType>().iter())
            .map(|pair| match pair {
                (Some(end), Some(start)) => {
                    let (end, start) = (end_operand.local(end), start_operand.local(start));
                    let in_range = |local: i128| i64::try_from(local).is_ok();
                    match i64::try_from(end - start) {
                        Ok(difference) if !local_bounded || (in_range(end) && in_range(start)) => {
                            Ok(Some(difference))
                        }
                        // Java's exact arithmetic message.
                        _ if self.null_on_error => Ok(None),
                        _ => exec_err!("long overflow"),
                    }
                }
                _ => Ok(None),
            })
            .collect::<Result<DurationMicrosecondArray>>()?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}
