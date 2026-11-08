// Local Spark TIMESTAMP subtraction. See the host crate's UPSTREAM.md.
//! Spark's TIMESTAMP subtraction, as `SubtractTimestamps` gives it: the DAY TO SECOND
//! interval between the two local date-times in their time zone, which differs from the
//! elapsed time Arrow gives across a DST change or a change of local mean time.

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
    signature: Signature,
}

impl Default for SparkSubtractTimestamps {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSubtractTimestamps {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    /// Whether the kernel subtracts TIMESTAMP values of these types: both with a time zone.
    pub fn accepts(end: &DataType, start: &DataType) -> bool {
        [end, start].iter().all(|data_type| {
            matches!(
                data_type,
                DataType::Timestamp(TimeUnit::Microsecond, Some(_))
            )
        })
    }
}

impl ScalarUDFImpl for SparkSubtractTimestamps {
    fn name(&self) -> &str {
        "spark_subtract_timestamps"
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
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    // Spark folds a constant subtraction during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [end, start] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let zone = |value: &ColumnarValue| match value.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) => {
                match TimestampZone::parse(&zone) {
                    Some(parsed) => Ok(parsed),
                    None => exec_err!("invalid time zone {zone}"),
                }
            }
            other => internal_err!("{} requires a TIMESTAMP, got {other}", self.name()),
        };
        let (end_zone, start_zone) = (zone(end)?, zone(start)?);
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
                    let local = |micros: i64, zone: &TimestampZone| {
                        i128::from(micros) + zone.offset_micros(micros)
                    };
                    match i64::try_from(local(end, &end_zone) - local(start, &start_zone)) {
                        Ok(difference) => Ok(Some(difference)),
                        // Java's exact arithmetic message.
                        Err(_) => exec_err!("long overflow"),
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
