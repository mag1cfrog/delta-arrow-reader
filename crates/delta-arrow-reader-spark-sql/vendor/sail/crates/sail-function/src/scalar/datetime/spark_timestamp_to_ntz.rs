// Local Spark TIMESTAMP to TIMESTAMP_NTZ conversion. See the host crate's UPSTREAM.md.
//! Spark's conversion from TIMESTAMP to TIMESTAMP_NTZ: the local date-time of each instant in
//! the session time zone, whatever zone the argument's type names.

use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::types::TimestampMicrosecondType;
use datafusion::arrow::array::{AsArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::scalar::datetime::local_time::local_micros;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampToNtz {
    session_timezone: Arc<str>,
    /// TRY_CAST and TRY_TO_TIMESTAMP return NULL for a local date-time beyond the BIGINT
    /// range.
    null_on_overflow: bool,
    signature: Signature,
}

impl SparkTimestampToNtz {
    /// The conversion in `session_timezone`, or `None` when Arrow cannot parse that zone.
    pub fn for_session_zone(session_timezone: Arc<str>, null_on_overflow: bool) -> Option<Self> {
        Tz::from_str(&session_timezone).ok()?;
        Some(Self {
            session_timezone,
            null_on_overflow,
            signature: Signature::any(1, Volatility::Immutable),
        })
    }

    /// The local date-time of `micros`, which Spark cannot store beyond the BIGINT range.
    fn convert(&self, micros: i64, zone: &Tz) -> Result<Option<i64>> {
        match i64::try_from(local_micros(micros, zone)) {
            Ok(local) => Ok(Some(local)),
            Err(_) if self.null_on_overflow => Ok(None),
            // Java's exact arithmetic message.
            Err(_) => exec_err!("long overflow"),
        }
    }
}

impl ScalarUDFImpl for SparkTimestampToNtz {
    fn name(&self) -> &str {
        "spark_timestamp_to_ntz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Timestamp(TimeUnit::Microsecond, Some(_))] => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
            _ => internal_err!("{} requires a TIMESTAMP argument", self.name()),
        }
    }

    // Spark folds a constant conversion during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        true
    }

    // Spark declares the result nullable only for a nullable argument, even for TRY_CAST.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return internal_err!("{} requires one argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[field.data_type().clone()])?,
            field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return internal_err!("{} requires one argument", self.name());
        };
        let Ok(zone) = Tz::from_str(&self.session_timezone) else {
            return exec_err!("invalid session time zone {}", self.session_timezone);
        };
        match value {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(micros, _)) => {
                let local = match micros {
                    Some(micros) => self.convert(*micros, &zone)?,
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    local, None,
                )))
            }
            ColumnarValue::Array(array) => {
                let local = array
                    .as_primitive::<TimestampMicrosecondType>()
                    .iter()
                    .map(|micros| match micros {
                        Some(micros) => self.convert(micros, &zone),
                        None => Ok(None),
                    })
                    .collect::<Result<TimestampMicrosecondArray>>()?;
                Ok(ColumnarValue::Array(Arc::new(local)))
            }
            other => internal_err!(
                "{} requires a TIMESTAMP argument, got {}",
                self.name(),
                other.data_type()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::exec_datafusion_err;

    use super::*;

    fn convert_with(zone: &str, micros: i64, null_on_overflow: bool) -> Result<Option<i64>> {
        let conversion = SparkTimestampToNtz::for_session_zone(zone.into(), null_on_overflow)
            .ok_or_else(|| exec_datafusion_err!("invalid test zone {zone}"))?;
        conversion.convert(micros, &Tz::from_str(zone)?)
    }

    fn convert(zone: &str, micros: i64) -> Result<i64> {
        convert_with(zone, micros, false)?.ok_or_else(|| exec_datafusion_err!("missing value"))
    }

    #[test]
    fn instants_read_their_local_date_time() -> Result<()> {
        // 2024-01-02 13:04:05.123456 UTC.
        let instant = 1_704_200_645_123_456;
        assert_eq!(convert("UTC", instant)?, instant);
        assert_eq!(
            convert("America/Los_Angeles", instant)?,
            instant - 8 * 3_600_000_000
        );
        assert_eq!(convert("+05:45", instant)?, instant + 345 * 60_000_000);
        // Both instants of the repeated hour on 2024-11-03 read 01:30 in Los Angeles.
        let daylight = 1_730_622_600_000_000;
        let standard = daylight + 3_600_000_000;
        assert_eq!(
            convert("America/Los_Angeles", daylight)?,
            convert("America/Los_Angeles", standard)?
        );
        Ok(())
    }

    #[test]
    fn local_date_times_beyond_bigint_fail() -> Result<()> {
        assert_eq!(convert("UTC", i64::MAX)?, i64::MAX);
        assert_eq!(
            convert("America/Los_Angeles", i64::MAX)?,
            i64::MAX - 8 * 3_600_000_000
        );
        assert!(convert("America/Los_Angeles", i64::MIN).is_err());
        assert!(convert("Asia/Kathmandu", i64::MAX).is_err());
        assert_eq!(convert_with("Asia/Kathmandu", i64::MAX, true)?, None);
        Ok(())
    }

    #[test]
    fn zones_arrow_cannot_parse_have_no_conversion() {
        assert!(SparkTimestampToNtz::for_session_zone("PST".into(), false).is_none());
        assert!(SparkTimestampToNtz::for_session_zone("+05:45".into(), false).is_some());
    }
}
