// Local Spark DATE and TIMESTAMP_NTZ cast. See the host crate's UPSTREAM.md.
//! A cast between TIMESTAMP_NTZ and DATE, with Arrow's values and Spark's nullability. Spark
//! declares such a cast nullable whatever its input (`Cast.forceNullable`), so a cast of a NOT
//! NULL column is nullable at every stage, and a literal's until constant folding replaces it.
//! Its TRY_CAST from DATE to TIMESTAMP_NTZ cannot fail and keeps its input's nullability.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTimestampNtzCast {
    target: DataType,
    /// Whether the result is nullable whatever the argument declares.
    nullable_result: bool,
    signature: Signature,
}

impl SparkDateTimestampNtzCast {
    /// A TIMESTAMP_NTZ cast to DATE, explicit or implicit, also under TRY_CAST.
    pub fn to_date() -> Self {
        Self::new(DataType::Date32, true)
    }

    /// A DATE cast to TIMESTAMP_NTZ: nullable for CAST and implicit casts, and nullable only for
    /// a nullable argument under TRY_CAST.
    pub fn to_timestamp_ntz(try_cast: bool) -> Self {
        Self::new(DataType::Timestamp(TimeUnit::Microsecond, None), !try_cast)
    }

    fn new(target: DataType, nullable_result: bool) -> Self {
        Self {
            target,
            nullable_result,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// Whether this cast converts between these types: a TIMESTAMP_NTZ to DATE, or a DATE to
    /// TIMESTAMP_NTZ.
    pub fn accepts(from: &DataType, to: &DataType) -> bool {
        matches!(
            (from, to),
            (DataType::Timestamp(_, None), DataType::Date32)
                | (
                    DataType::Date32 | DataType::Date64,
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                )
        )
    }
}

impl ScalarUDFImpl for SparkDateTimestampNtzCast {
    fn name(&self) -> &str {
        match self.target {
            DataType::Date32 => "spark_timestamp_ntz_to_date",
            _ => "spark_date_to_timestamp_ntz",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [argument] = args.arg_fields else {
            return internal_err!("{} requires one argument", self.name());
        };
        if !Self::accepts(argument.data_type(), &self.target) {
            return internal_err!(
                "{} cannot cast {} to {}",
                self.name(),
                argument.data_type(),
                self.target
            );
        }
        Ok(Arc::new(Field::new(
            self.name(),
            self.target.clone(),
            self.nullable_result || argument.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument] = args.args.as_slice() else {
            return internal_err!("{} requires one argument", self.name());
        };
        argument.cast_to(&self.target, None)
    }
}
