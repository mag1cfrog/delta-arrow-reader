// Local Spark BIGINT-to-INT conversion. See the host crate's UPSTREAM.md.
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type, Int64Type};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    lit,
};

use crate::functions_utils::make_scalar_function;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CastMode {
    Legacy,
    Ansi,
    Try,
}

/// Spark's semantic BIGINT-to-INT conversion, including each mode's result field.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastBigintToInt {
    signature: Signature,
    mode: CastMode,
}

impl SparkCastBigintToInt {
    /// Select the query's CAST mode, or checked NULL-on-overflow TRY_CAST.
    pub fn new(ansi_mode: bool, is_try: bool) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            mode: if is_try {
                CastMode::Try
            } else if ansi_mode {
                CastMode::Ansi
            } else {
                CastMode::Legacy
            },
        }
    }

    pub fn is_try(&self) -> bool {
        self.mode == CastMode::Try
    }

    /// Convert an already evaluated value without evaluating its producer again.
    pub fn cast_value(&self, value: &ColumnarValue) -> Result<ColumnarValue> {
        self.return_type(&[value.data_type()])?;
        if self.mode != CastMode::Legacy {
            return value.cast_to(
                &DataType::Int32,
                Some(&CastOptions {
                    safe: self.is_try(),
                    ..Default::default()
                }),
            );
        }
        make_scalar_function(
            |arrays| {
                // Rust integer narrowing keeps the signed low 32 bits.
                Ok(Arc::new(
                    arrays[0]
                        .as_primitive::<Int64Type>()
                        .unary::<_, Int32Type>(|value| value as i32),
                ) as ArrayRef)
            },
            vec![],
        )(std::slice::from_ref(value))
    }
}

impl ScalarUDFImpl for SparkCastBigintToInt {
    fn name(&self) -> &str {
        "spark_cast_bigint_to_int"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        if !matches!(arguments, [DataType::Int64]) {
            return exec_err!("BIGINT-to-INT CAST expects one BIGINT argument, got {arguments:?}");
        }
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = take_function_args(self.name(), args.arg_fields)?;
        self.return_type(std::slice::from_ref(input.data_type()))?;
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int32,
            input.is_nullable() || self.is_try(),
        )))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if let [Expr::Literal(value @ ScalarValue::Int64(_), _)] = args.as_slice() {
            // Keep literal CAST failures at the native planning boundary.
            let value = self.cast_value(&ColumnarValue::Scalar(value.clone()))?;
            if let ColumnarValue::Scalar(value) = value {
                return Ok(ExprSimplifyResult::Simplified(lit(value)));
            }
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = take_function_args(self.name(), args.args.as_slice())?;
        self.cast_value(value)
    }
}
