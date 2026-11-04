// Local Spark LIKE adapter. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
use std::sync::Arc;

use arrow::array::{BooleanArray, LargeStringArray, LargeStringBuilder};
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::physical_expr_common::datum::apply_cmp;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::type_coercion::binary::like_coercion;
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility, lit,
};

use super::rewrite_like_pattern::rewrite_like_into_string;

/// Validate only live dynamic patterns, then delegate matching to native LIKE.
/// The mandatory Spark analyzer checks foldable patterns after dead-input pruning.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkLike {
    escape: char,
    signature: Signature,
    interpreted_local: bool,
}

impl SparkLike {
    pub fn new(escape: char) -> Self {
        Self {
            escape,
            signature: Signature::user_defined(Volatility::Immutable),
            interpreted_local: false,
        }
    }

    // Early local interpretation retains this private state across optimizer
    // passes. Late interpretation marks only the discarded preview. Runtime
    // validation and matching never consult the marker.
    pub fn after_local_evaluation(&self) -> Self {
        Self {
            escape: self.escape,
            signature: self.signature.clone(),
            interpreted_local: true,
        }
    }

    pub fn requires_literal_initialization(&self) -> bool {
        !self.interpreted_local
    }

    pub fn validate_pattern(&self, pattern: &str) -> Result<()> {
        rewrite_like_into_string(&mut String::new(), pattern, self.escape)
    }

    fn rewrite_scalar_pattern(&self, value: ScalarValue) -> Result<ScalarValue> {
        match value.try_as_str() {
            Some(Some(pattern)) => {
                let mut buffer = String::new();
                rewrite_like_into_string(&mut buffer, pattern, self.escape)?;
                // ScalarValue::cast_to first materializes the source variant.
                // Keep i64 offsets before casting to the native representation.
                Ok(ScalarValue::LargeUtf8(Some(buffer)))
            }
            Some(None) => Ok(ScalarValue::LargeUtf8(None)),
            None if value.is_null() => Ok(ScalarValue::LargeUtf8(None)),
            None => exec_err!("unsupported LIKE pattern type: {}", value.data_type()),
        }
    }

    fn native_like_args(
        &self,
        mut source: ColumnarValue,
        pattern: ColumnarValue,
        source_nulls: Option<&NullBuffer>,
    ) -> Result<[ColumnarValue; 2]> {
        let pattern = match pattern {
            ColumnarValue::Scalar(value) => {
                ColumnarValue::Scalar(self.rewrite_scalar_pattern(value)?)
            }
            ColumnarValue::Array(values) => {
                source = source.cast_to(&DataType::LargeUtf8, None)?;
                let values = cast(&values, &DataType::LargeUtf8)?;
                let values = values
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        datafusion_common::internal_datafusion_err!("expected string LIKE patterns")
                    })?;
                let mut builder = LargeStringBuilder::new();
                let mut buffer = String::new();
                for (row, pattern) in values.iter().enumerate() {
                    if source_nulls.is_some_and(|nulls| nulls.is_null(row)) {
                        builder.append_null();
                    } else if let Some(pattern) = pattern {
                        rewrite_like_into_string(&mut buffer, pattern, self.escape)?;
                        builder.append_value(&buffer);
                    } else {
                        builder.append_null();
                    }
                }
                ColumnarValue::Array(Arc::new(builder.finish()))
            }
        };
        // Dynamic patterns use 64-bit scratch offsets even when rewriting
        // expands their bytes. Arrow requires equal representations, so widen
        // the source for that call too. Scalar patterns retain scalar dispatch.
        let pattern = match pattern {
            ColumnarValue::Scalar(value) => {
                ColumnarValue::Scalar(value.cast_to(&source.data_type())?)
            }
            pattern => pattern,
        };
        Ok([source, pattern])
    }
}

impl ScalarUDFImpl for SparkLike {
    fn name(&self) -> &str {
        "spark_like"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("spark_like requires 2 arguments");
        }
        Ok(DataType::Boolean)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [source, pattern] = arg_types else {
            return plan_err!("spark_like requires 2 arguments");
        };
        let data_type = like_coercion(source, pattern).ok_or_else(|| {
            datafusion_common::plan_datafusion_err!(
                "There isn't a common type to coerce {source} and {pattern} in LIKE expression"
            )
        })?;
        Ok(vec![data_type.clone(), data_type])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.return_type(
            &args
                .arg_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Boolean,
            args.arg_fields.iter().any(|f| f.is_nullable()),
        )))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn short_circuits_on_null_first_arg(&self) -> bool {
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        Some((args.iter().take(1).collect(), args.iter().skip(1).collect()))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // Known NULL operands precede pattern compilation. Column NULLs retain
        // the literal-pattern initialization check in the mandatory Spark analyzer.
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
        {
            Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Boolean(
                None,
            ))))
        } else {
            Ok(ExprSimplifyResult::Original(args))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [source, pattern] =
            datafusion_common::utils::take_function_args(self.name(), args.args)?;
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(0))));
        }
        let source_nulls = match &source {
            ColumnarValue::Scalar(value) if value.is_null() => {
                return Ok(if matches!(pattern, ColumnarValue::Array(_)) {
                    ColumnarValue::Array(Arc::new(BooleanArray::new_null(args.number_rows)))
                } else {
                    ColumnarValue::Scalar(ScalarValue::Boolean(None))
                });
            }
            ColumnarValue::Array(values) => values.logical_nulls(),
            _ => None,
        };
        if source_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.null_count() == args.number_rows)
        {
            return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
                args.number_rows,
            ))));
        }
        let [source, pattern] = self.native_like_args(source, pattern, source_nulls.as_ref())?;
        apply_cmp(Operator::LikeMatch, &source, &pattern)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, StringArray, StringViewArray};
    use datafusion_common::config::ConfigOptions;

    fn invoke(source: ColumnarValue, pattern: ColumnarValue, rows: usize) -> Result<ColumnarValue> {
        invoke_as(SparkLike::new('!'), source, pattern, rows)
    }

    fn invoke_as(
        function: SparkLike,
        source: ColumnarValue,
        pattern: ColumnarValue,
        rows: usize,
    ) -> Result<ColumnarValue> {
        let args = vec![source, pattern];
        let arg_fields = args
            .iter()
            .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
            .collect();
        function.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: rows,
            return_field: Arc::new(Field::new("r", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[test]
    fn local_consumption_marker_does_not_disable_runtime_validation() -> Result<()> {
        let error = invoke_as(
            SparkLike::new('!').after_local_evaluation(),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".into()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a!b".into()))),
            1,
        )
        .err()
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("expected invalid LIKE pattern")
        })?;
        assert!(
            error
                .to_string()
                .contains("[INVALID_FORMAT.ESC_IN_THE_MIDDLE]")
        );
        Ok(())
    }

    #[test]
    fn scalar_translation_is_wide_before_native_casts() -> Result<()> {
        let function = SparkLike::new('!');
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let value = ScalarValue::Utf8(Some("a\\b".into())).cast_to(&data_type)?;
            let rewritten = function.rewrite_scalar_pattern(value.clone())?;
            assert_eq!(rewritten, ScalarValue::LargeUtf8(Some("a\\\\b".into())));
            assert_eq!(rewritten.to_array()?.data_type(), &DataType::LargeUtf8);
            let null = ScalarValue::Utf8(None).cast_to(&data_type)?;
            assert_eq!(
                function.rewrite_scalar_pattern(null.clone())?,
                ScalarValue::LargeUtf8(None)
            );
            let result = invoke(ColumnarValue::Scalar(value), ColumnarValue::Scalar(null), 1)?;
            let ColumnarValue::Scalar(result) = result else {
                panic!("scalar LIKE operands must retain scalar dispatch");
            };
            assert_eq!(result, ScalarValue::Boolean(None));
            let invalid = ScalarValue::Utf8(Some("a!b".into())).cast_to(&data_type)?;
            assert!(
                function
                    .rewrite_scalar_pattern(invalid)
                    .unwrap_err()
                    .to_string()
                    .contains("[INVALID_FORMAT.ESC_IN_THE_MIDDLE]")
            );
        }
        assert_eq!(
            function.rewrite_scalar_pattern(ScalarValue::Null)?,
            ScalarValue::LargeUtf8(None)
        );
        Ok(())
    }

    #[test]
    fn dynamic_patterns_keep_large_offsets_for_native_matching() -> Result<()> {
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            Some("hidden"),
            None,
            Some("a\\b"),
            Some("a%b"),
            Some("a_b"),
            Some("nomatch"),
            Some("nonnull"),
            Some("hidden"),
        ]));
        let pattern: ArrayRef = Arc::new(StringArray::from(vec![
            Some("hidden"),
            Some("bad!q"),
            Some("a\\b"),
            Some("a!%b"),
            Some("a!_b"),
            Some("x%"),
            None,
            Some("hidden"),
        ]));
        let function = SparkLike::new('!');
        for source_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            for pattern_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                let source = cast(&source, &source_type)?.slice(1, 6);
                let pattern = cast(&pattern, &pattern_type)?.slice(1, 6);
                let [left, right] = function.native_like_args(
                    ColumnarValue::Array(source.clone()),
                    ColumnarValue::Array(pattern.clone()),
                    source.logical_nulls().as_ref(),
                )?;
                // Small values still expose an accidental i32 scratch path.
                assert_eq!(left.data_type(), DataType::LargeUtf8);
                assert_eq!(right.data_type(), DataType::LargeUtf8);
                let result = invoke(
                    ColumnarValue::Array(source),
                    ColumnarValue::Array(pattern),
                    6,
                )?
                .into_array(6)?;
                let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert_eq!(
                    result.iter().collect::<Vec<_>>(),
                    [None, Some(true), Some(true), Some(true), Some(false), None]
                );
            }
        }
        Ok(())
    }

    #[test]
    fn wide_array_preparation_preserves_scalar_dispatch_and_errors() -> Result<()> {
        let function = SparkLike::new('!');
        let source: ArrayRef = Arc::new(StringArray::from(vec![Some("a\\b"), None]));
        let pattern: ArrayRef = Arc::new(StringArray::from(vec![Some("a\\b"), None]));
        let invalid: ArrayRef = Arc::new(StringArray::from(vec![Some("a!b"), None]));
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let scalar = ScalarValue::Utf8(Some("a\\b".into())).cast_to(&data_type)?;
            let array = ColumnarValue::Array(cast(&source, &data_type)?);
            let [left, right] = function.native_like_args(
                array.clone(),
                ColumnarValue::Scalar(scalar.clone()),
                None,
            )?;
            assert_eq!(left.data_type(), data_type);
            assert_eq!(right.data_type(), data_type);
            assert!(matches!(right, ColumnarValue::Scalar(_)));
            let [left, right] = function.native_like_args(
                ColumnarValue::Scalar(scalar.clone()),
                ColumnarValue::Array(cast(&pattern, &data_type)?),
                None,
            )?;
            assert!(matches!(left, ColumnarValue::Scalar(_)));
            assert_eq!(left.data_type(), DataType::LargeUtf8);
            assert_eq!(right.data_type(), DataType::LargeUtf8);
            for (source, pattern) in [
                (array, ColumnarValue::Scalar(scalar.clone())),
                (
                    ColumnarValue::Scalar(scalar.clone()),
                    ColumnarValue::Array(cast(&pattern, &data_type)?),
                ),
            ] {
                let result = invoke(source, pattern, 2)?.into_array(2)?;
                let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert_eq!(result.iter().collect::<Vec<_>>(), [Some(true), None]);
            }
            let error = invoke(
                ColumnarValue::Scalar(scalar),
                ColumnarValue::Array(cast(&invalid, &data_type)?),
                2,
            )
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("[INVALID_FORMAT.ESC_IN_THE_MIDDLE]")
            );
        }
        Ok(())
    }

    #[test]
    fn sliced_view_patterns_validate_only_non_null_source_pairs() -> Result<()> {
        let source: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("hidden"),
            None,
            Some("a%b"),
            Some("a_b"),
            Some("a!b"),
            Some("hidden"),
        ]));
        let pattern: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("bad!x"),
            Some("bad!x"),
            Some("a!%b"),
            Some("a!_b"),
            Some("a!!b"),
            Some("bad!x"),
        ]));
        let result = invoke(
            ColumnarValue::Array(source.slice(1, 4)),
            ColumnarValue::Array(pattern.slice(1, 4)),
            4,
        )?
        .into_array(4)?;
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!("expected Boolean LIKE result")
            })?;
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            [None, Some(true), Some(true), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn direct_null_and_empty_calls_skip_scalar_pattern_validation() -> Result<()> {
        let invalid = ColumnarValue::Scalar(ScalarValue::Utf8(Some("a!b".into())));
        let null = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        assert_eq!(
            invoke(null, invalid.clone(), 1)?
                .into_array(1)?
                .null_count(),
            1
        );
        let empty = ColumnarValue::Array(Arc::new(StringArray::from(Vec::<Option<&str>>::new())));
        assert_eq!(invoke(empty, invalid.clone(), 0)?.into_array(0)?.len(), 0);
        let nulls = ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>, None])));
        assert_eq!(invoke(nulls, invalid, 2)?.into_array(2)?.null_count(), 2);
        let error = invoke(
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".into()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a!b".into()))),
            1,
        )
        .err()
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("expected invalid LIKE pattern")
        })?;
        assert!(
            error
                .to_string()
                .contains("[INVALID_FORMAT.ESC_IN_THE_MIDDLE]")
        );
        Ok(())
    }
}
