// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
//! Spark substring preserves character positions for strings and Java INT wrapping for binary ranges.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, GenericBinaryBuilder, Int64Array, MAX_INLINE_VIEW_LEN,
    OffsetSizeTrait, StringViewArray, StringViewBuilder, new_null_array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_int64_array;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDFImpl, LambdaParametersProgress, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, ValueOrLambda, Volatility,
};
use datafusion_spark::function::string::substring::SparkSubstring as NativeSparkSubstring;

use crate::functions_nested_utils::{evaluate_lambdas_until_null, scatter_active_rows};
use crate::functions_utils::make_scalar_function;

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct SparkSubstring {
    inner: NativeSparkSubstring,
    logical_arguments: bool,
    known_null_argument: bool,
    async_arguments: bool,
}

impl SparkSubstring {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_logical_arguments(mut self) -> Self {
        self.logical_arguments = true;
        self
    }

    pub fn has_logical_arguments(&self) -> bool {
        self.logical_arguments
    }

    pub fn with_known_null_argument(mut self, known_null: bool) -> Self {
        self.known_null_argument = known_null;
        self
    }

    pub fn with_async_arguments(mut self, asynchronous: bool) -> Self {
        self.async_arguments = asynchronous;
        self
    }

    pub fn closed_argument_bodies(expr: &Expr) -> Result<Option<[&Expr; 3]>> {
        let Expr::HigherOrderFunction(function) = expr else {
            return Ok(None);
        };
        if !(function.func.inner().as_ref() as &dyn std::any::Any).is::<Self>() {
            return Ok(None);
        }
        let [
            Expr::Lambda(source),
            Expr::Lambda(position),
            Expr::Lambda(length),
        ] = function.args.as_slice()
        else {
            return Ok(None);
        };
        for lambda in [source, position, length] {
            let [parameter] = lambda.params.as_slice() else {
                return Ok(None);
            };
            if lambda.body.exists(|node| {
                Ok(matches!(node, Expr::LambdaVariable(variable) if &variable.name == parameter))
            })? {
                return Ok(None);
            }
        }
        Ok(Some([&source.body, &position.body, &length.body]))
    }
}

impl ScalarUDFImpl for SparkSubstring {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn short_circuits_on_any_null_argument(&self) -> bool {
        self.logical_arguments && !self.async_arguments
    }

    fn conditional_arguments<'a>(
        &self,
        _args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        // Spark extracts shared arguments before runtime NULL selection.
        None
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if self.logical_arguments
            && (self.known_null_argument
                || args
                    .iter()
                    .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null())))
        {
            let fields = args
                .iter()
                .map(|arg| arg.to_field(info.schema().as_ref()).map(|(_, field)| field))
                .collect::<Result<Vec<_>>>()?;
            let scalar_arguments = args
                .iter()
                .map(|arg| match arg {
                    Expr::Literal(value, _) => Some(value),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let field = ScalarUDFImpl::return_field_from_args(
                self,
                ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &scalar_arguments,
                },
            )?;
            let metadata = (!field.metadata().is_empty()).then(|| {
                datafusion_common::metadata::FieldMetadata::from(field.metadata().clone())
            });
            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::try_new_null(field.data_type())?,
                metadata,
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() == 2 {
            args.args
                .push(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                    i32::MAX.into(),
                ))));
            args.arg_fields
                .push(Arc::new(Field::new("length", DataType::Int64, false)));
        }
        if let [
            value,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(position))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(length))),
        ] = args.args.as_slice()
            && value.data_type() == DataType::Utf8View
            && *position == 1
            && *length >= 0
        {
            if *length <= i64::from(MAX_INLINE_VIEW_LEN)
                && let ColumnarValue::Array(array) = value
                && let Some(prefixes) =
                    inline_ascii_prefixes(array.as_string_view(), *length as usize)
            {
                return Ok(ColumnarValue::Array(Arc::new(prefixes)));
            }
            // A prefix preserves the view type and avoids adding to a possibly large buffer offset.
            return datafusion_functions::unicode::substr().invoke_with_args(args);
        }
        if !args.args.first().is_some_and(|value| {
            matches!(
                value.data_type(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            )
        }) {
            return self.inner.invoke_with_args(args);
        }
        make_scalar_function(
            |arrays| {
                let [values, positions, lengths] = take_function_args("substring", arrays)?;
                let positions = as_int64_array(positions)?;
                let lengths = as_int64_array(lengths)?;
                match values.data_type() {
                    DataType::Binary => binary_substring::<i32>(
                        values.as_binary::<i32>().iter(),
                        positions,
                        lengths,
                    ),
                    DataType::LargeBinary => binary_substring::<i64>(
                        values.as_binary::<i64>().iter(),
                        positions,
                        lengths,
                    ),
                    DataType::BinaryView => {
                        let output = binary_substring::<i64>(
                            values.as_binary_view().iter(),
                            positions,
                            lengths,
                        )?;
                        Ok(cast(output.as_ref(), &DataType::BinaryView)?)
                    }
                    other => exec_err!("substring does not accept binary type {other}"),
                }
            },
            vec![],
        )(&args.args)
    }
}

fn inline_ascii_prefixes(array: &StringViewArray, length: usize) -> Option<StringViewArray> {
    let mut result = StringViewBuilder::with_capacity(array.len());
    for value in array {
        match value {
            Some(value) => {
                // Only the prefix must be ASCII for byte and character counts to agree.
                let prefix = value
                    .get(..length.min(value.len()))
                    .filter(|value| value.is_ascii())?;
                result.append_value(prefix);
            }
            None => result.append_null(),
        }
    }
    Some(result.finish())
}

impl HigherOrderUDFImpl for SparkSubstring {
    fn name(&self) -> &str {
        "spark_substring_lazy"
    }

    fn signature(&self) -> &HigherOrderSignature {
        static SIGNATURE: std::sync::LazyLock<HigherOrderSignature> =
            std::sync::LazyLock::new(|| HigherOrderSignature::variadic_any(Volatility::Immutable));
        &SIGNATURE
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        check_lazy_arguments(fields)?;
        let dummy = Arc::new(Field::new("", DataType::Null, true));
        Ok(LambdaParametersProgress::Complete(
            fields.iter().map(|_| vec![Arc::clone(&dummy)]).collect(),
        ))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        check_lazy_arguments(args.arg_fields)?;
        let fields = args
            .arg_fields
            .iter()
            .map(|argument| match argument {
                ValueOrLambda::Lambda(field) => Arc::clone(field),
                ValueOrLambda::Value(_) => unreachable!("checked lambda arguments"),
            })
            .collect::<Vec<_>>();
        ScalarUDFImpl::return_field_from_args(
            self,
            ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: args.scalar_arguments,
            },
        )
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        check_lazy_arguments(&args.args)?;
        let lambdas = args
            .args
            .iter()
            .map(|argument| match argument {
                ValueOrLambda::Lambda(lambda) => lambda,
                ValueOrLambda::Value(_) => unreachable!("checked lambda arguments"),
            })
            .collect::<Vec<_>>();
        let fields = args
            .arg_fields
            .iter()
            .map(|argument| match argument {
                ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
                ValueOrLambda::Value(_) => exec_err!("substring expected lambda fields"),
            })
            .collect::<Result<Vec<_>>>()?;
        // Each child runs once, only on rows whose previous children were non-NULL.
        let (values, active_rows) = evaluate_lambdas_until_null(&lambdas, args.number_rows)?;
        if active_rows.is_empty() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                args.number_rows,
            )));
        }
        let result = ScalarUDFImpl::invoke_with_args(
            self,
            ScalarFunctionArgs {
                args: values.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields: fields,
                number_rows: active_rows.len(),
                return_field: Arc::clone(&args.return_field),
                config_options: Arc::clone(&args.config_options),
            },
        )?
        .into_array(active_rows.len())?;
        Ok(ColumnarValue::Array(scatter_active_rows(
            result,
            &active_rows,
            args.number_rows,
        )?))
    }
}

fn check_lazy_arguments<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<()> {
    if args.len() != 3
        || args
            .iter()
            .any(|argument| matches!(argument, ValueOrLambda::Value(_)))
    {
        return exec_err!("substring requires three internal lambda arguments");
    }
    Ok(())
}

fn binary_substring<'a, O: OffsetSizeTrait>(
    values: impl Iterator<Item = Option<&'a [u8]>>,
    positions: &Int64Array,
    lengths: &Int64Array,
) -> Result<ArrayRef> {
    let mut builder = GenericBinaryBuilder::<O>::with_capacity(positions.len(), 0);
    let mut padded = Vec::new();
    for ((value, position), length) in values.zip(positions.iter()).zip(lengths.iter()) {
        let (Some(value), Some(position), Some(length)) = (value, position, length) else {
            builder.append_null();
            continue;
        };
        let size = i32::try_from(value.len())
            .map_err(|_| exec_datafusion_err!("substring binary value exceeds INT length"))?;
        let position = i32::try_from(position)
            .map_err(|_| exec_datafusion_err!("substring position exceeds INT range"))?;
        let length = i32::try_from(length)
            .map_err(|_| exec_datafusion_err!("substring length exceeds INT range"))?;
        if position > size {
            builder.append_value([]);
            continue;
        }

        // Apache Spark ByteArray.subStringSQL uses Java INT arithmetic before clamping.
        // Range arithmetic wraps even for negative lengths.
        let start = if position > 0 {
            position - 1
        } else if position < 0 {
            size.wrapping_add(position)
        } else {
            0
        };
        let end = if size.wrapping_sub(start) < length {
            size
        } else {
            start.wrapping_add(length)
        };
        let start = start.max(0);
        if start >= end {
            builder.append_value([]);
            continue;
        }
        let (start, end) = (start as usize, end as usize);
        let output_len = end - start;
        if builder
            .values_slice()
            .len()
            .checked_add(output_len)
            .and_then(O::from_usize)
            .is_none()
        {
            return exec_err!("substring binary output exceeds Arrow offset capacity");
        }
        if end <= value.len() {
            builder.append_value(&value[start..end]);
        } else {
            // Java Arrays.copyOfRange pads the requested range beyond the input with zeros.
            padded.clear();
            padded.extend_from_slice(&value[start..]);
            padded.resize(output_len, 0);
            builder.append_value(&padded);
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, BinaryViewArray, LargeBinaryArray};
    use datafusion_common::config::ConfigOptions;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    fn arguments(values: Vec<ColumnarValue>, rows: usize) -> Result<ScalarFunctionArgs> {
        let fields = values
            .iter()
            .map(|value| Arc::new(Field::new("", value.data_type(), true)))
            .collect::<Vec<_>>();
        let scalars = values
            .iter()
            .map(|value| match value {
                ColumnarValue::Scalar(value) => Some(value),
                ColumnarValue::Array(_) => None,
            })
            .collect::<Vec<_>>();
        let return_field =
            NativeSparkSubstring::default().return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            })?;
        Ok(ScalarFunctionArgs {
            args: values,
            arg_fields: fields,
            number_rows: rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn assert_same(actual: Result<ColumnarValue>, expected: Result<ColumnarValue>) -> TestResult {
        match (actual, expected) {
            (Ok(ColumnarValue::Scalar(actual)), Ok(ColumnarValue::Scalar(expected))) => {
                assert_eq!(actual.data_type(), expected.data_type());
                assert_eq!(actual, expected);
            }
            (Ok(ColumnarValue::Array(actual)), Ok(ColumnarValue::Array(expected))) => {
                assert_eq!(actual.data_type(), expected.data_type());
                assert_eq!(actual.as_ref(), expected.as_ref());
            }
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            (actual, expected) => {
                return Err(
                    format!("substring result forms differ: {actual:?}, {expected:?}").into(),
                );
            }
        }
        Ok(())
    }

    #[test]
    fn substring_row_path_matches_existing_string_kernel() -> TestResult {
        let inputs: Vec<(Option<String>, Option<i64>, Option<i64>)> = serde_json::from_str(
            include_str!("../../../tests/fixtures/substring-row-cost-1388-inputs.json"),
        )?;
        let wrapper = SparkSubstring::new();
        let original = NativeSparkSubstring::default();
        for (value, position, length) in inputs {
            for storage in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                let values = [
                    ScalarValue::try_from_array(
                        cast(&StringViewArray::from(vec![value.as_deref()]), &storage)?.as_ref(),
                        0,
                    )?,
                    ScalarValue::Int64(position),
                    ScalarValue::Int64(length),
                ];
                // Every scalar/column combination, including all-scalar and zero-row batches.
                for mask in 0..8 {
                    for rows in [0, 3] {
                        let args = values
                            .iter()
                            .enumerate()
                            .map(|(index, value)| {
                                if mask & (1 << index) == 0 {
                                    Ok(ColumnarValue::Scalar(value.clone()))
                                } else {
                                    Ok(ColumnarValue::Array(value.to_array_of_size(rows)?))
                                }
                            })
                            .collect::<Result<Vec<_>>>()?;
                        assert_same(
                            ScalarUDFImpl::invoke_with_args(
                                &wrapper,
                                arguments(args.clone(), rows)?,
                            ),
                            original.invoke_with_args(arguments(args, rows)?),
                        )?;
                    }
                }
                // The wrapper's omitted length is INT_MAX, including scalar calls.
                for rows in [0, 3] {
                    for source in [
                        ColumnarValue::Scalar(values[0].clone()),
                        ColumnarValue::Array(values[0].to_array_of_size(rows)?),
                    ] {
                        let short = vec![source, ColumnarValue::Scalar(values[1].clone())];
                        let mut explicit = short.clone();
                        explicit.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                            i32::MAX.into(),
                        ))));
                        assert_same(
                            ScalarUDFImpl::invoke_with_args(&wrapper, arguments(short, rows)?),
                            original.invoke_with_args(arguments(explicit, rows)?),
                        )?;
                    }
                }
            }
        }
        // Unsupported storage and bound types retain the original failure.
        for values in [
            vec![
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(12)),
            ],
            vec![
                ScalarValue::Utf8View(Some("text".into())),
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int64(Some(12)),
            ],
        ] {
            let values = values
                .into_iter()
                .map(ColumnarValue::Scalar)
                .collect::<Vec<_>>();
            assert_same(
                ScalarUDFImpl::invoke_with_args(&wrapper, arguments(values.clone(), 1)?),
                original.invoke_with_args(arguments(values, 1)?),
            )?;
        }
        Ok(())
    }

    #[test]
    fn substring_delegates_only_view_prefixes() -> TestResult {
        let source: ArrayRef = Arc::new(StringViewArray::from(vec![
            "abcdefghijklmnopqrstuvwxyz0123456789",
        ]));
        for position in [1, 2] {
            let result = ScalarUDFImpl::invoke_with_args(
                &SparkSubstring::new(),
                arguments(
                    vec![
                        ColumnarValue::Array(source.clone()),
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(position))),
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
                    ],
                    1,
                )?,
            )?
            .into_array(1)?;
            // Out-of-line prefixes use the native shared-buffer view. Other positions
            // retain the old copying kernel, including valid offsets near u32::MAX.
            let shared = result.as_string_view().data_buffers()[0].as_ptr()
                == source.as_string_view().data_buffers()[0].as_ptr();
            assert_eq!(shared, position == 1);
        }
        Ok(())
    }

    #[test]
    fn inline_prefix_agrees_with_native_character_bounds() -> TestResult {
        for values in [
            vec![Some(""), Some("x"), None, Some("abcdefghijkl中文")],
            vec![Some("abcé光def"), Some("é"), None],
        ] {
            let source = Arc::new(StringViewArray::from(values));
            for length in [0, 1, 4, i64::from(MAX_INLINE_VIEW_LEN)] {
                let args = || {
                    arguments(
                        vec![
                            ColumnarValue::Array(source.clone()),
                            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
                            ColumnarValue::Scalar(ScalarValue::Int64(Some(length))),
                        ],
                        source.len(),
                    )
                };
                assert_same(
                    ScalarUDFImpl::invoke_with_args(&SparkSubstring::new(), args()?),
                    datafusion_functions::unicode::substr().invoke_with_args(args()?),
                )?;
            }
        }
        Ok(())
    }

    #[test]
    fn substring_binary_keeps_existing_adapter() -> TestResult {
        let values = vec![
            Some(b"abcdefghijklmnop".as_slice()),
            None,
            Some(b"".as_slice()),
        ];
        let arrays: [ArrayRef; 3] = [
            Arc::new(BinaryArray::from(values.clone())),
            Arc::new(LargeBinaryArray::from(values.clone())),
            Arc::new(BinaryViewArray::from(values)),
        ];
        for source in arrays {
            for (position, length) in [
                (1, 12),
                (0, 2),
                (-2, 1),
                (1, -1),
                (i64::MAX, 1),
                (1, i64::MAX),
            ] {
                for mask in 0..8 {
                    let values = [
                        ScalarValue::try_from_array(source.as_ref(), 0)?,
                        ScalarValue::Int64(Some(position)),
                        ScalarValue::Int64(Some(length)),
                    ];
                    let args = values
                        .iter()
                        .enumerate()
                        .map(|(index, value)| {
                            if mask & (1 << index) == 0 {
                                Ok(ColumnarValue::Scalar(value.clone()))
                            } else if index == 0 {
                                Ok(ColumnarValue::Array(source.clone()))
                            } else {
                                Ok(ColumnarValue::Array(value.to_array_of_size(3)?))
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    // Invoke the unchanged binary helper through the old scalar expansion.
                    let expected = make_scalar_function(
                        |arrays| {
                            let source = &arrays[0];
                            let positions = as_int64_array(&arrays[1])?;
                            let lengths = as_int64_array(&arrays[2])?;
                            match source.data_type() {
                                DataType::Binary => binary_substring::<i32>(
                                    source.as_binary::<i32>().iter(),
                                    positions,
                                    lengths,
                                ),
                                DataType::LargeBinary => binary_substring::<i64>(
                                    source.as_binary::<i64>().iter(),
                                    positions,
                                    lengths,
                                ),
                                DataType::BinaryView => Ok(cast(
                                    binary_substring::<i64>(
                                        source.as_binary_view().iter(),
                                        positions,
                                        lengths,
                                    )?
                                    .as_ref(),
                                    &DataType::BinaryView,
                                )?),
                                other => exec_err!("unexpected test storage {other}"),
                            }
                        },
                        vec![],
                    )(&args);
                    assert_same(
                        ScalarUDFImpl::invoke_with_args(
                            &SparkSubstring::new(),
                            arguments(args, 3)?,
                        ),
                        expected,
                    )?;
                }
            }
        }
        Ok(())
    }
}
