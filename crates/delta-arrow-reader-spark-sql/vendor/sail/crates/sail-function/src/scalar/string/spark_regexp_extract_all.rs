// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericStringArray, Int64Array, ListArray, ListBuilder, OffsetSizeTrait,
    StringBuilder, StringViewArray, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::tree_node::TreeNode;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::function::Hint;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Expr, ExprSchemable, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, ValueOrLambda,
};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};
use regex::Regex;

use crate::error::{generic_exec_err, generic_internal_err, unsupported_data_types_exec_err};
use crate::functions_nested_utils::{
    evaluate_lambdas_until_null, opt_downcast_arg, scatter_active_rows,
};
use crate::functions_utils::{StrMemo, make_scalar_function};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkRegexpExtract {
    signature: Signature,
    null_short_circuit: bool,
    known_null_argument: bool,
    check_local_index: bool,
}

impl Default for SparkRegexpExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegexpExtract {
    pub const NAME: &'static str = "regexp_extract";

    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            null_short_circuit: false,
            known_null_argument: false,
            check_local_index: false,
        }
    }

    pub fn with_null_short_circuit(mut self, known_null: bool) -> Self {
        self.null_short_circuit = true;
        self.known_null_argument = known_null;
        self
    }

    pub fn has_null_short_circuit(&self) -> bool {
        self.null_short_circuit
    }

    pub fn has_known_null_argument(&self) -> bool {
        self.known_null_argument
    }

    pub fn with_local_index_check(mut self, required: bool) -> Self {
        self.check_local_index = required;
        self
    }

    pub fn requires_local_index_check(&self) -> bool {
        self.null_short_circuit && self.check_local_index && !self.known_null_argument
    }

    pub fn closed_argument_bodies(expr: &Expr) -> Result<Option<[&Expr; 3]>> {
        let Expr::HigherOrderFunction(function) = expr else {
            return Ok(None);
        };
        if !(function.func.inner().as_ref() as &dyn std::any::Any)
            .downcast_ref::<Self>()
            .is_some_and(Self::has_null_short_circuit)
        {
            return Ok(None);
        }
        let [
            Expr::Lambda(source),
            Expr::Lambda(pattern),
            Expr::Lambda(index),
        ] = function.args.as_slice()
        else {
            return Ok(None);
        };
        for lambda in [source, pattern, index] {
            let [parameter] = lambda.params.as_slice() else {
                return Ok(None);
            };
            if lambda.body.exists(|node| {
                Ok(matches!(node, Expr::LambdaVariable(variable) if &variable.name == parameter))
            })? {
                return Ok(None);
            }
        }
        Ok(Some([&source.body, &pattern.body, &index.body]))
    }
}

impl ScalarUDFImpl for SparkRegexpExtract {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(Self::NAME, DataType::Utf8, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_regexp_extract_types(Self::NAME, arg_types)
    }

    fn short_circuits_on_any_null_argument(&self) -> bool {
        self.null_short_circuit
    }

    fn conditional_arguments<'a>(
        &self,
        _args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        // Keep Spark's existing shared-argument extraction before row selection.
        None
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if self.null_short_circuit
            && (self.known_null_argument
                || args
                    .iter()
                    .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null())))
        {
            // Validate the native signature before pruning a known NULL result.
            let types = args
                .iter()
                .map(|arg| arg.get_type(info.schema().as_ref()))
                .collect::<Result<Vec<_>>>()?;
            let _ = self.coerce_types(&types)?;
            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::Utf8(None),
                None,
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;
        if args.len() == 2 {
            args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
        }
        make_scalar_function(
            regexp_extract_inner,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::AcceptsSingular],
        )(&args)
    }
}

impl HigherOrderUDFImpl for SparkRegexpExtract {
    fn name(&self) -> &str {
        "spark_regexp_extract_lazy"
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
        check_extract_arguments(fields)?;
        let dummy = Arc::new(Field::new("", DataType::Null, true));
        Ok(LambdaParametersProgress::Complete(
            fields.iter().map(|_| vec![Arc::clone(&dummy)]).collect(),
        ))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        check_extract_arguments(args.arg_fields)?;
        let fields = args
            .arg_fields
            .iter()
            .map(|argument| match argument {
                ValueOrLambda::Lambda(field) => Arc::clone(field),
                ValueOrLambda::Value(_) => unreachable!("checked lambda arguments"),
            })
            .collect::<Vec<_>>();
        ScalarUDFImpl::coerce_types(
            self,
            &fields
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        ScalarUDFImpl::return_field_from_args(
            self,
            ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: args.scalar_arguments,
            },
        )
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        check_extract_arguments(&args.args)?;
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
                ValueOrLambda::Value(_) => exec_err!("regexp_extract expected lambda fields"),
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

fn check_extract_arguments<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<()> {
    if args.len() != 3
        || args
            .iter()
            .any(|argument| matches!(argument, ValueOrLambda::Value(_)))
    {
        return exec_err!("regexp_extract requires three internal lambda arguments");
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegexpExtractAll {
    signature: Signature,
}

impl Default for SparkRegexpExtractAll {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegexpExtractAll {
    pub const NAME: &'static str = "regexp_extract_all";

    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkRegexpExtractAll {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Utf8,
            true,
        ))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_regexp_extract_types(Self::NAME, arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;
        if args.len() == 2 {
            args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
        }
        make_scalar_function(
            regexp_extract_all_inner,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::AcceptsSingular],
        )(&args)
    }
}

fn regexp_extract_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [values_arr, pattern_arr, idx_arr] = take_function_args(SparkRegexpExtract::NAME, args)?;
    let values = string_array_like(values_arr);
    let pattern = string_array_like(pattern_arr);
    let idx = opt_downcast_arg!(idx_arr, Int64Array);

    match (values.as_deref(), pattern.as_deref(), idx.as_ref()) {
        (Some(values), Some(pattern), Some(idx)) => {
            let mut builder = StringBuilder::new();
            let mut regex_memo = StrMemo::new();
            for i in 0..values.len_() {
                let pattern_index = if pattern.len_() == 1 { 0 } else { i };
                let idx_index = if idx.len() == 1 { 0 } else { i };
                if values.is_null_(i) || pattern.is_null_(pattern_index) || idx.is_null(idx_index) {
                    builder.append_null();
                } else {
                    // A singular pattern can still belong to a NULL row or empty batch.
                    let re = regex_memo
                        .get_or_try_insert_ref(pattern.value_(pattern_index), |p| {
                            parse_regex(SparkRegexpExtract::NAME, p)
                        })?;
                    builder.append_value(extract_first_match(
                        SparkRegexpExtract::NAME,
                        values.value_(i),
                        re,
                        idx.value(idx_index),
                    )?);
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(generic_internal_err(
            SparkRegexpExtract::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

fn regexp_extract_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::LargeUtf8 => regexp_extract_all_downcast::<i64>(args),
        _ => regexp_extract_all_downcast::<i32>(args),
    }
}

fn regexp_extract_all_downcast<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [values_arr, pattern_arr, idx_arr] = take_function_args(SparkRegexpExtractAll::NAME, args)?;
    let values = values_arr.as_any().downcast_ref::<GenericStringArray<O>>();
    let pattern = string_array_like(pattern_arr);
    let idx = opt_downcast_arg!(idx_arr, Int64Array);

    match (values, pattern.as_deref(), idx.as_ref()) {
        (Some(values), Some(pattern), Some(idx)) => {
            let pattern_len = pattern.len_();
            let idx_len = idx.len();

            let pattern_scalar_opt = (pattern_len == 1 && pattern.is_valid_(0))
                .then(|| parse_regex(SparkRegexpExtractAll::NAME, pattern.value_(0)))
                .transpose()?;
            let idx_scalar_opt = (idx_len == 1 && idx.is_valid(0)).then(|| idx.value(0));
            let is_pattern_null = pattern_len == 1 && pattern.is_null_(0);
            let is_idx_null = idx_len == 1 && idx.is_null(0);

            let mut builder = ListBuilder::new(StringBuilder::new());
            // Compile each distinct column pattern once per batch, not per row.
            let mut regex_memo = StrMemo::new();
            for i in 0..args[0].len() {
                let pattern_is_null = if pattern_len == 1 {
                    is_pattern_null
                } else {
                    pattern.is_null_(i)
                };
                let idx_is_null = if idx_len == 1 {
                    is_idx_null
                } else {
                    idx.is_null(i)
                };

                if pattern_is_null || idx_is_null || values.is_null(i) {
                    builder.append_null();
                } else {
                    let re = regex_memo.resolve(
                        pattern_scalar_opt.as_ref(),
                        || pattern.value_(i),
                        |p| parse_regex(SparkRegexpExtractAll::NAME, p),
                    )?;
                    let group_idx = idx_scalar_opt.unwrap_or_else(|| idx.value(i));
                    let matches = extract_all_matches(
                        SparkRegexpExtractAll::NAME,
                        values.value(i),
                        re,
                        group_idx,
                    )?;
                    builder.append_value(matches);
                }
            }
            let array: ListArray = builder.finish();
            Ok(Arc::new(array))
        }
        _ => Err(generic_internal_err(
            SparkRegexpExtractAll::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

fn coerce_regexp_extract_types(
    function_name: &str,
    arg_types: &[DataType],
) -> Result<Vec<DataType>> {
    if !(2..=3).contains(&arg_types.len()) {
        return Err(unsupported_data_types_exec_err(
            function_name,
            "Expected (STRING, STRING) or (STRING, STRING, INT)",
            arg_types,
        ));
    }

    let mut res_types = vec![];
    for i in 0..=1 {
        res_types.push(match arg_types.get(i) {
            Some(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) => {
                Ok(arg_types[i].clone())
            }
            Some(DataType::Null) => Ok(DataType::Utf8),
            _ => Err(unsupported_data_types_exec_err(
                function_name,
                "Expected (STRING, STRING) or (STRING, STRING, INT)",
                arg_types,
            )),
        });
    }
    if arg_types.len() == 3 {
        res_types.push(if arg_types[2].is_null() || arg_types[2].is_integer() {
            Ok(DataType::Int64)
        } else {
            Err(unsupported_data_types_exec_err(
                function_name,
                "Expected (STRING, STRING) or (STRING, STRING, INT)",
                arg_types,
            ))
        });
    }
    res_types.into_iter().collect::<Result<Vec<_>>>()
}

trait StringArrayLike {
    fn len_(&self) -> usize;
    fn is_valid_(&self, i: usize) -> bool;
    fn is_null_(&self, i: usize) -> bool;
    fn value_(&self, i: usize) -> &str;
}

impl<O: OffsetSizeTrait> StringArrayLike for GenericStringArray<O> {
    fn len_(&self) -> usize {
        self.len()
    }
    fn is_valid_(&self, i: usize) -> bool {
        self.is_valid(i)
    }
    fn is_null_(&self, i: usize) -> bool {
        self.is_null(i)
    }
    fn value_(&self, i: usize) -> &str {
        self.value(i)
    }
}

impl StringArrayLike for StringViewArray {
    fn len_(&self) -> usize {
        self.len()
    }
    fn is_valid_(&self, i: usize) -> bool {
        self.is_valid(i)
    }
    fn is_null_(&self, i: usize) -> bool {
        self.is_null(i)
    }
    fn value_(&self, i: usize) -> &str {
        self.value(i)
    }
}

fn string_array_like(array: &ArrayRef) -> Option<Box<dyn StringArrayLike + '_>> {
    if let Some(array) = array.as_any().downcast_ref::<GenericStringArray<i32>>() {
        Some(Box::new(array))
    } else if let Some(array) = array.as_any().downcast_ref::<GenericStringArray<i64>>() {
        Some(Box::new(array))
    } else {
        array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|array| Box::new(array) as Box<dyn StringArrayLike>)
    }
}

impl<T: StringArrayLike + ?Sized> StringArrayLike for &T {
    fn len_(&self) -> usize {
        (**self).len_()
    }
    fn is_valid_(&self, i: usize) -> bool {
        (**self).is_valid_(i)
    }
    fn is_null_(&self, i: usize) -> bool {
        (**self).is_null_(i)
    }
    fn value_(&self, i: usize) -> &str {
        (**self).value_(i)
    }
}

fn parse_regex(function_name: &str, pattern: &str) -> Result<Regex> {
    Regex::new(pattern).map_err(|error| {
        generic_exec_err(function_name, &format!("Invalid regex pattern: {error}"))
    })
}

fn validate_group_idx(function_name: &str, group_idx: i64) -> Result<usize> {
    if group_idx < 0 {
        return Err(generic_exec_err(
            function_name,
            &format!("The group index must be non-negative, but got {group_idx}"),
        ));
    }
    Ok(group_idx as usize)
}

fn extract_capture(
    function_name: &str,
    caps: &regex::Captures<'_>,
    group_idx: i64,
) -> Result<String> {
    let max_group_idx = caps.len().saturating_sub(1);
    if group_idx < 0 || group_idx as u64 > max_group_idx as u64 {
        return Err(generic_exec_err(
            function_name,
            &format!("Expects group index between 0 and {max_group_idx}, but got {group_idx}"),
        ));
    }
    Ok(caps
        .get(group_idx as usize)
        .map(|m| m.as_str())
        .unwrap_or("")
        .to_string())
}

fn extract_first_match(
    function_name: &str,
    value: &str,
    re: &Regex,
    group_idx: i64,
) -> Result<String> {
    match re.captures(value) {
        Some(caps) => extract_capture(function_name, &caps, group_idx),
        None => Ok(String::new()),
    }
}

fn extract_all_matches(
    function_name: &str,
    value: &str,
    re: &Regex,
    group_idx: i64,
) -> Result<Vec<Option<String>>> {
    validate_group_idx(function_name, group_idx)?;
    let mut results = Vec::new();
    for caps in re.captures_iter(value) {
        results.push(Some(extract_capture(function_name, &caps, group_idx)?));
    }
    Ok(results)
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{AsArray, Int64Array, LargeStringArray, ListArray, StringArray};

    use super::*;

    fn strings(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }

    #[test]
    fn extract_preserves_views_slices_and_null_evaluation() -> Result<()> {
        let source = vec![
            Some("padding"),
            Some("prefix-a11b-suffix"),
            Some("abc"),
            Some("b"),
            None,
            Some("a33b"),
            Some("abc"),
            Some("prefix-a22b-suffix"),
            Some("padding"),
        ];
        let pattern = vec![
            Some("padding"),
            Some("a([0-9]+)b(?:)"),
            Some("(z)"),
            Some("(a)?b"),
            Some("["),
            None,
            Some("["),
            Some("a([0-9]+)b(?:)"),
            Some("padding"),
        ];
        let sources: [ArrayRef; 3] = [
            Arc::new(StringArray::from(source.clone())),
            Arc::new(LargeStringArray::from(source.clone())),
            Arc::new(StringViewArray::from(source)),
        ];
        let patterns: [ArrayRef; 3] = [
            Arc::new(StringArray::from(pattern.clone())),
            Arc::new(LargeStringArray::from(pattern.clone())),
            Arc::new(StringViewArray::from(pattern)),
        ];
        let indices: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(1),
            Some(1),
            Some(1),
            None,
            Some(1),
            Some(0),
        ]));
        let expected = StringArray::from(vec![
            Some("11"),
            Some(""),
            Some(""),
            None,
            None,
            None,
            Some("22"),
        ]);
        for source in &sources {
            for pattern in &patterns {
                for batch_size in [1, 32] {
                    for offset in (0..7).step_by(batch_size) {
                        let len = batch_size.min(7 - offset);
                        let args = [
                            source.slice(1 + offset, len),
                            pattern.slice(1 + offset, len),
                            indices.slice(1 + offset, len),
                        ];
                        let output = regexp_extract_inner(&args)?;
                        assert_eq!(output.as_string::<i32>(), &expected.slice(offset, len));
                    }
                }
                let empty = regexp_extract_inner(&[
                    source.slice(0, 0),
                    strings(vec![Some("[")]),
                    Arc::new(Int64Array::from(vec![1])),
                ])?;
                assert!(empty.is_empty());
            }
        }
        for index in [Some(1), None] {
            let output = regexp_extract_inner(&[
                strings(vec![None]),
                strings(vec![Some("[")]),
                Arc::new(Int64Array::from(vec![index])),
            ])?;
            assert!(output.is_null(0));
        }
        let scalar_pattern = regexp_extract_inner(&[
            strings(vec![Some("a11b"), Some("a22b")]),
            strings(vec![Some("a([0-9]+)b")]),
            Arc::new(Int64Array::from(vec![1])),
        ])?;
        assert_eq!(
            scalar_pattern.as_string::<i32>(),
            &StringArray::from(vec!["11", "22"])
        );
        Ok(())
    }

    #[test]
    fn extract_fields_follow_argument_nullability() -> Result<()> {
        let udf = SparkRegexpExtract::new();
        for arity in [2, 3] {
            for nullable_index in 0..=arity {
                let arg_fields = [DataType::Utf8, DataType::Utf8, DataType::Int64]
                    .into_iter()
                    .take(arity)
                    .enumerate()
                    .map(|(index, data_type)| {
                        Arc::new(Field::new("v", data_type, index == nullable_index))
                    })
                    .collect::<Vec<_>>();
                let output = udf.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &arg_fields,
                    scalar_arguments: &vec![None; arity],
                })?;
                assert_eq!(output.data_type(), &DataType::Utf8);
                assert_eq!(output.is_nullable(), nullable_index < arity);
                assert!(output.metadata().is_empty());
            }
        }
        Ok(())
    }

    #[test]
    fn extract_validates_group_range_only_after_a_match() -> Result<()> {
        let re = parse_regex(SparkRegexpExtract::NAME, "(a)")?;
        for index in [-1, 2, i64::from(i32::MAX)] {
            assert_eq!(
                extract_first_match(SparkRegexpExtract::NAME, "b", &re, index)?,
                ""
            );
            let error = extract_first_match(SparkRegexpExtract::NAME, "a", &re, index)
                .expect_err("matched invalid index must fail")
                .to_string();
            assert!(
                error.contains(&format!("between 0 and 1, but got {index}")),
                "{error}"
            );
        }
        assert_eq!(
            extract_first_match(SparkRegexpExtract::NAME, "a", &re, 0)?,
            "a"
        );
        let error = parse_regex(SparkRegexpExtract::NAME, "[")
            .expect_err("invalid pattern must fail")
            .to_string();
        assert!(
            error.contains("Invalid regex pattern") && error.contains("unclosed character class"),
            "{error}"
        );
        Ok(())
    }

    /// A pattern supplied as a column selects the regex per row; null patterns
    /// yield null outputs and an invalid pattern in the column errors.
    #[test]
    fn extract_with_column_pattern() -> Result<()> {
        let values = strings(vec![Some("a11b"), Some("a22b"), Some("xyz"), Some("a33b")]);
        let patterns = strings(vec![
            Some("([0-9]+)"),
            Some("([0-9]+)"),
            None,
            Some("([a-z]+)"),
        ]);
        let idx: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1, 1]));
        let output = regexp_extract_inner(&[values.clone(), patterns, idx.clone()])?;
        let output = output
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string output");
        assert_eq!(output.value(0), "11");
        assert_eq!(output.value(1), "22");
        assert!(output.is_null(2));
        assert_eq!(output.value(3), "a");

        let invalid = strings(vec![Some("("), Some("("), Some("("), Some("(")]);
        assert!(regexp_extract_inner(&[values, invalid, idx]).is_err());
        Ok(())
    }

    #[test]
    fn extract_all_with_column_pattern() -> Result<()> {
        let values = strings(vec![Some("1a2"), Some("3b4"), Some("5c6")]);
        let patterns = strings(vec![Some("[0-9]"), Some("[a-z]"), Some("[0-9]")]);
        let idx: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 0, 0]));
        let output = regexp_extract_all_inner(&[values, patterns, idx])?;
        let output = output
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list output");
        let digits = output.value(0);
        let digits = digits
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string matches");
        assert_eq!(
            (digits.len(), digits.value(0), digits.value(1)),
            (2, "1", "2")
        );
        let letters = output.value(1);
        let letters = letters
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string matches");
        assert_eq!((letters.len(), letters.value(0)), (1, "b"));
        // Row 2 reuses "[0-9]" after row 0 compiled it: the memoized regex
        // must extract exactly as a fresh compile would.
        let reused = output.value(2);
        let reused = reused
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string matches");
        assert_eq!(
            (reused.len(), reused.value(0), reused.value(1)),
            (2, "5", "6")
        );
        Ok(())
    }
}
