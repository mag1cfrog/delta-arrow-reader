// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::cmp::Ordering;
use std::sync::Arc;

use crate::scalar::string::spark_substring::SparkSubstring;
use datafusion::arrow::array::{AsArray, BooleanArray, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::boolean::{is_null, or};
use datafusion::arrow::compute::kernels::concat_elements::concat_elements_dyn;
use datafusion::arrow::compute::kernels::nullif::nullif;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::functions::string::concat::ConcatFunc;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::utils::list_ndims;
use datafusion_common::{Result, ScalarValue, internal_err, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_functions_nested::concat::ArrayConcat;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkConcat {
    signature: Signature,
    ordered_evaluation: bool,
}

impl Default for SparkConcat {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConcat {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![]), TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
            ordered_evaluation: false,
        }
    }

    // C06 runtime selection is installed after logical folding/CSE. Scalar
    // invocation stays eager for Spark's constant and local-relation checks.
    pub fn with_ordered_evaluation(mut self) -> Self {
        self.ordered_evaluation = true;
        self
    }

    pub fn has_ordered_evaluation(&self) -> bool {
        self.ordered_evaluation
    }
}

impl ScalarUDFImpl for SparkConcat {
    fn name(&self) -> &str {
        "spark_concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "{}: `return_type` should not be called; `return_field_from_args` is used instead",
            self.name()
        )
    }

    /// `concat` propagates NULL: the result is null iff any argument is null
    /// (mirrors Spark `Concat.nullable`). Set here rather than in the deprecated
    /// `is_nullable`, which DataFusion no longer consults to derive the field.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let return_type = concat_return_type(&data_types)?;
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), return_type, nullable)))
    }

    /// Plan-time simplifications that mirror Spark's `concat` semantics.
    ///
    /// concat — simplify summary:
    /// ```text
    /// concat(concat(a, b), c)  -> concat(a, b, c)  (R1: CombineConcats — flatten nested)
    /// concat(a, NULL, b)       -> NULL             (R2: null propagation — any null arg)
    /// concat(x)                -> x                (R3: single-arg identity: string/array/binary)
    /// ```
    ///
    /// * **R1** — flatten nested `concat` calls first, so deeply nested calls
    ///   collapse in one bottom-up pass and a NULL bubbled up from an inner call
    ///   is caught by R2. Only flattens when the return type is preserved.
    /// * **R2** - STRING/BINARY `concat` keeps permitted pending constants so
    ///   their required errors survive. Folding-disabled extensions and list
    ///   results retain the existing NULL path. Falls through on a type-invalid
    ///   combo (e.g. array + untyped NULL) so the normal path still raises the error.
    /// * **R3** — single-argument identity for string/array/binary inputs; other
    ///   types fall through so the invoke path can apply Spark coercions.
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // R1: flatten nested concat (Spark's CombineConcats).
        let args = flatten_nested_concats(args, info, self.ordered_evaluation)?;

        // R2: null propagation. Only fold when the arg types form a valid concat —
        // otherwise fall through so the normal path still raises the type error
        // (e.g. array + untyped NULL).
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(scalar, _) if scalar.is_null()))
        {
            let arg_types = args
                .iter()
                .map(|arg| info.get_data_type(arg))
                .collect::<Result<Vec<_>>>()?;
            if let Ok(return_type) = concat_return_type(&arg_types) {
                // STRING/BINARY CONCAT folds permitted constant operands before
                // propagating NULL. ARRAY_CONCAT keeps its existing NULL rule.
                if matches!(
                    return_type,
                    DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Binary
                        | DataType::LargeBinary
                ) {
                    if has_unfolded_constant_argument(&args)? {
                        return Ok(ExprSimplifyResult::Original(args));
                    }
                    // A later NULL preserves runtime work before it. Opaque
                    // extensions keep #883's whole-call native NULL boundary.
                    if self.ordered_evaluation
                        && !matches!(args.first(), Some(Expr::Literal(value, _)) if value.is_null())
                        && !has_folding_disabled_argument(&args)?
                    {
                        return Ok(ExprSimplifyResult::Original(args));
                    }
                }
                if let Ok(null) = ScalarValue::try_from(&return_type) {
                    return Ok(ExprSimplifyResult::Simplified(Expr::Literal(null, None)));
                }
            }
        }

        if args.len() != 1 {
            return Ok(ExprSimplifyResult::Original(args));
        }
        let arg = args.one()?;
        // `FixedSizeList` and `LargeList` are intentionally omitted:
        // `concat_return_type` does not recognize them as array inputs (it falls
        // through to `Utf8`), so the invoke path coerces them to strings. Returning
        // the argument unchanged here would break the type contract advertised by
        // `concat_return_type`. Numeric, timestamp, and other non-string scalars are
        // also excluded so the invoke path can apply Spark-specific coercions
        // (e.g. timestamp formatting via `spark_format_timestamp_str`). BinaryView
        // and FixedSizeBinary likewise need conversion to the variable binary type.
        if matches!(
            info.get_data_type(&arg)?,
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::List(_)
        ) {
            Ok(ExprSimplifyResult::Simplified(arg))
        } else {
            Ok(ExprSimplifyResult::Original(vec![arg]))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                Some(String::new()),
            )));
        }
        // `return_type` is cloned because `args.return_field` is moved into the
        // delegate call below, but the null-typed early-return still needs it.
        let return_type = args.return_field.data_type().clone();
        let mut null_mask = None;
        for arg in &args.args {
            match arg {
                ColumnarValue::Scalar(s) if s.is_null() => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::try_from(&return_type)?));
                }
                ColumnarValue::Array(a) => {
                    let mask = is_null(a)?;
                    null_mask = match null_mask {
                        Some(existing) => Some(or(&existing, &mask)?),
                        None => Some(mask),
                    };
                }
                _ => (),
            }
        }
        let null_mask = null_mask.unwrap_or_else(|| BooleanArray::from(vec![false; 1]));

        let return_field = args.return_field.clone();
        let return_type = return_field.data_type();

        let concatenated = if args
            .args
            .iter()
            .any(|arg| matches!(arg.data_type(), DataType::List(_)))
        {
            // Cast arrays with Null element type to the return type for proper
            // concatenation. This handles cases like `concat(array(), array(1, 2, 3))`
            // where the first array has type `List(Null)` and needs to be cast to
            // `List(Int32)` so `ArrayConcat` can merge them.
            let casted_args = cast_list_columnar_values(args.args, return_type)?;
            let casted_scalar_args = ScalarFunctionArgs {
                args: casted_args,
                arg_fields: args.arg_fields,
                number_rows: args.number_rows,
                return_field: args.return_field,
                config_options: args.config_options,
            };
            ArrayConcat::new().invoke_with_args(casted_scalar_args)
        } else if matches!(return_type, DataType::Binary | DataType::LargeBinary) {
            concat_binary_values(&args.args, return_type)
        } else {
            let casted_columns =
                if can_concat_views_with_utf8_limits(&args.args) {
                    args.args
                } else if args.args.iter().any(|arg| {
                    matches!(arg.data_type(), DataType::LargeUtf8 | DataType::LargeBinary)
                }) {
                    cast_columnar_values(args.args, &DataType::LargeUtf8)?
                } else {
                    cast_columnar_values(args.args, &DataType::Utf8)?
                };

            let casted_args = ScalarFunctionArgs {
                args: casted_columns,
                arg_fields: args.arg_fields,
                number_rows: args.number_rows,
                return_field: args.return_field,
                config_options: args.config_options,
            };

            ConcatFunc::new().invoke_with_args(casted_args)
        }?;

        let concatenated_array = match concatenated {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        Ok(ColumnarValue::Array(nullif(
            cast(&concatenated_array, return_type)?.as_ref(),
            &null_mask,
        )?))
    }
}

fn can_concat_views_with_utf8_limits(values: &[ColumnarValue]) -> bool {
    if !values
        .iter()
        .all(|value| matches!(value.data_type(), DataType::Utf8 | DataType::Utf8View))
    {
        return false;
    }
    let Some(rows) = values.iter().find_map(|value| match value {
        ColumnarValue::Array(array) if array.data_type() == &DataType::Utf8View => {
            Some(array.len())
        }
        _ => None,
    }) else {
        return false;
    };
    // Preserve both old Utf8 input/output limits, including encoded NULL view lengths.
    let limit = i32::MAX as usize;
    let mut remaining = limit;
    for value in values {
        let available = match value {
            ColumnarValue::Array(array) if array.len() == rows => match array.data_type() {
                DataType::Utf8View => array
                    .as_string_view()
                    .views()
                    .iter()
                    .try_fold(remaining, |bytes, view| {
                        bytes.checked_sub(*view as u32 as usize)
                    }),
                DataType::Utf8 => remaining.checked_sub(array.as_string::<i32>().values().len()),
                _ => None,
            },
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(value)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(value)))
                if value.len() <= limit =>
            {
                value
                    .len()
                    .checked_mul(rows)
                    .and_then(|bytes| remaining.checked_sub(bytes))
            }
            _ => None,
        };
        let Some(available) = available else {
            return false;
        };
        remaining = available;
    }
    true
}

// Explicit folding opt-outs keep the whole CONCAT on its native NULL path.
// Their descendants may fold when reachable; this guard must not force them.
fn has_folding_disabled_argument(args: &[Expr]) -> Result<bool> {
    for arg in args {
        if arg.exists(|expr| {
            Ok(matches!(expr, Expr::ScalarFunction(function)
                if !function.func.inner().supports_constant_folding()))
        })? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn has_unfolded_constant_argument(args: &[Expr]) -> Result<bool> {
    if has_folding_disabled_argument(args)? {
        return Ok(false);
    }
    for arg in args {
        if concat_constant_state(arg)?.1 {
            return Ok(true);
        }
    }
    Ok(false)
}

// Return (whole scalar constant, pending constant requiring native folding).
// The caller has excluded folding-disabled extension contexts.
// Summarize each child once so column-dependent chains take one syntax walk.
fn concat_constant_state(expr: &Expr) -> Result<(bool, bool)> {
    if let Some(bodies) = SparkSubstring::closed_argument_bodies(expr)? {
        let mut constant = true;
        let mut pending = false;
        for body in bodies {
            let (child_constant, child_pending) = concat_constant_state(body)?;
            constant &= child_constant;
            pending |= child_pending;
        }
        return Ok((constant, constant || pending));
    }
    let mut constant = match expr {
        Expr::Literal(_, _)
        | Expr::Alias(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::BinaryExpr(_)
        | Expr::Case(_)
        | Expr::Negative(_)
        | Expr::Not(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Between(_)
        | Expr::InList(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_) => true,
        Expr::ScalarFunction(function) => {
            function.func.signature().volatility != Volatility::Volatile
        }
        _ => return Ok((false, false)),
    };
    let mut pending = false;
    expr.apply_children(|child| {
        let (child_constant, child_pending) = concat_constant_state(child)?;
        constant &= child_constant;
        pending |= child_pending;
        Ok(TreeNodeRecursion::Continue)
    })?;
    // Like native ConstEvaluator, defer failures under nonconstant CASE and
    // short-circuit functions. Whole constant conditionals fold as one operand.
    if constant {
        pending = !matches!(expr, Expr::Literal(_, _));
    } else if expr.short_circuits()
        && !matches!(expr, Expr::BinaryExpr(_))
        && !matches!(expr, Expr::ScalarFunction(function)
            if function.func.inner().downcast_ref::<SparkSubstring>()
                .is_some_and(SparkSubstring::has_logical_arguments))
    {
        pending = false;
    }
    Ok((constant, pending))
}

fn concat_binary_values(values: &[ColumnarValue], return_type: &DataType) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(values)?;
    let mut arrays = arrays
        .into_iter()
        .map(|array| Ok(cast(&array, return_type)?))
        .collect::<Result<Vec<_>>>()?
        .into_iter();
    let Some(first) = arrays.next() else {
        return internal_err!("spark_concat: binary concatenation requires an argument");
    };
    let concatenated = arrays.try_fold(first, |left, right| -> Result<_> {
        Ok(concat_elements_dyn(left.as_ref(), right.as_ref())?)
    })?;
    Ok(ColumnarValue::Array(concatenated))
}

fn cast_columnar_values(
    values: Vec<ColumnarValue>,
    target_type: &DataType,
) -> Result<Vec<ColumnarValue>> {
    values
        .into_iter()
        .map(|value| {
            let is_timestamp = matches!(value.data_type(), DataType::Timestamp(_, _));
            match value {
                ColumnarValue::Scalar(scalar) => {
                    let casted = scalar.cast_to(target_type)?;
                    if is_timestamp {
                        Ok(ColumnarValue::Scalar(spark_format_timestamp_scalar(
                            casted,
                        )?))
                    } else {
                        Ok(ColumnarValue::Scalar(casted))
                    }
                }
                ColumnarValue::Array(array) => {
                    let cast_array = cast(&array, target_type)?;
                    if is_timestamp {
                        Ok(ColumnarValue::Array(spark_format_timestamp_array(
                            cast_array,
                        )?))
                    } else {
                        Ok(ColumnarValue::Array(cast_array))
                    }
                }
            }
        })
        .collect()
}

/// Arrow formats timestamps as ISO 8601 (e.g. "2024-01-15T12:00:00Z" or
/// "2024-01-15 12:00:00+08:00"). Spark uses a space separator and no timezone
/// suffix (e.g. "2024-01-15 12:00:00").
fn spark_format_timestamp_str(s: &str) -> String {
    // Strip the timezone suffix (Z, +HH:MM, -HH:MM) that starts after the seconds
    // part. "YYYY-MM-DD HH:MM:SS" is 19 chars, so search from position 19 to avoid
    // matching the '-' in the date portion. The cutoff is the same on `s` as on the
    // 'T'-replaced string (the 'T' is at index 10, before 19), so we slice first and
    // replace once — a single allocation instead of replace-then-slice.
    let end = s
        .char_indices()
        .skip_while(|(i, _)| *i < 19)
        .find(|(_, c)| matches!(c, 'Z' | '+' | '-'))
        .map_or(s.len(), |(i, _)| i);
    s[..end].replace('T', " ")
}

fn spark_format_timestamp_scalar(scalar: ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Ok(ScalarValue::Utf8(Some(spark_format_timestamp_str(&s)))),
        ScalarValue::LargeUtf8(Some(s)) => {
            Ok(ScalarValue::LargeUtf8(Some(spark_format_timestamp_str(&s))))
        }
        other => Ok(other),
    }
}

fn spark_format_timestamp_array(
    array: Arc<dyn datafusion::arrow::array::Array>,
) -> Result<Arc<dyn datafusion::arrow::array::Array>> {
    use datafusion::arrow::array::Array;
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "expected StringArray after timestamp cast".to_string(),
            )
        })?;
    let result: StringArray = string_array
        .iter()
        .map(|opt| opt.map(spark_format_timestamp_str))
        .collect();
    Ok(Arc::new(result))
}

/// Cast list arrays to the target list type.
/// This is needed for concatenating empty arrays and arrays that only differ by
/// element nullability.
fn cast_list_columnar_values(
    values: Vec<ColumnarValue>,
    target_type: &DataType,
) -> Result<Vec<ColumnarValue>> {
    values
        .into_iter()
        .map(|value| {
            let value_type = value.data_type();
            let needs_cast = matches!(value_type, DataType::List(_)) && value_type != *target_type;
            if needs_cast {
                match value {
                    ColumnarValue::Scalar(scalar) => {
                        Ok(ColumnarValue::Scalar(scalar.cast_to(target_type)?))
                    }
                    ColumnarValue::Array(array) => {
                        let cast_array = cast(&array, target_type)?;
                        Ok(ColumnarValue::Array(cast_array))
                    }
                }
            } else {
                Ok(value)
            }
        })
        .collect()
}

/// If `expr` is a nested `spark_concat` call, return its arguments.
fn nested_concat_args(expr: &Expr, ordered_evaluation: bool) -> Option<&[Expr]> {
    match expr {
        Expr::ScalarFunction(inner)
            if inner
                .func
                .inner()
                .downcast_ref::<SparkConcat>()
                .is_some_and(|concat| concat.ordered_evaluation == ordered_evaluation) =>
        {
            Some(&inner.args)
        }
        _ => None,
    }
}

/// Spark's `CombineConcats`: flatten nested `spark_concat` children,
/// `concat(concat(a, b), c)` -> `concat(a, b, c)`, reducing nesting to a single
/// kernel pass. Only flatten within the same evaluation mode and when the return
/// type is preserved. This guards the array element-type / dimension merge in
/// `return_type`; otherwise the arguments are returned unchanged.
fn flatten_nested_concats(
    args: Vec<Expr>,
    info: &SimplifyContext,
    ordered_evaluation: bool,
) -> Result<Vec<Expr>> {
    if !args
        .iter()
        .any(|arg| nested_concat_args(arg, ordered_evaluation).is_some())
    {
        return Ok(args);
    }
    let mut flattened = Vec::with_capacity(args.len());
    for arg in &args {
        match nested_concat_args(arg, ordered_evaluation) {
            Some(inner) => flattened.extend(inner.iter().cloned()),
            None => flattened.push(arg.clone()),
        }
    }
    let before = args
        .iter()
        .map(|arg| info.get_data_type(arg))
        .collect::<Result<Vec<_>>>()?;
    let after = flattened
        .iter()
        .map(|arg| info.get_data_type(arg))
        .collect::<Result<Vec<_>>>()?;
    match (concat_return_type(&before), concat_return_type(&after)) {
        (Ok(before_type), Ok(after_type)) if before_type == after_type => Ok(flattened),
        _ => Ok(args),
    }
}

/// Spark `concat` output type: array concat (list dim/element merge), all-binary,
/// or string (widest of Utf8View / LargeUtf8 / Utf8).
///
/// [Credit]: <https://github.com/apache/datafusion/blob/7ccc6d7c55ae9dbcb7dee031f394bf11a03000ba/datafusion/functions-nested/src/concat.rs#L276-L310>
fn concat_return_type(arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.is_empty() {
        return Ok(DataType::Utf8);
    }
    if arg_types
        .iter()
        .any(|arg_type| matches!(arg_type, DataType::List(_)))
    {
        let mut expr_type: Option<DataType> = None;
        let mut max_dims = 0;
        for arg_type in arg_types {
            match arg_type {
                DataType::List(field) => {
                    if !field.data_type().equals_datatype(&DataType::Null) {
                        let dims = list_ndims(arg_type);
                        expr_type = Some(match max_dims.cmp(&dims) {
                            Ordering::Greater => expr_type.unwrap_or_else(|| arg_type.clone()),
                            Ordering::Equal => {
                                if let Some(expr_type) = expr_type {
                                    merge_list_types(&expr_type, arg_type).ok_or_else(|| {
                                        datafusion_common::plan_datafusion_err!(
                                            "It is not possible to concatenate arrays of different types. Expected: {expr_type}, got: {arg_type}"
                                        )
                                    })?
                                } else {
                                    arg_type.clone()
                                }
                            }
                            Ordering::Less => {
                                max_dims = dims;
                                arg_type.clone()
                            }
                        });
                    }
                }
                _ => {
                    return plan_err!(
                        "The array_concat function can only accept list as the args."
                    );
                }
            }
        }
        // All arrays had Null element type (e.g. array() + array()) — keep as List(Null)
        Ok(expr_type.unwrap_or_else(|| arg_types[0].clone()))
    } else if arg_types.iter().all(|arg_type| {
        matches!(
            arg_type,
            DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
        )
    }) {
        if arg_types.contains(&DataType::LargeBinary) {
            Ok(DataType::LargeBinary)
        } else {
            Ok(DataType::Binary)
        }
    } else {
        Ok(arg_types
            .iter()
            .find(|&arg_type| matches!(arg_type, &DataType::Utf8View))
            .or_else(|| {
                arg_types
                    .iter()
                    .find(|&arg_type| matches!(arg_type, &DataType::LargeUtf8))
            })
            .unwrap_or(&DataType::Utf8)
            .clone())
    }
}

fn merge_list_types(left: &DataType, right: &DataType) -> Option<DataType> {
    match (left, right) {
        (DataType::List(left), DataType::List(right)) => {
            let data_type =
                merge_list_types(left.data_type(), right.data_type()).or_else(|| {
                    // Leaf list item types may only differ by field/nullability metadata.
                    left.data_type()
                        .equals_datatype(right.data_type())
                        .then(|| left.data_type().clone())
                })?;
            Some(DataType::List(Arc::new(
                left.as_ref()
                    .clone()
                    .with_data_type(data_type)
                    .with_nullable(left.is_nullable() || right.is_nullable()),
            )))
        }
        _ if left.equals_datatype(right) => Some(left.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, AsArray, BinaryArray, ListArray, StringViewArray};
    use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    include!("../../../tests/fixtures/concat-row-cost-1388-original.rs");

    fn concat_arguments(values: Vec<ColumnarValue>, rows: usize) -> Result<ScalarFunctionArgs> {
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
        let return_field = SparkConcat::new().return_field_from_args(ReturnFieldArgs {
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

    fn assert_original_concat(values: Vec<ColumnarValue>, rows: usize) -> Result<()> {
        let description = format!("args={values:?}, rows={rows}");
        let expected = original_concat(concat_arguments(values.clone(), rows)?);
        let actual = SparkConcat::new().invoke_with_args(concat_arguments(values, rows)?);
        match (actual, expected) {
            (Ok(ColumnarValue::Array(actual)), Ok(ColumnarValue::Array(expected))) => {
                assert_eq!(actual.data_type(), expected.data_type(), "{description}");
                assert_eq!(actual.as_ref(), expected.as_ref(), "{description}");
            }
            (Ok(ColumnarValue::Scalar(actual)), Ok(ColumnarValue::Scalar(expected))) => {
                assert_eq!(actual.data_type(), expected.data_type(), "{description}");
                assert_eq!(actual, expected, "{description}");
            }
            (Err(actual), Err(expected)) => {
                assert_eq!(actual.to_string(), expected.to_string(), "{description}")
            }
            (actual, expected) => {
                return internal_err!(
                    "CONCAT result forms differ: {actual:?}, {expected:?}; {description}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn concat_row_path_matches_frozen_materialization() -> Result<()> {
        let inputs: Vec<Vec<Option<String>>> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/concat-row-cost-1388-inputs.json"
        ))
        .map_err(|error| {
            datafusion_common::internal_datafusion_err!("invalid CONCAT corpus: {error}")
        })?;
        let types = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
        for values in inputs {
            for mut storage in 0..3_usize.pow(values.len() as u32) {
                let scalars = values
                    .iter()
                    .map(|value| {
                        let data_type = &types[storage % 3];
                        storage /= 3;
                        ScalarValue::try_from_array(
                            cast(&StringViewArray::from(vec![value.as_deref()]), data_type)?
                                .as_ref(),
                            0,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                for mask in 0..1 << values.len() {
                    for rows in [0, 3] {
                        let args = scalars
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
                        assert_original_concat(args, rows)?;
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn concat_view_guard_preserves_utf8_capacity_boundaries() -> Result<()> {
        let source = StringViewArray::from(vec!["x".repeat(32768)]);
        let repeated = |bytes: usize, nulls: bool| {
            let full = bytes / 32768;
            let tail = bytes % 32768;
            let mut views = vec![source.views()[0]; full];
            if tail != 0 {
                let value = source.value(0);
                views.push(datafusion::arrow::array::make_view(
                    &value.as_bytes()[..tail],
                    0,
                    0,
                ));
            }
            let validity = nulls.then(|| NullBuffer::new_null(views.len()));
            ColumnarValue::Array(Arc::new(StringViewArray::new(
                ScalarBuffer::from(views),
                source.data_buffers().to_vec(),
                validity,
            )))
        };
        let limit = i32::MAX as usize;
        assert!(can_concat_views_with_utf8_limits(&[repeated(limit, false)]));
        assert!(!can_concat_views_with_utf8_limits(&[repeated(
            limit + 1,
            false
        )]));
        // Each input fits Utf8; their concatenation would cross its old output limit.
        assert!(!can_concat_views_with_utf8_limits(&[
            repeated(1 << 30, false),
            repeated(1 << 30, false)
        ]));
        let nulls = repeated(limit + 1, true);
        if let ColumnarValue::Array(array) = &nulls {
            assert_eq!(array.as_string_view().total_bytes_len(), 0);
        }
        assert!(!can_concat_views_with_utf8_limits(&[nulls]));
        let fitting_rows = limit / 4096;
        let scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".repeat(4096))));
        for rows in [fitting_rows, fitting_rows + 1] {
            let empty_views = ColumnarValue::Array(Arc::new(StringViewArray::new(
                ScalarBuffer::from(vec![0_u128; rows]),
                vec![],
                None,
            )));
            assert_eq!(
                can_concat_views_with_utf8_limits(&[empty_views, scalar.clone()]),
                rows == fitting_rows,
                "scalar broadcast rows={rows}"
            );
        }
        let view = ColumnarValue::Array(Arc::new(StringViewArray::from(vec!["left", "right"])));
        assert!(can_concat_views_with_utf8_limits(&[
            view.clone(),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(":".into())))
        ]));
        assert!(!can_concat_views_with_utf8_limits(&[
            view.clone(),
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["one"])))
        ]));
        assert!(!can_concat_views_with_utf8_limits(&[
            view,
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(":".into())))
        ]));
        // Above-limit inputs are guard-only checks: invoking the retained old
        // route would allocate multi-GiB Utf8 buffers before failing.
        Ok(())
    }

    #[test]
    fn concat_other_paths_match_frozen_invocation() -> Result<()> {
        let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(b"abcdefghijklmnop".as_slice()),
            None,
            Some(b"".as_slice()),
        ]));
        for storage in [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(16),
        ] {
            let input = if storage == DataType::FixedSizeBinary(16) {
                cast(
                    &BinaryArray::from(vec![
                        Some(b"abcdefghijklmnop".as_slice()),
                        None,
                        Some(b"qrstuvwxyz012345".as_slice()),
                    ]),
                    &storage,
                )?
            } else {
                cast(bytes.as_ref(), &storage)?
            };
            for mask in 0..4 {
                let scalar = ScalarValue::try_from_array(input.as_ref(), 0)?;
                let args = (0..2)
                    .map(|index| {
                        if mask & (1 << index) == 0 {
                            ColumnarValue::Scalar(scalar.clone())
                        } else {
                            ColumnarValue::Array(input.clone())
                        }
                    })
                    .collect();
                assert_original_concat(args, 3)?;
            }
        }
        let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
        ]));
        assert_original_concat(
            vec![
                ColumnarValue::Array(list.clone()),
                ColumnarValue::Array(list),
            ],
            3,
        )?;
        for value in [
            ScalarValue::Int64(Some(42)),
            ScalarValue::Float64(Some(1.5)),
            ScalarValue::TimestampMicrosecond(Some(0), None),
            ScalarValue::Binary(Some(vec![255])),
        ] {
            assert_original_concat(
                vec![
                    ColumnarValue::Scalar(value),
                    ColumnarValue::Array(Arc::new(StringViewArray::from(vec!["text"]))),
                ],
                1,
            )?;
        }
        Ok(())
    }

    #[test]
    fn binary_concat_preserves_bytes_and_nulls_across_representations() -> Result<()> {
        let left: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(b"xxxxxxxxxxxxxxxx".as_slice()),
            Some(b"\x00\xffabcdefghijklmn".as_slice()),
            None,
            Some(b"\x80aabcdefghijklmn".as_slice()),
            Some(b"ababcdefghijklmn".as_slice()),
            Some(b"zzzzzzzzzzzzzzzz".as_slice()),
        ]));
        let right: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(b"xxxxxxxxxxxxxxxx".as_slice()),
            Some(b"\x80bABCDEFGHIJKLMN".as_slice()),
            Some(b"1234567890abcdef".as_slice()),
            None,
            Some(b"\xc3\xa9ABCDEFGHIJKLMN".as_slice()),
            Some(b"zzzzzzzzzzzzzzzz".as_slice()),
        ]));
        let types = [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(16),
        ];
        for left_type in &types {
            for right_type in &types {
                let args = vec![
                    ColumnarValue::Array(cast(&left, left_type)?.slice(1, 4)),
                    ColumnarValue::Array(cast(&right, right_type)?.slice(1, 4)),
                ];
                let arg_fields = args
                    .iter()
                    .map(|value| Arc::new(Field::new("v", value.data_type(), true)))
                    .collect::<Vec<_>>();
                let udf = SparkConcat::new();
                let return_field = udf.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &arg_fields,
                    scalar_arguments: &[None, None],
                })?;
                let expected_type = if left_type == &DataType::LargeBinary
                    || right_type == &DataType::LargeBinary
                {
                    DataType::LargeBinary
                } else {
                    DataType::Binary
                };
                assert_eq!(return_field.data_type(), &expected_type);
                assert!(return_field.is_nullable());
                let result = udf
                    .invoke_with_args(ScalarFunctionArgs {
                        args,
                        arg_fields,
                        number_rows: 4,
                        return_field,
                        config_options: Arc::new(ConfigOptions::default()),
                    })?
                    .into_array(4)?;
                assert_eq!(result.data_type(), &expected_type);
                let result = cast(&result, &DataType::Binary)?;
                assert_eq!(
                    result.as_binary::<i32>(),
                    &BinaryArray::from(vec![
                        Some(b"\x00\xffabcdefghijklmn\x80bABCDEFGHIJKLMN".as_slice()),
                        None,
                        None,
                        Some(b"ababcdefghijklmn\xc3\xa9ABCDEFGHIJKLMN".as_slice()),
                    ])
                );
            }
        }
        Ok(())
    }

    #[test]
    fn binary_concat_identity_agrees_with_its_result_type() -> Result<()> {
        let info = SimplifyContext::builder().build();
        let udf = SparkConcat::new();
        for value in [
            ScalarValue::Binary(Some(vec![0, 255])),
            ScalarValue::LargeBinary(Some(vec![0, 255])),
            ScalarValue::BinaryView(Some(vec![0, 255])),
            ScalarValue::FixedSizeBinary(2, Some(vec![0, 255])),
        ] {
            let input_type = value.data_type();
            let result_type = concat_return_type(std::slice::from_ref(&input_type))?;
            let expression = Expr::Literal(value, None);
            match udf.simplify(vec![expression.clone()], &info)? {
                ExprSimplifyResult::Simplified(actual) => {
                    assert_eq!(input_type, result_type);
                    assert_eq!(actual, expression);
                }
                ExprSimplifyResult::Original(actual) => {
                    assert_ne!(input_type, result_type);
                    assert_eq!(actual, vec![expression]);
                }
            }
        }
        Ok(())
    }
}
