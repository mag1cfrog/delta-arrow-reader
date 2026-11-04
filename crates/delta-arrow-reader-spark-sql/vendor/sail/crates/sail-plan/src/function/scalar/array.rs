// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::functions::expr_fn::{btrim, nvl};
use datafusion::functions_nested::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{
    ColumnarValue, ExprSchemable, HigherOrderUDF, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, cast, expr, is_null, lit, not, or, when,
};
use datafusion_functions_nested::extract::array_slice_udf;
use datafusion_functions_nested::flatten::Flatten;
use datafusion_functions_nested::make_array::make_array;
use datafusion_spark::function::array::expr_fn as array_fn;
use sail_common::utils::string::SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::array_contains::ArrayContains;
use sail_function::scalar::array::array_intersect::ArrayIntersect;
use sail_function::scalar::array::array_join::ArrayJoin;
use sail_function::scalar::array::array_position::SparkArrayPosition;
use sail_function::scalar::array::array_sort_by_direction::ArraySortByDirection;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::array::spark_array_compact::SparkArrayCompact;
use sail_function::scalar::array::spark_array_min_max::{ArrayMax, ArrayMin};
use sail_function::scalar::array::spark_sequence::{SparkSequence, SparkSequenceLazy};
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::misc::raise_error::RaiseError;

use super::lambda::lambda_with_fresh_parameter;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    NormalizeFloatArrayForSparkComparison, ScalarFunction, ScalarFunctionInput,
    SparkComparisonFloat,
};
use crate::function::float_keys::normalize_spark_float_array_comparison;
use crate::function::numeric_coercion::common_numeric_type;
use crate::function::result_coercion::{
    cast_nested_value, common_array_element_type, nested_result_cast_input,
    same_positional_logical_type,
};
use crate::function::temporal_coercion::cast_local_timestamp_expr_to_ltz;

fn array_size(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let value = input.arguments.one()?;
    let length = if value.get_type(input.function_context.schema)? == DataType::Null {
        value
    } else {
        expr_fn::array_length(value)
    };
    Ok(cast(length, DataType::Int32))
}

fn array_repeat(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (element, count) = input.arguments.two()?;
    let count = cast(count, DataType::Int64);
    let output_type = make_nullable_array_type(
        &expr_fn::array_repeat(element.clone(), count.clone()).get_type(schema.as_ref())?,
    );
    array_repeat_with_nullable_element(element, count, output_type, schema)
}

fn array_repeat_with_nullable_element(
    element: expr::Expr,
    count: expr::Expr,
    output_type: DataType,
    schema: &datafusion_common::DFSchemaRef,
) -> PlanResult<expr::Expr> {
    let element_type = make_nullable_array_type(&element.get_type(schema.as_ref())?);
    // A CASE without ELSE keeps the value but makes DataFusion plan it as nullable.
    let nullable_element = when(lit(true), cast(element, element_type)).end()?;
    Ok(cast(
        expr_fn::array_repeat(nullable_element, count),
        output_type,
    ))
}

fn array_compact(array: expr::Expr) -> expr::Expr {
    ScalarUDF::from(SparkArrayCompact::new()).call(vec![array])
}

fn arrays_zip(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let field_names: Vec<String> = input
        .arguments
        .iter()
        .zip(input.function_context.argument_display_names)
        .enumerate()
        .map(|(i, (expr, name))| match expr {
            expr::Expr::Column(_) | expr::Expr::Alias(_) => name.clone(),
            _ => format!("{i}"),
        })
        .collect();
    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(ArraysZip::new(field_names))),
        args: input.arguments,
    }))
}

fn slice(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (array, start, length) = input.arguments.three()?;
    slice_expr(array, start, length, schema)
}

fn slice_expr(
    array: expr::Expr,
    start: expr::Expr,
    length: expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
) -> PlanResult<expr::Expr> {
    let output_type = array.get_type(schema.as_ref())?;
    let array = cast_list_value_nullability(array, schema, true)?;
    let start = cast(start, DataType::Int64);
    let length = cast(length, DataType::Int64);
    let end = start.clone() + length - lit(1_i64);
    Ok(cast(
        ScalarUDF::from(SliceResultField(array_slice_udf())).call(vec![array, start, end]),
        output_type,
    ))
}

// Spark's result is NULL only when an array or numeric argument is NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SliceResultField(Arc<ScalarUDF>);

impl ScalarUDFImpl for SliceResultField {
    fn name(&self) -> &str {
        "spark_slice"
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arguments: &[DataType]) -> datafusion_common::Result<DataType> {
        self.0.return_type(arguments)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion_common::Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        let field = self.0.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(nullable)))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        self.0.invoke_with_args(args)
    }
}

fn sort_array(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (array, asc) = match input.arguments.len() {
        1 => (
            input.arguments.one()?,
            lit(ScalarValue::Boolean(Some(true))),
        ),
        2 => input.arguments.two()?,
        n => {
            return Err(PlanError::invalid(format!(
                "sort_array requires 1 or 2 arguments, got {n}"
            )));
        }
    };
    let array = if array.get_type(input.function_context.schema)? == DataType::Null {
        cast(array, DataType::new_list(DataType::Null, true))
    } else {
        array
    };
    let (sort, nulls) = match asc {
        expr::Expr::Literal(ScalarValue::Boolean(Some(true)), _metadata) => (
            lit(ScalarValue::Utf8(Some("ASC".to_string()))),
            lit(ScalarValue::Utf8(Some("NULLS FIRST".to_string()))),
        ),
        expr::Expr::Literal(ScalarValue::Boolean(Some(false)), _metadata) => (
            lit(ScalarValue::Utf8(Some("DESC".to_string()))),
            lit(ScalarValue::Utf8(Some("NULLS LAST".to_string()))),
        ),
        _ => {
            return Err(PlanError::invalid(format!(
                "Invalid asc value for sort_array: {asc}"
            )));
        }
    };
    Ok(ScalarUDF::from(ArraySortByDirection::default()).call(vec![array, sort, nulls]))
}

fn array_append(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (array, element) = input.arguments.two()?;
    let array_input = cast_list_value_nullability(array.clone(), schema, true)?;
    let appended = expr_fn::array_append(array_input, element);
    let output_type = with_list_value_nullability(&appended.get_type(schema.as_ref())?, true);
    let appended = cast(appended, output_type);
    Ok(when(array.clone().is_not_null(), appended).end()?)
}

fn array_prepend(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (array, element) = input.arguments.two()?;
    let array = cast_list_value_nullability(array, schema, true)?;
    Ok(when(
        array.clone().is_not_null(),
        expr_fn::array_prepend(element, array),
    )
    .end()?)
}

fn array_element(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    expr_fn::array_element(array, element + lit(1_i64))
}

fn array_contains(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    if input.arguments.len() != 2 {
        return Err(PlanError::invalid(format!(
            "ARRAY_CONTAINS requires two arguments, got {}",
            input.arguments.len()
        )));
    }
    let (mut array, mut element) = input.arguments.two()?;
    let array_type = array.get_type(schema)?;
    let element_type = element.get_type(schema)?;
    let mut coerced_array_type = array_type.clone();
    match &mut coerced_array_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            let types = [field.data_type().clone(), element_type.clone()];
            if types
                .iter()
                .any(|data_type| matches!(data_type, DataType::Decimal128(..)))
                && let Some(target) =
                    common_numeric_type(&types, input.function_context.plan_config.ansi_mode)
            {
                *field = Arc::new(field.as_ref().clone().with_data_type(target.clone()));
                if element_type != target {
                    element = cast(element, target);
                }
            }
        }
        _ => {}
    }
    if array_type != coerced_array_type {
        array = cast(array, coerced_array_type);
    }
    if let DataType::List(field)
    | DataType::LargeList(field)
    | DataType::FixedSizeList(field, _)
    | DataType::ListView(field)
    | DataType::LargeListView(field) = array.get_type(schema)?
        && matches!(field.data_type(), DataType::Float32 | DataType::Float64)
    {
        array = ScalarUDF::from(NormalizeFloatArrayForSparkComparison).call(vec![array]);
    }
    if matches!(
        element.get_type(schema)?,
        DataType::Float32 | DataType::Float64
    ) {
        element = ScalarUDF::from(SparkComparisonFloat { widen: false }).call(vec![element]);
    }
    Ok(ScalarUDF::from(ArrayContains::default()).call(vec![array, element]))
}

fn array_contains_all(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    nvl(expr_fn::array_has_all(array, element), lit(false))
}

fn array_insert(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (array, position, value) = input.arguments.three()?;
    let array_input = cast_list_value_nullability(array.clone(), schema, true)?;
    let output_type = array_update_output_type(&array_input, &value, schema)?;
    let array_input = cast(array_input, output_type.clone());
    let array_len = cast(expr_fn::array_length(array.clone()), DataType::Int64);

    let pos_from_zero = when(position.clone().gt(lit(0)), position.clone() - lit(1))
        .when(
            position.clone().lt(lit(0)),
            array_len.clone() + position + lit(1),
        )
        .end()?;

    let zero_index_error = cast(ScalarUDF::from(RaiseError::new()).call(vec![lit(
        "array_insert: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)"
    )]), output_type.clone());

    Ok(when(
        array.clone().is_null(),
        cast(array.clone(), output_type.clone()),
    )
    .when(pos_from_zero.clone().is_null(), zero_index_error)
    .when(
        pos_from_zero.clone().lt(lit(0)),
        expr_fn::array_concat(vec![
            array_repeat_with_nullable_element(value.clone(), lit(1), output_type.clone(), schema)?,
            array_repeat_with_nullable_element(
                lit(ScalarValue::Null),
                -pos_from_zero.clone(),
                output_type.clone(),
                schema,
            )?,
            array_input.clone(),
        ]),
    )
    .when(
        pos_from_zero.clone().eq(lit(0)),
        expr_fn::array_prepend(value.clone(), array_input.clone()),
    )
    .when(
        pos_from_zero
            .clone()
            .between(lit(1), array_len.clone() - lit(1)),
        expr_fn::array_concat(vec![
            expr_fn::array_slice(array_input.clone(), lit(1), pos_from_zero.clone(), None),
            array_repeat_with_nullable_element(value.clone(), lit(1), output_type.clone(), schema)?,
            expr_fn::array_slice(
                array_input.clone(),
                pos_from_zero.clone() + lit(1),
                array_len.clone(),
                None,
            ),
        ]),
    )
    .when(
        pos_from_zero.clone().eq(array_len.clone()),
        expr_fn::array_append(array_input.clone(), value.clone()),
    )
    .when(
        pos_from_zero.clone().gt(array_len.clone()),
        expr_fn::array_concat(vec![
            array_input,
            array_repeat_with_nullable_element(
                lit(ScalarValue::Null),
                pos_from_zero - array_len,
                output_type.clone(),
                schema,
            )?,
            array_repeat_with_nullable_element(value, lit(1), output_type, schema)?,
        ]),
    )
    .end()?)
}

fn arrays_overlap(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    if arguments.len() != 2 {
        return Err(PlanError::invalid(format!(
            "ARRAYS_OVERLAP requires two arguments, got {}",
            arguments.len()
        )));
    }
    let (left, right) = arguments.two()?;
    let schema = function_context.schema.as_ref();
    let left_type = left.get_type(schema)?;
    let right_type = right.get_type(schema)?;
    let (Some(left_element), Some(right_element)) = (
        array_element_field(&left_type),
        array_element_field(&right_type),
    ) else {
        return Err(PlanError::invalid(
            "[DATATYPE_MISMATCH.BINARY_ARRAY_DIFF_TYPES] ARRAYS_OVERLAP requires two array arguments",
        ));
    };
    let element_type = common_array_element_type(left_element, right_element, &function_context)?
        .ok_or_else(|| PlanError::invalid(format!(
            "[DATATYPE_MISMATCH.BINARY_ARRAY_DIFF_TYPES] ARRAYS_OVERLAP requires compatible array element types, got {left_type:?} and {right_type:?}"
        )))?;
    let operand_nullable = [
        left.nullable(schema)? || left_element.is_nullable(),
        right.nullable(schema)? || right_element.is_nullable(),
    ];
    let left_needs_value_cast =
        !same_positional_logical_type(left_element.data_type(), &element_type);
    let right_needs_value_cast =
        !same_positional_logical_type(right_element.data_type(), &element_type);

    let element = Arc::new(Field::new_list_field(element_type, true));
    let nullable_array_type = if matches!(
        left_type,
        DataType::LargeList(_) | DataType::LargeListView(_)
    ) || matches!(
        right_type,
        DataType::LargeList(_) | DataType::LargeListView(_)
    ) {
        DataType::LargeList(element)
    } else {
        DataType::List(element)
    };
    let prepare_operand =
        |value: expr::Expr, source: &DataType, needs_value_cast| -> PlanResult<expr::Expr> {
            let value = if needs_value_cast {
                cast_nested_value(value, source, &nullable_array_type, &function_context)?
            } else {
                value
            };
            let value = normalize_spark_float_array_comparison(value, schema)?;
            let source = value.get_type(schema)?;
            cast_nested_value(value, &source, &nullable_array_type, &function_context)
        };
    let left = prepare_operand(left, &left_type, left_needs_value_cast)?;
    let right = prepare_operand(right, &right_type, right_needs_value_cast)?;
    let same_type_null_only_array = cast(
        make_array(vec![lit(ScalarValue::Null)]),
        nullable_array_type,
    );

    let left_has_null = expr_fn::array_has_any(left.clone(), same_type_null_only_array.clone());
    let right_has_null = expr_fn::array_has_any(right.clone(), same_type_null_only_array);

    // A required ARRAY constructor can still fail while evaluating its elements.
    let result = when(
        is_null(expr_fn::array_length(left.clone())),
        lit(ScalarValue::Null),
    )
    .when(
        is_null(expr_fn::array_length(right.clone())),
        lit(ScalarValue::Null),
    )
    .when(
        or(
            expr_fn::array_length(left.clone()).eq(lit(0_i64)),
            expr_fn::array_length(right.clone()).eq(lit(0_i64)),
        ),
        lit(false),
    )
    .when(
        or(left_has_null, right_has_null),
        or(
            expr_fn::array_has_any(array_compact(left.clone()), array_compact(right.clone())),
            lit(ScalarValue::Null),
        ),
    )
    .when(lit(true), expr_fn::array_has_any(left, right))
    .end()?;
    Ok(ScalarUDF::from(ArraysOverlapResultField { operand_nullable }).call(vec![result]))
}

// Preserve the operands' declaration across the nullable CASE and singleton NULL probes.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArraysOverlapResultField {
    operand_nullable: [bool; 2],
}

impl ScalarUDFImpl for ArraysOverlapResultField {
    fn name(&self) -> &str {
        "spark_arrays_overlap_result_field"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _args: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(
        &self,
        _args: ReturnFieldArgs,
    ) -> datafusion_common::Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Boolean,
            self.operand_nullable.into_iter().any(|nullable| nullable),
        )))
    }

    fn simplify(
        &self,
        args: Vec<expr::Expr>,
        info: &datafusion_expr::simplify::SimplifyContext,
    ) -> datafusion_common::Result<datafusion_expr::simplify::ExprSimplifyResult> {
        use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
        let mut operand_nullable = self.operand_nullable;
        // This private wrapper owns the lowered CASE's result-value membership calls.
        // Prepare each whole operand, preserving its own conditional reachability.
        if let [expr::Expr::Case(case)] = args.as_slice() {
            for value in case
                .when_then_expr
                .iter()
                .map(|(_, value)| value.as_ref())
                .chain(case.else_expr.iter().map(|value| value.as_ref()))
            {
                value.apply(|value| {
                    if let expr::Expr::ScalarFunction(function) = value {
                        if function.func.name() == "array_has_any" {
                            for operand in &function.args {
                                datafusion::optimizer::simplify_expressions::ExprSimplifier::new(
                                    info.clone(),
                                )
                                .simplify(operand.clone())?;
                            }
                            // A folded synthetic cast hides its input fields;
                            // retain that operand's recorded declaration.
                            if let [left, right] = function.args.as_slice() {
                                for (nullable, operand) in
                                    operand_nullable.iter_mut().zip([left, right])
                                {
                                    if let Some(input) = nested_result_cast_input(operand) {
                                        let data_type = info.get_data_type(input)?;
                                        *nullable &= info.nullable(input)?
                                            || array_element_field(&data_type)
                                                .is_none_or(|field| field.is_nullable());
                                    }
                                }
                            }
                        }
                        return Ok(TreeNodeRecursion::Jump);
                    }
                    // User CASE expressions inside operands retain their own short-circuiting.
                    Ok(if matches!(value, expr::Expr::Case(_)) {
                        TreeNodeRecursion::Jump
                    } else {
                        TreeNodeRecursion::Continue
                    })
                })?;
            }
        }
        if operand_nullable != self.operand_nullable {
            return Ok(datafusion_expr::simplify::ExprSimplifyResult::Simplified(
                ScalarUDF::from(Self { operand_nullable }).call(args),
            ));
        }
        Ok(datafusion_expr::simplify::ExprSimplifyResult::Original(
            args,
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [result] = datafusion_common::utils::take_function_args(self.name(), args.args)?;
        Ok(result)
    }
}

fn normalize_flatten_inner_views(
    array: expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
) -> PlanResult<expr::Expr> {
    let mut array_type = array.get_type(schema)?;
    let inner = match &mut array_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => field,
        _ => return Ok(array),
    };
    let inner_type = match inner.data_type() {
        DataType::ListView(field) => DataType::List(field.clone()),
        DataType::LargeListView(field) => DataType::LargeList(field.clone()),
        _ => return Ok(array),
    };
    // Native FLATTEN needs canonical inner lists; Arrow gathers the checked view ranges.
    *inner = Arc::new(inner.as_ref().clone().with_data_type(inner_type));
    Ok(cast(array, array_type))
}

fn flatten(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    if arguments.len() != 1 {
        return Err(PlanError::invalid(format!(
            "FLATTEN requires one argument, got {}",
            arguments.len()
        )));
    }
    let array = normalize_flatten_inner_views(arguments.one()?, function_context.schema)?;
    let array_type = array.get_type(function_context.schema)?;
    let flattened = ScalarUDF::from(FlattenResult::default()).call(vec![array.clone()]);
    let inner = array_element_field(&array_type)
        .ok_or_else(|| PlanError::invalid("FLATTEN requires an array of arrays"))?;
    if !inner.data_type().is_list() {
        return Err(PlanError::invalid("FLATTEN requires an array of arrays"));
    }
    if !inner.is_nullable() {
        return Ok(flattened);
    }

    // The NULL probe only needs nullable inner arrays, not nullable output elements.
    let nullable_array_type = nullable_array_probe_type(&array_type);
    let nullable_array = cast(array.clone(), nullable_array_type.clone());

    // Cast the null-only array to the nullable type to match
    let same_type_null_only_array = cast(
        make_array(vec![lit(ScalarValue::Null)]),
        nullable_array_type,
    );

    let array_has_null = expr_fn::array_has_any(nullable_array.clone(), same_type_null_only_array);

    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(not(array_has_null)), Box::new(flattened))],
        else_expr: None,
    }))
}

fn array_element_field(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field),
        _ => None,
    }
}

fn nullable_array_probe_type(data_type: &DataType) -> DataType {
    let Some(field) = array_element_field(data_type) else {
        return data_type.clone();
    };
    let field = Arc::new(field.as_ref().clone().with_nullable(true));
    if matches!(
        data_type,
        DataType::LargeList(_) | DataType::LargeListView(_)
    ) {
        DataType::LargeList(field)
    } else {
        DataType::List(field)
    }
}

// The planner guards nullable inner arrays; native FLATTEN supplies the values.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
struct FlattenResult(Flatten);

impl ScalarUDFImpl for FlattenResult {
    fn name(&self) -> &str {
        "flatten"
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arguments: &[DataType]) -> datafusion_common::Result<DataType> {
        self.0.return_type(arguments)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion_common::Result<FieldRef> {
        let [array] = datafusion_common::utils::take_function_args(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[array.data_type().clone()])?,
            array.is_nullable(),
        )))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        self.0.invoke_with_args(args)
    }
}

fn sequence_trim_string(argument: expr::Expr) -> expr::Expr {
    btrim(vec![
        argument,
        lit(SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS),
    ])
}

fn sequence_cast(
    argument: expr::Expr,
    source_type: &DataType,
    target_type: &DataType,
    ansi_mode: bool,
) -> PlanResult<expr::Expr> {
    if source_type == target_type {
        return Ok(argument);
    }

    Ok(match (source_type, target_type) {
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64,
        ) => cast(sequence_trim_string(argument), DataType::Int64),
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, DataType::Date32) => {
            ScalarUDF::from(SparkDate::new(false)).call(vec![sequence_trim_string(argument)])
        }
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Timestamp(TimeUnit::Microsecond, timezone),
        ) => ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), ansi_mode, false)?)
            .call(vec![sequence_trim_string(argument)]),
        (
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
        ) => cast_local_timestamp_expr_to_ltz(argument, timezone),
        _ => cast(argument, target_type.clone()),
    })
}

fn sequence(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    if !matches!(arguments.len(), 2 | 3) {
        return Err(PlanError::invalid(format!(
            "sequence requires 2 or 3 arguments, got {}",
            arguments.len()
        )));
    }

    let config = function_context.plan_config;
    let udf = SparkSequence::new(Arc::clone(&config.session_timezone), config.ansi_mode);

    let argument_types = arguments
        .iter()
        .map(|argument| argument.get_type(function_context.schema.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let target_types = udf.coerce_types(&argument_types)?;

    let arguments = arguments
        .into_iter()
        .zip(argument_types)
        .zip(target_types)
        .map(|((argument, source_type), target_type)| {
            sequence_cast(argument, &source_type, &target_type, config.ansi_mode)
        })
        .collect::<PlanResult<Vec<_>>>()?;

    // TODO: Fold all-foldable arguments in Spark's left-to-right order.
    //  Optimizing independent lambda bodies can change which NULL or error wins.
    let arguments = arguments
        .into_iter()
        .map(|argument| lambda_with_fresh_parameter(argument, "_sequence"))
        .collect::<PlanResult<Vec<_>>>()?;

    Ok(expr::Expr::HigherOrderFunction(
        expr::HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(SparkSequenceLazy::new(udf))),
            arguments,
        ),
    ))
}

fn array_update_output_type(
    array: &expr::Expr,
    element: &expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
) -> PlanResult<DataType> {
    let updated = expr_fn::array_append(array.clone(), element.clone());
    Ok(with_list_value_nullability(
        &updated.get_type(schema.as_ref())?,
        true,
    ))
}

fn cast_list_value_nullability(
    expr: expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
    nullable: bool,
) -> PlanResult<expr::Expr> {
    let data_type = expr.get_type(schema.as_ref())?;
    let target_type = with_list_value_nullability(&data_type, nullable);
    if target_type == data_type {
        Ok(expr)
    } else {
        Ok(cast(expr, target_type))
    }
}

fn with_list_value_nullability(data_type: &DataType, nullable: bool) -> DataType {
    match data_type {
        DataType::List(field) => {
            DataType::List(Arc::new(field.as_ref().clone().with_nullable(nullable)))
        }
        DataType::LargeList(field) => {
            DataType::LargeList(Arc::new(field.as_ref().clone().with_nullable(nullable)))
        }
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            Arc::new(field.as_ref().clone().with_nullable(nullable)),
            *size,
        ),
        _ => data_type.clone(),
    }
}

/// Convert an array type to its nullable equivalent (all nested fields become nullable)
fn make_nullable_array_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => {
            let inner_type = make_nullable_array_type(field.data_type());
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                field.name(),
                inner_type,
                true,
            )))
        }
        DataType::LargeList(field) => {
            let inner_type = make_nullable_array_type(field.data_type());
            DataType::LargeList(Arc::new(arrow::datatypes::Field::new(
                field.name(),
                inner_type,
                true,
            )))
        }
        DataType::FixedSizeList(field, size) => {
            let inner_type = make_nullable_array_type(field.data_type());
            DataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new(field.name(), inner_type, true)),
                *size,
            )
        }
        other => other.clone(),
    }
}

pub(super) fn list_built_in_array_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("array", F::udf(SparkArray::new())),
        ("array_append", F::custom(array_append)),
        ("array_compact", F::unary(array_compact)),
        ("array_contains", F::custom(array_contains)),
        ("array_contains_all", F::binary(array_contains_all)),
        ("array_distinct", F::unary(expr_fn::array_distinct)),
        ("array_except", F::binary(expr_fn::array_except)),
        ("array_insert", F::custom(array_insert)),
        ("array_intersect", F::udf(ArrayIntersect::new())),
        ("array_join", F::udf(ArrayJoin::default())),
        ("array_max", F::udf(ArrayMax::new())),
        ("array_min", F::udf(ArrayMin::new())),
        ("array_position", F::udf(SparkArrayPosition::new())),
        ("array_prepend", F::custom(array_prepend)),
        ("array_remove", F::binary(expr_fn::array_remove_all)),
        ("array_repeat", F::custom(array_repeat)),
        ("array_size", F::custom(array_size)),
        ("array_union", F::binary(expr_fn::array_union)),
        ("arrays_overlap", F::custom(arrays_overlap)),
        ("arrays_zip", F::custom(arrays_zip)),
        ("flatten", F::custom(flatten)),
        ("get", F::binary(array_element)),
        ("sequence", F::custom(sequence)),
        ("shuffle", F::unary(array_fn::shuffle)),
        ("slice", F::custom(slice)),
        ("sort_array", F::custom(sort_array)),
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ListArray, RecordBatch};
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::prelude::SessionContext;
    use datafusion_common::DFSchema;
    use datafusion_expr::{ColumnarValue, col};

    use super::*;

    #[test]
    fn arrays_overlap_normalizes_float_bits_across_list_storage_and_slices() -> PlanResult<()> {
        use crate::function::common::FunctionContextInput;
        use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::Schema;

        let session = SessionContext::new();
        let make_values = |double, left| -> ArrayRef {
            if double {
                let values = if left {
                    vec![
                        Some(-0.0),
                        Some(1.0),
                        Some(f64::from_bits(0xfff8000000001234)),
                        Some(2.0),
                        None,
                        Some(5.0),
                        Some(700.0),
                        Some(701.0),
                        Some(8.0),
                        Some(9.0),
                    ]
                } else {
                    vec![
                        Some(0.0),
                        Some(3.0),
                        Some(f64::from_bits(0x7ff8000000005678)),
                        Some(4.0),
                        Some(6.0),
                        Some(7.0),
                        Some(800.0),
                        Some(801.0),
                        Some(10.0),
                        Some(11.0),
                    ]
                };
                Arc::new(Float64Array::from(values))
            } else {
                let values = if left {
                    vec![
                        Some(-0.0),
                        Some(1.0),
                        Some(f32::from_bits(0xffc01234)),
                        Some(2.0),
                        None,
                        Some(5.0),
                        Some(700.0),
                        Some(701.0),
                        Some(8.0),
                        Some(9.0),
                    ]
                } else {
                    vec![
                        Some(0.0),
                        Some(3.0),
                        Some(f32::from_bits(0x7fc05678)),
                        Some(4.0),
                        Some(6.0),
                        Some(7.0),
                        Some(800.0),
                        Some(801.0),
                        Some(10.0),
                        Some(11.0),
                    ]
                };
                Arc::new(Float32Array::from(values))
            }
        };
        for ansi_mode in [true, false] {
            let config = Arc::new(crate::config::PlanConfig {
                ansi_mode,
                ..Default::default()
            });
            for left_double in [false, true] {
                for right_double in [false, true] {
                    for (left_layout, right_layout) in [(0, 0), (0, 1), (3, 2), (4, 3), (2, 4)] {
                        let inputs = [
                            (left_double, true, left_layout),
                            (right_double, false, right_layout),
                        ]
                        .map(|(double, left, layout)| {
                            let values = make_values(double, left);
                            let element = Arc::new(
                                Field::new_list_field(values.data_type().clone(), true)
                                    .with_metadata([("origin".into(), "float-bits".into())].into()),
                            );
                            let list = ListArray::try_new(
                                element.clone(),
                                OffsetBuffer::from_lengths([2; 5]),
                                values,
                                left.then(|| NullBuffer::from(vec![true, true, true, false, true])),
                            )?;
                            let storage = match layout {
                                0 => DataType::List(element),
                                1 => DataType::LargeList(element),
                                2 => DataType::FixedSizeList(element, 2),
                                3 => DataType::ListView(element),
                                _ => DataType::LargeListView(element),
                            };
                            arrow::compute::cast(&list, &storage)
                        })
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;
                        for (offset, len) in [(0, 5), (1, 3), (1, 0)] {
                            let schema = Arc::new(Schema::new(vec![
                                Field::new("left", inputs[0].data_type().clone(), true),
                                Field::new("right", inputs[1].data_type().clone(), false),
                            ]));
                            let batch = RecordBatch::try_new(
                                schema.clone(),
                                inputs
                                    .iter()
                                    .map(|input| input.slice(offset, len))
                                    .collect(),
                            )?;
                            let schema = Arc::new(DFSchema::try_from(schema)?);
                            for arguments in [
                                vec![col("left"), col("right")],
                                vec![col("right"), col("left")],
                            ] {
                                let expression = arrays_overlap(ScalarFunctionInput {
                                    arguments,
                                    function_context: FunctionContextInput {
                                        argument_display_names: &[],
                                        plan_config: &config,
                                        session_context: &session,
                                        schema: &schema,
                                    },
                                })?;
                                let (_, field) = expression.to_field(&schema)?;
                                assert_eq!(field.data_type(), &DataType::Boolean);
                                assert!(field.is_nullable());
                                assert!(field.metadata().is_empty());
                                let actual = session
                                    .create_physical_expr(expression, &schema)?
                                    .evaluate(&batch)?
                                    .into_array(len)?;
                                actual.to_data().validate_full()?;
                                assert_eq!(
                                    actual.as_boolean().iter().collect::<Vec<_>>(),
                                    [Some(true), Some(true), None, None, Some(false)]
                                        [offset..offset + len]
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn arrays_overlap_preserves_null_results_across_list_storage_and_slices() -> PlanResult<()> {
        use crate::function::common::FunctionContextInput;
        use arrow::array::{Array, ArrayRef, AsArray};
        use arrow::datatypes::Schema;

        let session = SessionContext::new();
        let left: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), Some(2)]),
            Some(vec![None, Some(2)]),
            Some(vec![Some(3), Some(5)]),
            None,
        ]));
        let right: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), Some(4)]),
            Some(vec![Some(3), Some(4)]),
            Some(vec![Some(4), Some(6)]),
            Some(vec![Some(4), Some(6)]),
        ]));
        let element = Arc::new(
            Field::new_list_field(DataType::Int32, true)
                .with_metadata([("origin".into(), "overlap-element".into())].into()),
        );
        for ansi_mode in [true, false] {
            let config = Arc::new(crate::config::PlanConfig {
                ansi_mode,
                ..Default::default()
            });
            for storage in [
                DataType::List(element.clone()),
                DataType::LargeList(element.clone()),
                DataType::FixedSizeList(element.clone(), 2),
                DataType::ListView(element.clone()),
                DataType::LargeListView(element.clone()),
            ] {
                let inputs = [left.clone(), right.clone()]
                    .map(|input| arrow::compute::cast(input.as_ref(), &storage))
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;
                for (offset, len) in [(0, 4), (1, 2), (1, 0)] {
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("left", storage.clone(), true)
                            .with_metadata([("source".into(), "left".into())].into()),
                        Field::new("right", storage.clone(), false),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        inputs
                            .iter()
                            .map(|input| input.slice(offset, len))
                            .collect(),
                    )?;
                    for input in batch.columns() {
                        input.to_data().validate_full()?;
                    }
                    let schema = Arc::new(DFSchema::try_from(schema)?);
                    for arguments in [
                        vec![col("left"), col("right")],
                        vec![col("right"), col("left")],
                    ] {
                        let expression = arrays_overlap(ScalarFunctionInput {
                            arguments,
                            function_context: FunctionContextInput {
                                argument_display_names: &[],
                                plan_config: &config,
                                session_context: &session,
                                schema: &schema,
                            },
                        })?;
                        let (_, field) = expression.to_field(&schema)?;
                        assert_eq!(field.data_type(), &DataType::Boolean);
                        assert!(field.is_nullable());
                        assert!(field.metadata().is_empty());
                        let physical = session.create_physical_expr(expression, &schema)?;
                        let physical_field = physical.return_field(schema.inner())?;
                        assert_eq!(physical_field.data_type(), &DataType::Boolean);
                        assert!(physical_field.is_nullable());
                        assert!(physical_field.metadata().is_empty());
                        let result = physical.evaluate(&batch)?.into_array(len)?;
                        result.to_data().validate_full()?;
                        assert_eq!(
                            result.as_boolean().iter().collect::<Vec<_>>(),
                            [Some(true), None, Some(false), None][offset..offset + len]
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn flatten_keeps_element_fields_across_list_storage_and_slices() -> PlanResult<()> {
        use crate::function::common::FunctionContextInput;
        use arrow::array::{Array, ArrayRef, AsArray, Int32Array};
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::{Field, Schema};

        let session = SessionContext::new();
        let config = Arc::new(crate::config::PlanConfig::default());
        for element_nullable in [false, true] {
            for inner_nullable in [false, true] {
                for outer_nullable in [false, true] {
                    let element = Arc::new(
                        Field::new("element", DataType::Int32, element_nullable)
                            .with_metadata([("origin".into(), "flatten-leaf".into())].into()),
                    );
                    let values: ArrayRef = Arc::new(Int32Array::from(vec![
                        Some(1),
                        if element_nullable { None } else { Some(2) },
                        Some(3),
                        Some(4),
                    ]));
                    let inner: ArrayRef = Arc::new(ListArray::try_new(
                        element.clone(),
                        OffsetBuffer::from_lengths([1; 4]),
                        values,
                        inner_nullable.then(|| NullBuffer::from(vec![true, false, true, true])),
                    )?);
                    let inner_field = Arc::new(Field::new(
                        "inner",
                        inner.data_type().clone(),
                        inner_nullable,
                    ));
                    let outer: ArrayRef = Arc::new(ListArray::try_new(
                        inner_field.clone(),
                        OffsetBuffer::from_lengths([2; 2]),
                        inner,
                        outer_nullable.then(|| NullBuffer::from(vec![true, false])),
                    )?);
                    for storage in [
                        DataType::List(inner_field.clone()),
                        DataType::LargeList(inner_field.clone()),
                        DataType::FixedSizeList(inner_field.clone(), 2),
                        DataType::ListView(inner_field.clone()),
                        DataType::LargeListView(inner_field.clone()),
                    ] {
                        let input = arrow::compute::cast(outer.as_ref(), &storage)?;
                        for (offset, len) in [(0, 2), (1, 1), (1, 0)] {
                            let schema = Arc::new(Schema::new(vec![Field::new(
                                "items",
                                storage.clone(),
                                outer_nullable,
                            )]));
                            let batch = RecordBatch::try_new(
                                schema.clone(),
                                vec![input.slice(offset, len)],
                            )?;
                            let schema = Arc::new(DFSchema::try_from(schema)?);
                            let expression = flatten(ScalarFunctionInput {
                                arguments: vec![col("items")],
                                function_context: FunctionContextInput {
                                    argument_display_names: &[],
                                    plan_config: &config,
                                    session_context: &session,
                                    schema: &schema,
                                },
                            })?;
                            let output_type = if matches!(
                                storage,
                                DataType::LargeList(_) | DataType::LargeListView(_)
                            ) {
                                DataType::LargeList(element.clone())
                            } else {
                                DataType::List(element.clone())
                            };
                            let (_, field) = expression.to_field(&schema)?;
                            assert_eq!(field.data_type(), &output_type);
                            assert_eq!(field.is_nullable(), outer_nullable || inner_nullable);
                            let physical = session.create_physical_expr(expression, &schema)?;
                            assert_eq!(
                                physical.return_field(schema.inner())?.data_type(),
                                &output_type
                            );
                            let result = physical.evaluate(&batch)?.into_array(len)?;
                            result.to_data().validate_full()?;
                            assert_eq!(result.data_type(), &output_type);
                            for row in 0..len {
                                let source_row = offset + row;
                                assert_eq!(
                                    result.is_null(row),
                                    if source_row == 0 {
                                        inner_nullable
                                    } else {
                                        outer_nullable
                                    }
                                );
                                if result.is_valid(row) {
                                    let values = match result.data_type() {
                                        DataType::List(_) => result.as_list::<i32>().value(row),
                                        _ => result.as_list::<i64>().value(row),
                                    };
                                    let actual =
                                        values.as_any().downcast_ref::<Int32Array>().ok_or_else(
                                            || PlanError::internal("expected flattened integers"),
                                        )?;
                                    assert_eq!(
                                        actual.iter().collect::<Vec<_>>(),
                                        if source_row == 0 {
                                            vec![
                                                Some(1),
                                                if element_nullable { None } else { Some(2) },
                                            ]
                                        } else {
                                            vec![Some(3), Some(4)]
                                        }
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[expect(clippy::unwrap_used, clippy::panic)]
    #[test]
    fn test_slice() -> PlanResult<()> {
        let l1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ])]);
        let batch = RecordBatch::try_from_iter([("l1", Arc::new(l1) as _)])?;

        let df_schema = Arc::new(DFSchema::try_from(batch.schema())?);
        let expr = slice_expr(col("l1"), lit(2), lit(3), &df_schema)?;
        let physical_expr = SessionContext::new().create_physical_expr(expr, df_schema.as_ref())?;

        let result = physical_expr.evaluate(&batch)?;
        let result_array = match result {
            ColumnarValue::Array(array) => {
                array.as_any().downcast_ref::<ListArray>().unwrap().clone()
            }
            _ => panic!("Expected an array result"),
        };

        let expected_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(2),
            Some(3),
            Some(4),
        ])]);

        assert_eq!(result_array, expected_array);

        Ok(())
    }
}
