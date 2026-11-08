// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::compute::{can_cast_types, cast as arrow_cast};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, TimeUnit};
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::prelude::SessionContext;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams, WindowFunctionParams};
use datafusion_expr::{
    AggregateUDF, BinaryExpr, ColumnarValue, ExprSchemable, ExpressionPlacement, Operator,
    ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    WindowFrame, WindowFunctionDefinition, WindowUDF, cast, expr, lit,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::spark_cast_numeric_to_integer::SparkCastNumericToInteger;
use sail_function::scalar::spark_cast_string_to_decimal::SparkCastStringToDecimal;
use sail_function::scalar::spark_decimal_cast::{DecimalCastMode, SparkDecimalCast};
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::sketch::{DEFAULT_HLL_LG_CONFIG_K, DEFAULT_THETA_LG_NOM_ENTRIES};

use crate::config::PlanConfig;
use crate::error::{IntoPlanResult, PlanError, PlanResult};
use crate::formatter::SparkPlanFormatter;
use crate::function::comparison::{
    nanosecond_comparison_cast, string_comparison_cast, string_comparison_type,
    temporal_comparison_type,
};
use crate::function::temporal_coercion::cast_local_timestamp_expr_to_ltz;

pub(crate) fn drop_inherited_column_metadata(expression: expr::Expr) -> expr::Expr {
    set_result_metadata(expression, vec![])
}

pub(crate) fn clear_inherited_field_metadata(field: &mut Field) {
    // Spark drops user metadata on derived values; Arrow extensions describe their type.
    field.metadata_mut().retain(|key, _| {
        matches!(
            key.as_str(),
            EXTENSION_TYPE_NAME_KEY | EXTENSION_TYPE_METADATA_KEY
        )
    });
}

pub(crate) fn drop_inherited_column_metadata_for_analysis(expression: expr::Expr) -> expr::Expr {
    ScalarUDF::from(SparkResultMetadata {
        metadata: FieldMetadata::default(),
        lifetime: ResultMetadataLifetime::AnalysisOnly,
        hidden_sort_output_type: None,
    })
    .call(vec![expression])
}

pub(crate) fn drop_scalar_subquery_metadata(
    expression: expr::Expr,
    output_field: &FieldRef,
) -> expr::Expr {
    let mut field = output_field.as_ref().clone();
    clear_inherited_field_metadata(&mut field);
    ScalarUDF::from(SparkResultMetadata {
        metadata: FieldMetadata::from(&field),
        lifetime: ResultMetadataLifetime::UntilScalarInlining,
        hidden_sort_output_type: None,
    })
    .call(vec![expression])
}

pub(crate) fn is_scalar_subquery_metadata_boundary(expression: &expr::Expr) -> bool {
    if let expr::Expr::ScalarFunction(function) = expression
        && let Some(annotation) = function.func.inner().downcast_ref::<SparkResultMetadata>()
    {
        annotation.lifetime == ResultMetadataLifetime::UntilScalarInlining
    } else {
        false
    }
}

// Only the guarded one-row inliner may end a scalar metadata boundary.
pub(crate) fn restore_inlined_scalar_metadata(expression: expr::Expr) -> Transformed<expr::Expr> {
    if is_scalar_subquery_metadata_boundary(&expression)
        && let expr::Expr::ScalarFunction(mut function) = expression
    {
        if function.args.len() == 1 && !matches!(function.args[0], expr::Expr::ScalarSubquery(_)) {
            return Transformed::yes(function.args.remove(0));
        }
        return Transformed::no(expr::Expr::ScalarFunction(function));
    }
    Transformed::no(expression)
}

pub(crate) fn set_result_metadata(
    expression: expr::Expr,
    metadata: Vec<(String, String)>,
) -> expr::Expr {
    ScalarUDF::from(SparkResultMetadata {
        metadata: metadata.into_iter().collect::<HashMap<_, _>>().into(),
        lifetime: ResultMetadataLifetime::Permanent,
        hidden_sort_output_type: None,
    })
    .call(vec![expression])
}

pub(crate) fn preserve_hidden_sort_output_declaration(
    expression: expr::Expr,
    field: &FieldRef,
) -> expr::Expr {
    ScalarUDF::from(SparkResultMetadata {
        metadata: FieldMetadata::from(field.as_ref()),
        lifetime: ResultMetadataLifetime::Permanent,
        hidden_sort_output_type: Some(field.data_type().clone()),
    })
    .call(vec![expression])
}

fn overlay_hidden_sort_nested_fields(
    actual: &DataType,
    captured: &DataType,
) -> datafusion_common::Result<DataType> {
    let field = |actual: &FieldRef, captured: &FieldRef| -> datafusion_common::Result<FieldRef> {
        if actual.name() != captured.name() || (actual.is_nullable() && !captured.is_nullable()) {
            return datafusion_common::plan_err!(
                "result metadata declaration cannot change {actual:?} to {captured:?}"
            );
        }
        Ok(Arc::new(
            actual
                .as_ref()
                .clone()
                .with_data_type(overlay_hidden_sort_nested_fields(
                    actual.data_type(),
                    captured.data_type(),
                )?)
                .with_nullable(captured.is_nullable())
                .with_metadata(captured.metadata().clone()),
        ))
    };
    match (actual, captured) {
        (DataType::Struct(actual), DataType::Struct(captured))
            if actual.len() == captured.len() =>
        {
            Ok(DataType::Struct(
                actual
                    .iter()
                    .zip(captured)
                    .map(|(actual, captured)| field(actual, captured))
                    .collect::<datafusion_common::Result<Vec<_>>>()?
                    .into(),
            ))
        }
        (DataType::List(actual), DataType::List(captured)) => {
            Ok(DataType::List(field(actual, captured)?))
        }
        (DataType::LargeList(actual), DataType::LargeList(captured)) => {
            Ok(DataType::LargeList(field(actual, captured)?))
        }
        (DataType::FixedSizeList(actual, a), DataType::FixedSizeList(captured, b)) if a == b => {
            Ok(DataType::FixedSizeList(field(actual, captured)?, *a))
        }
        (DataType::Map(actual, a), DataType::Map(captured, b)) if a == b => {
            Ok(DataType::Map(field(actual, captured)?, *a))
        }
        _ if actual == captured => Ok(actual.clone()),
        _ => datafusion_common::plan_err!(
            "result metadata declaration cannot change {actual:?} to {captured:?}"
        ),
    }
}

// Arrow's positional cast keeps distinct values under duplicate STRUCT child names.
fn redeclare_scalar_fields(
    value: &ScalarValue,
    declared: &DataType,
) -> datafusion_common::Result<ScalarValue> {
    let array = arrow_cast(value.to_array()?.as_ref(), declared)?;
    ScalarValue::try_from_array(array.as_ref(), 0)
}

// Call after semantic preparation, or on a preparation-only expression copy.
// Resolution must retain CAST nodes for discovery and grouping value identity.
pub(crate) fn materialize_metadata_literals(
    expression: expr::Expr,
) -> datafusion_common::Result<Transformed<expr::Expr>> {
    expression.transform_up(|expression| {
        if let expr::Expr::ScalarFunction(function) = &expression
            && let Some(annotation) = function.func.inner().downcast_ref::<SparkResultMetadata>()
            && let [argument] = function.args.as_slice()
            && let Some(value) = literal_value_without_evaluation(argument)
            && let Ok((_, field)) = argument.to_field(&DFSchema::empty())
        {
            if annotation.lifetime == ResultMetadataLifetime::AnalysisOnly {
                return Ok(Transformed::yes(argument.clone()));
            }
            let field = annotation.annotate_field(&field)?;
            let value = if annotation.hidden_sort_output_type.is_some()
                && value.data_type() != *field.data_type()
            {
                redeclare_scalar_fields(&value, field.data_type())?
            } else {
                value
            };
            return Ok(Transformed::yes(expr::Expr::Literal(
                value,
                (!field.metadata().is_empty()).then(|| FieldMetadata::from(field.as_ref())),
            )));
        }
        Ok(Transformed::no(expression))
    })
}

pub(crate) fn literal_value_without_evaluation(expression: &expr::Expr) -> Option<ScalarValue> {
    match without_metadata_annotations(expression) {
        expr::Expr::Literal(value, _) => Some(value.clone()),
        expr::Expr::Alias(alias) => literal_value_without_evaluation(&alias.expr),
        expr::Expr::Cast(expr::Cast { expr, field })
        | expr::Expr::TryCast(expr::TryCast { expr, field }) => {
            let value = literal_value_without_evaluation(expr)?;
            // Keep invalid casts for the normal analyzer/planner to reject.
            if value.is_null() && can_cast_types(&value.data_type(), field.data_type()) {
                ScalarValue::try_new_null(field.data_type()).ok()
            } else {
                None
            }
        }
        expr::Expr::ScalarFunction(function)
            if function.func.inner().is::<SparkCastNumericToInteger>() =>
        {
            let cast = function
                .func
                .inner()
                .downcast_ref::<SparkCastNumericToInteger>()?;
            let [input] = function.args.as_slice() else {
                return None;
            };
            let value = literal_value_without_evaluation(input)?;
            cast.return_type(&[value.data_type()]).ok()?;
            if value.is_null() {
                ScalarValue::try_new_null(cast.target_type()).ok()
            } else {
                None
            }
        }
        _ => None,
    }
}

#[test]
fn numeric_integer_typed_null_lookup_does_not_evaluate_producers() -> datafusion_common::Result<()>
{
    use datafusion_expr::create_udf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let producer = create_udf(
        "numeric_integer_lookup_producer",
        vec![],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(move |_| {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(1.9))))
        }),
    );
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for (ansi, is_try) in [(false, false), (true, false), (true, true)] {
            let cast = ScalarUDF::from(SparkCastNumericToInteger::try_new(
                target.clone(),
                ansi,
                is_try,
            )?);
            for source in [ScalarValue::Float32(None), ScalarValue::Float64(None)] {
                assert_eq!(
                    literal_value_without_evaluation(&cast.call(vec![lit(source)])),
                    Some(ScalarValue::try_new_null(&target)?),
                );
            }
            assert_eq!(
                literal_value_without_evaluation(&cast.call(vec![producer.call(vec![])])),
                None
            );
        }
    }
    let bigint = ScalarUDF::from(SparkCastNumericToInteger::try_new(
        DataType::Int32,
        false,
        false,
    )?);
    assert_eq!(
        literal_value_without_evaluation(&bigint.call(vec![lit(ScalarValue::Int64(None))])),
        Some(ScalarValue::Int32(None))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

pub(crate) fn without_metadata_annotations(mut expression: &expr::Expr) -> &expr::Expr {
    while let expr::Expr::ScalarFunction(function) = expression
        && function.func.inner().is::<SparkResultMetadata>()
        && let [value] = function.args.as_slice()
    {
        expression = value;
    }
    expression
}

pub(crate) fn without_inherited_column_metadata(mut expression: &expr::Expr) -> &expr::Expr {
    while let expr::Expr::ScalarFunction(function) = expression
        && let Some(annotation) = function.func.inner().downcast_ref::<SparkResultMetadata>()
        && annotation.metadata == FieldMetadata::default()
        && annotation.lifetime == ResultMetadataLifetime::Permanent
        && annotation.hidden_sort_output_type.is_none()
        && let [value] = function.args.as_slice()
    {
        expression = value;
    }
    expression
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum ResultMetadataLifetime {
    Permanent,
    AnalysisOnly,
    UntilScalarInlining,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkResultMetadata {
    metadata: FieldMetadata,
    lifetime: ResultMetadataLifetime,
    hidden_sort_output_type: Option<DataType>,
}

impl SparkResultMetadata {
    fn result_data_type(&self, actual: &DataType) -> datafusion_common::Result<DataType> {
        match &self.hidden_sort_output_type {
            Some(captured) => overlay_hidden_sort_nested_fields(actual, captured),
            None => Ok(actual.clone()),
        }
    }

    fn annotate_field(&self, value: &FieldRef) -> datafusion_common::Result<FieldRef> {
        let mut field = value
            .as_ref()
            .clone()
            .with_name(self.name())
            .with_data_type(self.result_data_type(value.data_type())?);
        clear_inherited_field_metadata(&mut field);
        field.metadata_mut().extend(self.metadata.to_hashmap());
        Ok(Arc::new(field))
    }
}

impl ScalarUDFImpl for SparkResultMetadata {
    fn name(&self) -> &str {
        "spark_result_metadata"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, args: &[DataType]) -> datafusion_common::Result<DataType> {
        let [data_type] = args else {
            return datafusion_common::plan_err!("expected one result value");
        };
        self.result_data_type(data_type)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion_common::Result<FieldRef> {
        self.annotate_field(&args.arg_fields[0])
    }

    fn simplify(
        &self,
        args: Vec<expr::Expr>,
        _: &datafusion_expr::simplify::SimplifyContext,
    ) -> datafusion_common::Result<datafusion_expr::simplify::ExprSimplifyResult> {
        use datafusion_expr::simplify::ExprSimplifyResult;
        if self.lifetime == ResultMetadataLifetime::AnalysisOnly {
            let [value] = datafusion_common::utils::take_function_args(self.name(), args)?;
            Ok(ExprSimplifyResult::Simplified(value))
        } else {
            Ok(ExprSimplifyResult::Original(args))
        }
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        // An output annotation must not repartition a globally sorted input.
        args[0]
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [value] = datafusion_common::utils::take_function_args(self.name(), args.args)?;
        if self.hidden_sort_output_type.is_none() {
            return Ok(value);
        }
        let result_type = self.result_data_type(&value.data_type())?;
        if value.data_type() == result_type {
            return Ok(value);
        }
        match value {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(arrow_cast(
                array.as_ref(),
                &result_type,
            )?)),
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(redeclare_scalar_fields(
                &value,
                &result_type,
            )?)),
        }
    }
}

pub struct FunctionContextInput<'a> {
    /// The names of function arguments.
    /// Most functions do not need this information, so it is
    /// passed as `&[String]` rather than `Vec<String>` to avoid unnecessary clone.
    /// These are the display names from the [`crate::resolver::expression::NamedExpr`]s,
    /// not to be confused with named function arguments
    /// (e.g., not like encode(charset => "utf-8", expr => "abc")).
    pub argument_display_names: &'a [String],
    pub plan_config: &'a Arc<PlanConfig>,
    pub session_context: &'a SessionContext,
    pub schema: &'a DFSchemaRef,
}

pub struct ScalarFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub function_context: FunctionContextInput<'a>,
}

pub(crate) type ScalarFunction =
    Arc<dyn Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct ScalarFunctionBuilder;

impl ScalarFunctionBuilder {
    pub fn nullary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn() -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                arguments.zero()?;
                f().into_plan_result()
            },
        )
    }

    pub fn unary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| f(arguments.one()?).into_plan_result(),
        )
    }

    pub fn binary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (left, right) = arguments.two()?;
                f(left, right).into_plan_result()
            },
        )
    }

    pub fn ternary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (first, second, third) = arguments.three()?;
                f(first, second, third).into_plan_result()
            },
        )
    }

    pub fn quaternary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (first, second, third, fourth) = arguments.four()?;
                f(first, second, third, fourth).into_plan_result()
            },
        )
    }

    pub fn var_arg<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(Vec<expr::Expr>) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| f(arguments).into_plan_result(),
        )
    }

    pub fn binary_op(op: Operator) -> ScalarFunction {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context,
                  }| {
                let (left, right) = arguments.two()?;
                spark_binary_expr(
                    left,
                    op,
                    right,
                    function_context.schema,
                    function_context.plan_config,
                )
            },
        )
    }

    pub fn cast(data_type: DataType) -> ScalarFunction {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context,
                  }| {
                let arg = arguments.one()?;
                let source = arg.get_type(function_context.schema)?;
                if (matches!(source, DataType::Float32 | DataType::Float64)
                    && data_type.is_signed_integer())
                    || (source == DataType::Int64 && data_type == DataType::Int32)
                {
                    return Ok(ScalarUDF::from(SparkCastNumericToInteger::try_new(
                        data_type.clone(),
                        function_context.plan_config.ansi_mode,
                        false,
                    )?)
                    .call(vec![arg]));
                }
                if let DataType::Decimal128(precision, scale) = data_type {
                    match arg.get_type(function_context.schema)? {
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                            return Ok(ScalarUDF::new_from_impl(SparkCastStringToDecimal::new(
                                precision,
                                scale,
                                !function_context.plan_config.ansi_mode,
                            ))
                            .call(vec![arg]));
                        }
                        source if source.is_numeric() => {
                            let mode = if function_context.plan_config.ansi_mode {
                                DecimalCastMode::Ansi
                            } else {
                                DecimalCastMode::Legacy
                            };
                            return Ok(ScalarUDF::new_from_impl(SparkDecimalCast::to_decimal(
                                precision, scale, mode,
                            ))
                            .call(vec![arg]));
                        }
                        _ => {}
                    }
                }
                Ok(cast(arg, data_type.clone()))
            },
        )
    }

    pub fn udf<F>(f: F) -> ScalarFunction
    where
        F: ScalarUDFImpl + Send + Sync + 'static,
    {
        let func = ScalarUDF::from(f);
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| { Ok(func.call(arguments)) },
        )
    }

    pub fn custom<F>(f: F) -> ScalarFunction
    where
        F: Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> ScalarFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}

fn numeric_literal(value: &expr::Expr) -> Option<ScalarValue> {
    match without_metadata_annotations(value) {
        expr::Expr::Literal(value, _) => Some(value.clone()),
        expr::Expr::Cast(expr::Cast { expr, field })
        | expr::Expr::TryCast(expr::TryCast { expr, field }) => without_metadata_annotations(expr)
            .as_literal()
            .filter(|value| value.data_type().is_numeric() || value.is_null())
            .and_then(|value| value.cast_to(field.data_type()).ok()),
        expr::Expr::ScalarFunction(function) => {
            let cast = function
                .func
                .inner()
                .downcast_ref::<SparkCastNumericToInteger>()?;
            let [input] = function.args.as_slice() else {
                return None;
            };
            let input = without_metadata_annotations(input).as_literal()?;
            let ColumnarValue::Scalar(value) = cast
                .cast_value(&ColumnarValue::Scalar(input.clone()))
                .ok()?
            else {
                return None;
            };
            (!value.is_null() || input.is_null()).then_some(value)
        }
        _ => None,
    }
}

fn integer_needs_double(value: &expr::Expr, data_type: &DataType) -> bool {
    // Every TINYINT/SMALLINT value is exactly representable as FLOAT.
    if !matches!(data_type, DataType::Int32 | DataType::Int64) {
        return false;
    }
    // Compare against Spark's DOUBLE value, including BIGINT's f64 rounding.
    // ponytail: reuse direct numeric literal evaluation; leave other constants
    // and dynamic INT/BIGINT values on the widening path.
    let double = match numeric_literal(value) {
        Some(ScalarValue::Int32(value)) => value.map(f64::from),
        Some(ScalarValue::Int64(value)) => value.map(|v| v as f64),
        _ => return true,
    };
    double.is_some_and(|v| f64::from(v as f32) != v)
}

/// Whether a type is or holds, inside an ARRAY or STRUCT, a calendar interval, which Spark can
/// neither order nor compare.
pub(crate) fn holds_calendar_interval(data_type: &DataType) -> bool {
    match data_type {
        DataType::Interval(IntervalUnit::MonthDayNano) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => holds_calendar_interval(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| holds_calendar_interval(field.data_type())),
        _ => false,
    }
}

/// Spark's name for a type in its errors, such as `ARRAY<INTERVAL>`.
fn spark_error_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Interval(IntervalUnit::MonthDayNano) => "INTERVAL".to_owned(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => {
            format!("ARRAY<{}>", spark_error_type_name(field.data_type()))
        }
        DataType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|field| format!(
                    "{}: {}",
                    field.name(),
                    spark_error_type_name(field.data_type())
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => SparkPlanFormatter
            .data_type_to_cast_error_string(other)
            .map(|name| name.to_uppercase())
            .unwrap_or_else(|_| other.to_string()),
    }
}

/// Whether Spark's type checks accept `other` beside `interval`, which holds a calendar
/// interval: a NULL matches anything, types without calendar intervals are left to the usual
/// coercion, and ARRAY and STRUCT shapes must match where a calendar interval sits.
fn matches_calendar_interval_shape(interval: &DataType, other: &DataType) -> bool {
    if *interval == DataType::Null || *other == DataType::Null {
        return true;
    }
    if !holds_calendar_interval(interval) && !holds_calendar_interval(other) {
        return true;
    }
    match (interval, other) {
        (
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => true,
        (
            DataType::List(left)
            | DataType::LargeList(left)
            | DataType::FixedSizeList(left, _)
            | DataType::ListView(left)
            | DataType::LargeListView(left),
            DataType::List(right)
            | DataType::LargeList(right)
            | DataType::FixedSizeList(right, _)
            | DataType::ListView(right)
            | DataType::LargeListView(right),
        ) => matches_calendar_interval_shape(left.data_type(), right.data_type()),
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right.iter()).all(|(left, right)| {
                    matches_calendar_interval_shape(left.data_type(), right.data_type())
                })
        }
        _ => false,
    }
}

/// Spark's analysis of `function_name` over operands of `operand_types`
/// (`TypeUtils.checkForOrderingExpr` after its type checks). A calendar interval beside an
/// operand of another shape fails first with `mismatch_condition`; otherwise a calendar
/// interval fails with `DATATYPE_MISMATCH.INVALID_ORDERING_TYPE`.
pub(crate) fn reject_calendar_interval_ordering(
    operand_types: &[DataType],
    function_name: &str,
    mismatch_condition: &str,
) -> PlanResult<()> {
    let Some(interval) = operand_types
        .iter()
        .find(|data_type| holds_calendar_interval(data_type))
    else {
        return Ok(());
    };
    let names = operand_types
        .iter()
        .map(|data_type| format!("\"{}\"", spark_error_type_name(data_type)))
        .collect::<Vec<_>>()
        .join(", ");
    if operand_types
        .iter()
        .any(|data_type| !matches_calendar_interval_shape(interval, data_type))
    {
        return Err(PlanError::analysis(format!(
            "[{mismatch_condition}] The operands of `{function_name}` have incompatible types: {names}. SQLSTATE: 42K09"
        )));
    }
    Err(PlanError::analysis(format!(
        "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] The `{function_name}` does not support ordering on type \"{}\". SQLSTATE: 42K09",
        spark_error_type_name(interval)
    )))
}

/// Spark's check of a window partition key, which it sorts.
pub(crate) fn reject_calendar_interval_partition_key(
    display_name: &str,
    data_type: &DataType,
) -> PlanResult<()> {
    if !holds_calendar_interval(data_type) {
        return Ok(());
    }
    Err(PlanError::analysis(format!(
        "[EXPRESSION_TYPE_IS_NOT_ORDERABLE] Column expression \"{display_name}\" cannot be sorted because its type \"{}\" is not orderable. SQLSTATE: 42822",
        spark_error_type_name(data_type)
    )))
}

/// Spark's check of a sort key.
pub(crate) fn reject_calendar_interval_sort_key(data_type: &DataType) -> PlanResult<()> {
    reject_calendar_interval_ordering(
        std::slice::from_ref(data_type),
        "sortorder",
        "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
    )
}

pub(crate) fn spark_in_list(
    value: expr::Expr,
    list: Vec<expr::Expr>,
    negated: bool,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> PlanResult<expr::Expr> {
    let operand_types = std::iter::once(&value)
        .chain(&list)
        .map(|operand| operand.get_type(schema))
        .collect::<Result<Vec<_>, _>>()?;
    reject_calendar_interval_ordering(&operand_types, "in", "DATATYPE_MISMATCH.DATA_DIFF_TYPES")?;
    let mut input = expr::InList::new(Box::new(value), list, negated);
    let (mut decimal, mut floating, mut double, mut single, mut integer) =
        (false, false, false, false, false);
    for value in std::iter::once(input.expr.as_ref()).chain(&input.list) {
        let data_type = value.get_type(schema)?;
        // Leave string promotion and non-numeric coercion to the existing path.
        if !data_type.is_numeric() && data_type != DataType::Null {
            return Ok(expr::Expr::InList(input));
        }
        decimal |= data_type.is_decimal();
        floating |= data_type.is_floating();
        double |= data_type == DataType::Float64;
        single |= data_type == DataType::Float32;
        integer |= data_type.is_signed_integer();
    }
    if floating {
        // A BOOLEAN predicate can keep FLOAT if every integer has the same
        // value at FLOAT and Spark's DOUBLE comparison precision.
        let mut coerce_to_double = decimal;
        if !decimal && ansi_mode && single && integer && !double {
            for value in std::iter::once(input.expr.as_ref()).chain(&input.list) {
                if integer_needs_double(value, &value.get_type(schema)?) {
                    coerce_to_double = true;
                    break;
                }
            }
        }
        // Only zero and NaN can change membership when the input is normalized.
        // ponytail: recognize literals and direct numeric literal casts only;
        // leave other constant expressions on the existing normalization path.
        let (mut zero, mut negative_zero) = (false, false);
        let known_without_nan = matches!(
            input.expr.get_type(schema)?,
            DataType::Float32 | DataType::Float64
        ) && input.list.iter().all(|value| {
            let scalar = numeric_literal(value);
            match scalar.and_then(|value| match value {
                value @ ScalarValue::Float64(_) => Some(value),
                value => value.cast_to(&DataType::Float64).ok(),
            }) {
                Some(ScalarValue::Float64(value)) => value.is_none_or(|value| {
                    zero |= value == 0.0;
                    negative_zero |= value == 0.0 && value.is_sign_negative();
                    !value.is_nan()
                }),
                _ => false,
            }
        });
        let skip_normalization = known_without_nan && !zero;
        // Adding positive zero preserves NaNs and unifies both zero signs.
        // Keep computed operands and fused widening on their existing path.
        let zero_only = known_without_nan
            && zero
            && input.expr.try_as_col().is_some()
            && !(input.expr.get_type(schema)? == DataType::Float32 && (coerce_to_double || double));
        // Keep DOUBLE when Decimal or integer precision requires it. Preserve other types
        // so normalization does not change the existing numeric coercion.
        let convert = |value: expr::Expr, skip_normalization: bool| -> PlanResult<expr::Expr> {
            let data_type = value.get_type(schema)?;
            let floating = data_type.is_floating();
            Ok(if floating && !skip_normalization {
                ScalarUDF::from(SparkComparisonFloat {
                    widen: coerce_to_double || double,
                })
                .call(vec![value])
            } else if data_type != DataType::Float64 && (coerce_to_double || (floating && double)) {
                cast(value, DataType::Float64)
            } else {
                value
            })
        };
        input.expr = Box::new(if zero_only {
            let zero = ScalarValue::new_zero(&input.expr.get_type(schema)?)?;
            *input.expr + lit(zero)
        } else {
            convert(*input.expr, skip_normalization)?
        });
        input.list = input
            .list
            .into_iter()
            .map(|value| convert(value, known_without_nan && !negative_zero))
            .collect::<PlanResult<_>>()?;
    }
    Ok(expr::Expr::InList(input))
}

pub(crate) fn spark_comparison_widens_to_double(
    left: &DataType,
    right: &DataType,
    widen_integer_float: bool,
) -> bool {
    (left.is_decimal() && right.is_floating())
        || (left.is_floating() && right.is_decimal())
        || (widen_integer_float
            && ((left == &DataType::Float32 && right.is_signed_integer())
                || (right == &DataType::Float32 && left.is_signed_integer())))
}

pub(crate) fn spark_binary_expr(
    mut left: expr::Expr,
    op: Operator,
    mut right: expr::Expr,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> PlanResult<expr::Expr> {
    let ansi_mode = config.ansi_mode;
    if matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    ) {
        let mut left_type = left.get_type(schema)?;
        let mut right_type = right.get_type(schema)?;
        // Spark's `<>` is the negation of `=`, which it names.
        let comparison = match op {
            Operator::Eq | Operator::NotEq => "=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => "<=>",
            other => {
                return Err(PlanError::internal(format!(
                    "unexpected comparison operator {other}"
                )));
            }
        };
        reject_calendar_interval_ordering(
            &[left_type.clone(), right_type.clone()],
            comparison,
            "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES",
        )?;
        if let Some(conversion) =
            nanosecond_comparison_cast(&left_type, &right_type, &config.session_timezone)
        {
            let target = conversion.inner().return_type(&[left_type.clone()])?;
            left = conversion.call(vec![left]);
            right = conversion.call(vec![right]);
            left_type = target.clone();
            right_type = target;
        } else if let Some(target) =
            temporal_comparison_type(&left_type, &right_type, &config.session_timezone)
        {
            let convert = |value, source: &DataType| {
                if matches!(
                    source,
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
                ) && let DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) = &target
                {
                    cast_local_timestamp_expr_to_ltz(value, source, timezone)
                } else if source != &target {
                    cast(value, target.clone())
                } else {
                    value
                }
            };
            left = convert(left, &left_type);
            right = convert(right, &right_type);
            left_type = target.clone();
            right_type = target;
        }
        if let Some(target) =
            string_comparison_type(&left_type, &right_type, ansi_mode, &config.session_timezone)
        {
            let convert = |value, source: &DataType| -> PlanResult<_> {
                Ok(if source.is_string() {
                    string_comparison_cast(&target, ansi_mode)?.call(vec![value])
                } else if source != &target {
                    cast(value, target.clone())
                } else {
                    value
                })
            };
            left = convert(left, &left_type)?;
            right = convert(right, &right_type)?;
            left_type = target.clone();
            right_type = target;
        }
        if (left_type.is_numeric() || left_type == DataType::Null)
            && (right_type.is_numeric() || right_type == DataType::Null)
        {
            let ansi_float_integer = ansi_mode
                && ((left_type == DataType::Float32 && integer_needs_double(&right, &right_type))
                    || (right_type == DataType::Float32
                        && integer_needs_double(&left, &left_type)));
            let promote_to_double =
                spark_comparison_widens_to_double(&left_type, &right_type, ansi_float_integer);
            let widen = promote_to_double
                || left_type == DataType::Float64
                || right_type == DataType::Float64;
            let convert = |value, data_type: DataType| {
                if data_type.is_floating() {
                    ScalarUDF::from(SparkComparisonFloat { widen }).call(vec![value])
                } else if promote_to_double {
                    cast(value, DataType::Float64)
                } else {
                    value
                }
            };
            left = convert(left, left_type);
            right = convert(right, right_type);
        }
    }
    Ok(expr::Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        op,
        Box::new(right),
    )))
}

#[test]
fn in_list_skips_only_irrelevant_float_normalization() -> datafusion_common::Result<()> {
    use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
    use arrow::datatypes::{Field, Float64Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion::physical_expr::{create_physical_expr, execution_props::ExecutionProps};
    use datafusion_common::{DFSchema, tree_node::TreeNodeRecursion};
    use datafusion_expr::{col, simplify::SimplifyContext};

    let half_schema = Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
        "v",
        DataType::Float16,
        true,
    )]))?);
    let half = spark_in_list(col("v"), vec![lit(1.25_f32)], false, &half_schema, false).unwrap();
    let expr::Expr::InList(half) = half else {
        unreachable!()
    };
    assert!(matches!(half.expr.as_ref(), expr::Expr::ScalarFunction(f)
        if f.func.inner().is::<SparkComparisonFloat>()));

    let cast_schema = Arc::new(DFSchema::try_from(Schema::new(vec![
        Field::new("decimal", DataType::Decimal128(18, 4), true),
        Field::new("integer", DataType::Int64, true),
    ]))?);
    for name in ["decimal", "integer"] {
        let expr::Expr::InList(input) = spark_in_list(
            cast(col(name), DataType::Float64),
            vec![lit(0.0_f64), lit(1.25_f64)],
            false,
            &cast_schema,
            false,
        )
        .unwrap() else {
            unreachable!()
        };
        assert_eq!(input.list.len(), 2, "preserve inverse-cast candidates");
        assert!(matches!(input.expr.as_ref(), expr::Expr::ScalarFunction(f)
            if f.func.inner().is::<SparkComparisonFloat>()));
    }

    let mut values = vec![
        None,
        Some(0.0),
        Some(-0.0),
        Some(1.25),
        Some(1.5),
        Some(-1.5),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
        Some(f64::from_bits(0xfff0_0000_0000_0123)),
        Some(1e-40),
        Some(-1e-40),
    ];
    let mut bits = 0x1234_5678_9abc_def0_u64;
    values.extend((0..8192).map(|i| {
        bits = bits.wrapping_mul(6364136223846793005).wrapping_add(1);
        (i % 19 != 0).then_some(f64::from_bits(bits))
    }));
    let arrays: [ArrayRef; 2] = [
        Arc::new(Float32Array::from_iter(
            values.iter().map(|v| v.map(|v| v as f32)),
        )),
        Arc::new(Float64Array::from(values)),
    ];
    let mut checks = 0;
    let mut checked_rows = 0;
    for input in arrays {
        let ty = input.data_type().clone();
        let literal = |v: Option<f64>| {
            lit(match ty {
                DataType::Float32 => ScalarValue::Float32(v.map(|v| v as f32)),
                _ => ScalarValue::Float64(v),
            })
        };
        let finite = vec![Some(1.25), Some(1.5), Some(2.0)];
        let mut cases = vec![
            (
                "finite",
                finite.iter().copied().map(literal).collect::<Vec<_>>(),
                finite.clone(),
                true,
            ),
            (
                "nullable",
                vec![literal(Some(1.25)), literal(None)],
                vec![Some(1.25), None],
                true,
            ),
            (
                "zero",
                vec![literal(Some(0.0)), literal(Some(1.25))],
                vec![Some(0.0), Some(1.25)],
                true,
            ),
            (
                "negative_zero",
                vec![literal(Some(-0.0))],
                vec![Some(-0.0)],
                true,
            ),
            (
                "nan",
                vec![literal(Some(f64::NAN))],
                vec![Some(f64::NAN)],
                false,
            ),
            (
                "negative_nan",
                vec![literal(Some(-f64::NAN))],
                vec![Some(f64::NAN)],
                false,
            ),
            (
                "infinity",
                vec![
                    literal(Some(f64::INFINITY)),
                    literal(Some(f64::NEG_INFINITY)),
                ],
                vec![Some(f64::INFINITY), Some(f64::NEG_INFINITY)],
                true,
            ),
            ("all_null", vec![literal(None)], vec![None], true),
            ("empty", vec![], vec![], true),
            (
                "integer",
                vec![lit(1_i64), lit(2_i64)],
                vec![Some(1.0), Some(2.0)],
                true,
            ),
            (
                "decimal",
                vec![lit(ScalarValue::Decimal128(Some(125), 5, 2))],
                vec![Some(1.25)],
                true,
            ),
            (
                "casts",
                vec![cast(
                    lit(ScalarValue::Decimal128(Some(125), 5, 2)),
                    ty.clone(),
                )],
                vec![Some(1.25)],
                true,
            ),
            (
                "try_casts",
                vec![datafusion_expr::try_cast(lit(1_i64), ty.clone())],
                vec![Some(1.0)],
                true,
            ),
            (
                "underflow",
                vec![cast(lit(1e-50_f64), DataType::Float32)],
                vec![Some(0.0)],
                true,
            ),
            (
                "zero_nullable",
                vec![literal(Some(0.0)), literal(None)],
                vec![Some(0.0), None],
                true,
            ),
            (
                "zero_both_signs",
                vec![literal(Some(0.0)), literal(Some(-0.0))],
                vec![Some(0.0), Some(-0.0)],
                true,
            ),
            (
                "zero_integer",
                vec![lit(0_i64), lit(1_i64)],
                vec![Some(0.0), Some(1.0)],
                true,
            ),
            (
                "zero_decimal",
                vec![
                    lit(ScalarValue::Decimal128(Some(0), 5, 2)),
                    lit(ScalarValue::Decimal128(Some(125), 5, 2)),
                ],
                vec![Some(0.0), Some(1.25)],
                ty == DataType::Float64,
            ),
            (
                "zero_widening",
                vec![lit(0.0_f64), lit(1.25_f64)],
                vec![Some(0.0), Some(1.25)],
                ty == DataType::Float64,
            ),
            (
                "zero_negative_underflow",
                vec![cast(lit(-1e-50_f64), DataType::Float32)],
                vec![Some(-0.0)],
                true,
            ),
            (
                "zero_half",
                vec![cast(lit(0.0_f64), DataType::Float16)],
                vec![Some(0.0)],
                true,
            ),
            (
                "negative_zero_half",
                vec![cast(lit(-0.0_f64), DataType::Float16)],
                vec![Some(-0.0)],
                true,
            ),
            (
                "zero_nan",
                vec![literal(Some(0.0)), literal(Some(-f64::NAN))],
                vec![Some(0.0), Some(f64::NAN)],
                false,
            ),
            (
                "zero_unknown_cast",
                vec![literal(Some(0.0)), cast(lit("1.25"), ty.clone())],
                vec![Some(0.0), Some(1.25)],
                false,
            ),
            (
                "cast_nan",
                vec![cast(lit(-f64::NAN), DataType::Float32)],
                vec![Some(f64::NAN)],
                false,
            ),
            (
                "string_cast",
                vec![cast(lit("1.25"), ty.clone())],
                vec![Some(1.25)],
                false,
            ),
            (
                "nested_cast",
                vec![cast(cast(lit(1_i64), DataType::Float32), ty.clone())],
                vec![Some(1.0)],
                false,
            ),
            (
                "dynamic",
                vec![col("v"), literal(Some(1.25))],
                vec![Some(1.25)],
                false,
            ),
        ];
        let long: Vec<_> = (1..=128).map(|v| Some(f64::from(v))).collect();
        cases.push((
            "long",
            long.iter().copied().map(literal).collect(),
            long,
            true,
        ));
        let long_zero: Vec<_> = (0..=128).map(|v| Some(f64::from(v))).collect();
        cases.push((
            "zero_long",
            long_zero.iter().copied().map(literal).collect(),
            long_zero,
            true,
        ));
        for (offset, rows) in [(0, input.len()), (1, input.len() - 1), (2, 1), (0, 0)] {
            let input = input.slice(offset, rows);
            let schema = Arc::new(Schema::new(vec![Field::new("v", ty.clone(), true)]));
            let batch = RecordBatch::try_new(schema.clone(), vec![input.clone()])?;
            let schema = Arc::new(DFSchema::try_from(schema)?);
            let simplifier = ExprSimplifier::new(
                SimplifyContext::builder()
                    .with_schema(schema.clone())
                    .build(),
            );
            let wide = arrow::compute::cast(&input, &DataType::Float64)?;
            for (name, list, expected, skip) in &cases {
                for negated in [false, true] {
                    let candidate =
                        spark_in_list(col("v"), list.clone(), negated, &schema, false).unwrap();
                    let expr::Expr::InList(in_list) = &candidate else {
                        unreachable!()
                    };
                    let mut normalizers = 0;
                    in_list.expr.apply(|e| {
                        if let expr::Expr::ScalarFunction(f) = e {
                            normalizers += usize::from(f.func.inner().is::<SparkComparisonFloat>());
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    assert_eq!(normalizers, usize::from(!skip), "{ty} {name}");
                    assert_eq!(
                        in_list.list.len(),
                        list.len(),
                        "preserve native IN dispatch"
                    );
                    if matches!(*name, "zero" | "zero_nullable" | "zero_long") {
                        assert!(
                            in_list.list.iter().all(|value| {
                                !matches!(value, expr::Expr::ScalarFunction(f)
                                if f.func.inner().is::<SparkComparisonFloat>())
                            }),
                            "canonical constants do not need normalization: {ty} {name}"
                        );
                    }
                    if *skip && expected.contains(&Some(0.0)) {
                        assert!(
                            matches!(
                                in_list.expr.as_ref(),
                                expr::Expr::BinaryExpr(BinaryExpr {
                                    op: Operator::Plus,
                                    ..
                                })
                            ),
                            "{ty} {name}"
                        );
                    } else if *skip && ty == DataType::Float64 {
                        // An identity cast can hide native IN and inverse-Decimal rewrites.
                        assert_eq!(in_list.expr.as_ref(), &col("v"));
                    }
                    let normalized =
                        ScalarUDF::from(SparkComparisonFloat { widen: false }).call(vec![col("v")]);
                    let baseline =
                        spark_in_list(normalized, list.clone(), negated, &schema, false).unwrap();
                    assert_eq!(baseline.get_type(&schema)?, candidate.get_type(&schema)?);
                    assert_eq!(baseline.nullable(&schema)?, candidate.nullable(&schema)?);
                    for expression in [baseline, candidate] {
                        let expression =
                            simplifier.simplify(simplifier.coerce(expression, &schema)?)?;
                        let result =
                            create_physical_expr(&expression, &schema, &ExecutionProps::new())?
                                .evaluate(&batch)?
                                .into_array(rows)?;
                        for (i, value) in wide.as_primitive::<Float64Type>().iter().enumerate() {
                            let found = if expected.is_empty() && *name != "dynamic" {
                                Some(false)
                            } else {
                                value.and_then(|v| {
                                    if *name == "dynamic"
                                        || expected
                                            .iter()
                                            .flatten()
                                            .any(|w| v == *w || (v.is_nan() && w.is_nan()))
                                    {
                                        Some(true)
                                    } else if expected.contains(&None) {
                                        None
                                    } else {
                                        Some(false)
                                    }
                                })
                            }
                            .map(|v| v != negated);
                            assert_eq!(
                                result.as_boolean().is_null(i),
                                found.is_none(),
                                "{ty} {name} {i}"
                            );
                            if let Some(found) = found {
                                assert_eq!(result.as_boolean().value(i), found, "{ty} {name} {i}");
                            }
                            checked_rows += 1;
                        }
                        checks += 1;
                    }
                }
            }
        }
    }
    assert_eq!(checks, 960);
    assert_eq!(checked_rows, 3937920);
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkComparisonFloat {
    pub(crate) widen: bool,
}

pub(crate) fn normalize_spark_float32(value: f32) -> f32 {
    if value.is_nan() {
        f32::NAN
    } else {
        value + 0.0
    }
}

pub(crate) fn normalize_spark_float64(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else {
        value + 0.0
    }
}

impl ScalarUDFImpl for SparkComparisonFloat {
    fn name(&self) -> &str {
        "spark_comparison_float"
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        static SIGNATURE: std::sync::LazyLock<datafusion_expr::Signature> =
            std::sync::LazyLock::new(|| {
                datafusion_expr::Signature::uniform(
                    1,
                    vec![DataType::Float32, DataType::Float64],
                    datafusion_expr::Volatility::Immutable,
                )
            });
        &SIGNATURE
    }

    fn return_type(&self, args: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(if self.widen {
            DataType::Float64
        } else {
            args[0].clone()
        })
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> datafusion_common::Result<arrow::datatypes::FieldRef> {
        Ok(Arc::new(
            arrow::datatypes::Field::new(
                self.name(),
                self.return_type(&[args.arg_fields[0].data_type().clone()])?,
                args.arg_fields[0].is_nullable(),
            )
            .with_metadata(args.arg_fields[0].metadata().clone()),
        ))
    }

    fn simplify(
        &self,
        args: Vec<expr::Expr>,
        info: &datafusion_expr::simplify::SimplifyContext,
    ) -> datafusion_common::Result<datafusion_expr::simplify::ExprSimplifyResult> {
        use datafusion_expr::simplify::ExprSimplifyResult;
        let canonical = match without_metadata_annotations(&args[0]) {
            expr::Expr::ScalarFunction(f) => f.func.inner().is::<Self>(),
            expr::Expr::Cast(expr::Cast { expr, field })
            | expr::Expr::TryCast(expr::TryCast { expr, field }) => {
                let source = info.get_data_type(expr)?;
                // Integer casts cannot produce -0 or NaN. Decimal-to-DOUBLE
                // cannot underflow; Decimal-to-FLOAT can, so keep its check.
                source.is_integer()
                    || (source.is_decimal() && field.data_type() == &DataType::Float64)
            }
            _ => false,
        };
        if !canonical {
            return Ok(ExprSimplifyResult::Original(args));
        }
        let [arg] = datafusion_common::utils::take_function_args(self.name(), args)?;
        Ok(ExprSimplifyResult::Simplified(
            if self.widen && info.get_data_type(&arg)? == DataType::Float32 {
                cast(arg, DataType::Float64)
            } else {
                arg
            },
        ))
    }

    fn invoke_with_args(
        &self,
        input: datafusion_expr::ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        use arrow::array::AsArray;
        use arrow::datatypes::{Float32Type, Float64Type};
        use datafusion_expr::ColumnarValue;
        // Fuse FLOAT widening with normalization; evaluate the operand once.
        // Adding positive zero clears the sign of -0; NaNs still need explicit canonicalization.
        let normalize = normalize_spark_float64;
        let [value] = datafusion_common::utils::take_function_args(self.name(), input.args)?;
        if !self.widen && value.data_type() == DataType::Float32 {
            let normalize = normalize_spark_float32;
            return Ok(match value {
                ColumnarValue::Scalar(ScalarValue::Float32(v)) => {
                    ColumnarValue::Scalar(ScalarValue::Float32(v.map(normalize)))
                }
                ColumnarValue::Array(a) => ColumnarValue::Array(Arc::new(
                    a.as_primitive::<Float32Type>()
                        .unary::<_, Float32Type>(normalize),
                )),
                _ => unreachable!("FLOAT operand checked above"),
            });
        }
        Ok(match value {
            ColumnarValue::Scalar(ScalarValue::Float32(v)) => {
                ColumnarValue::Scalar(ScalarValue::Float64(v.map(|v| normalize(f64::from(v)))))
            }
            ColumnarValue::Scalar(ScalarValue::Float64(v)) => {
                ColumnarValue::Scalar(ScalarValue::Float64(v.map(normalize)))
            }
            ColumnarValue::Array(a) if a.data_type() == &DataType::Float32 => {
                ColumnarValue::Array(Arc::new(
                    a.as_primitive::<Float32Type>()
                        .unary::<_, Float64Type>(|v| normalize(f64::from(v))),
                ))
            }
            ColumnarValue::Array(a) if a.data_type() == &DataType::Float64 => {
                ColumnarValue::Array(Arc::new(
                    a.as_primitive::<Float64Type>()
                        .unary::<_, Float64Type>(normalize),
                ))
            }
            _ => {
                return datafusion_common::internal_err!(
                    "expected FLOAT or DOUBLE comparison operand"
                );
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct NormalizeFloatArrayForSparkComparison;

impl ScalarUDFImpl for NormalizeFloatArrayForSparkComparison {
    fn name(&self) -> &str {
        "spark_normalize_float_array_for_comparison"
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        static SIGNATURE: std::sync::LazyLock<datafusion_expr::Signature> =
            std::sync::LazyLock::new(|| {
                datafusion_expr::Signature::array(datafusion_expr::Volatility::Immutable)
            });
        &SIGNATURE
    }

    fn return_type(&self, args: &[DataType]) -> datafusion_common::Result<DataType> {
        match args {
            [
                data_type @ (DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _)
                | DataType::ListView(field)
                | DataType::LargeListView(field)),
            ] if matches!(field.data_type(), DataType::Float32 | DataType::Float64) => {
                Ok(data_type.clone())
            }
            _ => datafusion_common::plan_err!("expected an array of FLOAT or DOUBLE values"),
        }
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> datafusion_common::Result<arrow::datatypes::FieldRef> {
        self.return_type(&[args.arg_fields[0].data_type().clone()])?;
        Ok(Arc::new(
            args.arg_fields[0].as_ref().clone().with_name(self.name()),
        ))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        self.return_type(
            &args
                .args
                .iter()
                .map(|value| value.data_type())
                .collect::<Vec<_>>(),
        )?;
        crate::function::float_keys::NormalizeNestedFloatKeys::default().invoke_with_args(args)
    }
}

#[test]
fn floating_array_comparison_only_normalizes_visible_values() -> datafusion_common::Result<()> {
    use arrow::array::{Array, AsArray, ListArray};
    use arrow::datatypes::Float64Type;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

    let backing = ListArray::from_iter_primitive::<Float64Type, _, _>(
        (0..2048).map(|_| Some(vec![Some(-0.0), None])),
    );
    for value_type in [DataType::Float32, DataType::Float64] {
        let field = Arc::new(arrow::datatypes::Field::new(
            "item",
            value_type.clone(),
            true,
        ));
        for array_type in [DataType::List(field.clone()), DataType::LargeList(field)] {
            let backing = arrow::compute::cast(&backing, &array_type)?;
            for len in [0, 1, 2] {
                let slice = backing.slice(1001, len);
                let field = Arc::new(arrow::datatypes::Field::new("a", array_type.clone(), false));
                let result = NormalizeFloatArrayForSparkComparison
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![ColumnarValue::Array(slice)],
                        arg_fields: vec![field.clone()],
                        number_rows: len,
                        return_field: field,
                        config_options: Arc::default(),
                    })?
                    .into_array(len)?;
                assert_eq!(result.data_type(), &array_type);
                assert_eq!(result.len(), len);
                assert_eq!(result.null_count(), 0);
                let child = arrow::array::make_array(result.to_data().child_data()[0].clone());
                assert_eq!(
                    child.len(),
                    len * 2,
                    "unused values retained for {array_type}"
                );
                assert_eq!(child.null_count(), len);
                let child = arrow::compute::cast(&child, &DataType::Float64)?;
                for (index, value) in child.as_primitive::<Float64Type>().iter().enumerate() {
                    assert_eq!(value.map(f64::to_bits), (index % 2 == 0).then_some(0));
                }
            }
        }
    }
    Ok(())
}

#[test]
fn comparison_float_preserves_values_nulls_and_shapes() -> datafusion_common::Result<()> {
    use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
    use arrow::datatypes::{Field, Float64Type};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
    let run = |value: ColumnarValue, rows, widen| {
        let return_type = if widen {
            DataType::Float64
        } else {
            value.data_type()
        };
        SparkComparisonFloat { widen }.invoke_with_args(ScalarFunctionArgs {
            arg_fields: vec![Arc::new(Field::new("x", value.data_type(), true))],
            args: vec![value],
            number_rows: rows,
            return_field: Arc::new(Field::new("x", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    };
    let values = [
        Some(-0.0),
        Some(0.0),
        None,
        Some(-0.1),
        Some(0.1),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
        Some(-f64::NAN),
        Some(f64::from_bits(0x7ff0_0000_0000_0001)),
        Some(f64::from_bits(0xfff0_0000_0000_1234)),
        Some(1e-40),
        Some(-1e-40),
    ];
    let arrays: [ArrayRef; 2] = [
        Arc::new(Float32Array::from_iter(values.map(|v| v.map(|v| v as f32)))),
        Arc::new(Float64Array::from_iter(values)),
    ];
    let mut arrays = arrays.to_vec();
    // Exercise IEEE boundaries and arbitrary bit patterns without a random dependency.
    let mut bits = vec![
        0,
        1,
        0x000f_ffff_ffff_ffff,
        0x0010_0000_0000_0000,
        0x7fef_ffff_ffff_ffff,
        0x7ff0_0000_0000_0000,
        0x7ff0_0000_0000_0001,
        0x7ff8_0000_0000_0000,
        0x7fff_ffff_ffff_ffff,
        0x0000_0001_0000_0000,
        0x007f_ffff_0000_0000,
        0x0080_0000_0000_0000,
        0x7f7f_ffff_0000_0000,
        0x7f80_0000_0000_0000,
        0x7f80_0001_0000_0000,
        0x7fc0_0000_0000_0000,
        0x7fff_ffff_0000_0000,
    ];
    bits.extend(bits.clone().into_iter().map(|v| v | (1 << 63)));
    let mut state = 0x1234_5678_9abc_def0_u64;
    bits.extend((0..8192).map(|_| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    }));
    arrays.push(Arc::new(Float32Array::from_iter(
        bits.iter()
            .enumerate()
            .map(|(i, &v)| (i % 19 != 18).then_some(f32::from_bits((v >> 32) as u32))),
    )));
    arrays.push(Arc::new(Float64Array::from_iter(
        bits.iter()
            .enumerate()
            .map(|(i, &v)| (i % 19 != 18).then_some(f64::from_bits(v))),
    )));
    for input in arrays {
        for widen in [false, true] {
            for (offset, len) in [(0, input.len()), (1, 10), (1, input.len() - 1), (0, 0)] {
                let input = input.slice(offset, len);
                let output =
                    run(ColumnarValue::Array(input.clone()), len, widen)?.into_array(len)?;
                assert_eq!(
                    output.data_type(),
                    if widen {
                        &DataType::Float64
                    } else {
                        input.data_type()
                    }
                );
                let output = arrow::compute::cast(&output, &DataType::Float64)?;
                let actual = output.as_primitive::<Float64Type>();
                let widened = arrow::compute::cast(&input, &DataType::Float64)?;
                for (i, value) in widened.as_primitive::<Float64Type>().iter().enumerate() {
                    let expected = value.map(|v| {
                        if v.is_nan() {
                            f64::NAN.to_bits()
                        } else if v == 0.0 {
                            0
                        } else {
                            v.to_bits()
                        }
                    });
                    assert_eq!(actual.is_null(i), expected.is_none());
                    if let Some(bits) = expected {
                        assert_eq!(actual.value(i).to_bits(), bits);
                    }
                    let scalar = ScalarValue::try_from_array(&input, i)?;
                    let ColumnarValue::Scalar(result) =
                        run(ColumnarValue::Scalar(scalar), 1, widen)?
                    else {
                        panic!("expected scalar")
                    };
                    assert_eq!(
                        result.data_type(),
                        if widen {
                            DataType::Float64
                        } else {
                            input.data_type().clone()
                        }
                    );
                    let ScalarValue::Float64(result) = result.cast_to(&DataType::Float64)? else {
                        panic!("expected floating scalar")
                    };
                    assert_eq!(result.map(f64::to_bits), expected);
                }
            }
        }
    }
    Ok(())
}

/// Aggregate function input components, excluding the function name.
///
/// Populated by the resolver from `spec::UnresolvedFunction` after resolving spec expressions
/// to DataFusion expressions. Used by aggregate function builders to produce the final
/// DataFusion aggregate expression.
pub struct AggFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub distinct: bool,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<expr::Expr>>,
    pub order_by: Vec<expr::Sort>,
    pub function_context: FunctionContextInput<'a>,
}

/// Builds a DataFusion aggregate expression from resolved function components.
///
/// Takes the resolved arguments, modifiers (DISTINCT, FILTER, ORDER BY), and context,
/// and produces a `datafusion_expr::Expr::AggregateFunction` ready for inclusion in the logical plan.
pub(crate) type AggFunction = Arc<dyn Fn(AggFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

/// Factory methods for creating `AggFunction`s.
///
/// Provides different ways to build aggregate function handlers:
/// - `default`: Use an existing DataFusion UDAF directly
/// - `custom`: Provide custom logic for building the expression
/// - `unknown`: Placeholder for unimplemented functions
pub(crate) struct AggFunctionBuilder;

impl AggFunctionBuilder {
    /// Converts a DataFusion UDAF factory into an AggFunction.
    ///
    /// Passes all resolved components through directly with no transformation.
    pub fn default<F>(f: F) -> AggFunction
    where
        F: Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let AggFunctionInput {
                arguments,
                distinct,
                ignore_nulls,
                filter,
                order_by,
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::AggregateFunction(AggregateFunction {
                func: f(),
                params: AggregateFunctionParams {
                    args: arguments,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
            }))
        })
    }

    /// Wraps a custom function that builds the aggregate expression with full control.
    ///
    /// Use when the function needs special argument handling that `default` cannot provide.
    pub fn custom<F>(f: F) -> AggFunction
    where
        F: Fn(AggFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> AggFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}

pub struct WinFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub partition_by: Vec<expr::Expr>,
    pub order_by: Vec<expr::Sort>,
    pub window_frame: WindowFrame,
    pub ignore_nulls: Option<bool>,
    pub distinct: bool,
    pub filter: Option<Box<expr::Expr>>,
    pub function_context: FunctionContextInput<'a>,
}

pub(crate) type WinFunction = Arc<dyn Fn(WinFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct WinFunctionBuilder;

impl WinFunctionBuilder {
    pub fn aggregate<F>(f: F) -> WinFunction
    where
        F: Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let WinFunctionInput {
                arguments,
                partition_by,
                order_by,
                window_frame,
                ignore_nulls,
                distinct,
                filter,
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::AggregateUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    filter,
                    null_treatment,
                    distinct,
                },
            })))
        })
    }

    pub fn window<F>(f: F) -> WinFunction
    where
        F: Fn() -> Arc<WindowUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let WinFunctionInput {
                arguments,
                partition_by,
                order_by,
                window_frame,
                ignore_nulls,
                distinct,
                filter,
                function_context,
            } = input;
            if filter.is_some() {
                return Err(PlanError::invalid(
                    "FILTER is only supported for aggregate window functions",
                ));
            }
            let null_treatment = get_null_treatment(ignore_nulls);
            let win_func_expr = expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::WindowUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    filter,
                    null_treatment,
                    distinct,
                },
            }));
            Ok(match win_func_expr.get_type(function_context.schema)? {
                DataType::UInt64 => cast(win_func_expr.clone(), DataType::Int32),
                _ => win_func_expr,
            })
        })
    }

    pub fn custom<F>(f: F) -> WinFunction
    where
        F: Fn(WinFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> WinFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("unknown window function: {name}"))))
    }
}

pub(crate) fn get_null_treatment(ignore_nulls: Option<bool>) -> Option<NullTreatment> {
    match ignore_nulls {
        Some(true) => Some(NullTreatment::IgnoreNulls),
        Some(false) => Some(NullTreatment::RespectNulls),
        None => None,
    }
}

pub(crate) fn get_arguments_and_null_treatment(
    args: Vec<expr::Expr>,
    ignore_nulls: Option<bool>,
) -> PlanResult<(Vec<expr::Expr>, Option<NullTreatment>)> {
    if args.len() == 1 {
        let expr = args.one()?;
        Ok((vec![expr], get_null_treatment(ignore_nulls)))
    } else if args.len() == 2 {
        if ignore_nulls.is_some() {
            return Err(PlanError::invalid(
                "arguments conflict with IGNORE NULLS clause",
            ));
        }
        let (expr, ignore_nulls) = args.two()?;
        let null_treatment = match ignore_nulls {
            expr::Expr::Literal(ScalarValue::Boolean(Some(ignore_nulls)), _metadata) => {
                if ignore_nulls {
                    Some(NullTreatment::IgnoreNulls)
                } else {
                    Some(NullTreatment::RespectNulls)
                }
            }
            _ => {
                return Err(PlanError::invalid(
                    "requires a boolean literal as the second argument",
                ));
            }
        };
        Ok((vec![expr], null_treatment))
    } else {
        Err(PlanError::invalid("requires 1 or 2 arguments"))
    }
}

pub(crate) fn hll_args_with_default_lg(
    arguments: Vec<expr::Expr>,
    function_name: &str,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(DEFAULT_HLL_LG_CONFIG_K)])
        }
        2 => {
            let (value, lg_config_k) = arguments.two()?;
            Ok(vec![value, cast(lg_config_k, DataType::Int32)])
        }
        count => Err(PlanError::invalid(format!(
            "{function_name} requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub(crate) fn hll_union_args_with_default_allow_different_lg(
    arguments: Vec<expr::Expr>,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(false)])
        }
        2 => {
            let (value, allow_different_lg_config_k) = arguments.two()?;
            Ok(vec![
                value,
                cast(allow_different_lg_config_k, DataType::Boolean),
            ])
        }
        count => Err(PlanError::invalid(format!(
            "hll_union_agg requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub(crate) fn count_min_sketch_args(arguments: Vec<expr::Expr>) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        4 => {
            let (value, eps, confidence, seed) = arguments.four()?;
            // Spark requires `eps` and `confidence` to be DOUBLE literals and rejects
            // other types (including DECIMAL/FLOAT). Pass them through unchanged so the
            // aggregate's type validation enforces the same rule, instead of casting and
            // silently accepting types Spark rejects.
            Ok(vec![value, eps, confidence, seed])
        }
        count => Err(PlanError::invalid(format!(
            "count_min_sketch requires 4 arguments, got {count}"
        ))),
    }
}

pub(crate) fn theta_args_with_default_lg(
    arguments: Vec<expr::Expr>,
    function_name: &str,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(DEFAULT_THETA_LG_NOM_ENTRIES)])
        }
        2 => {
            let (value, lg_nom_entries) = arguments.two()?;
            Ok(vec![value, cast(lg_nom_entries, DataType::Int32)])
        }
        count => Err(PlanError::invalid(format!(
            "{function_name} requires 1 or 2 arguments, got {count}"
        ))),
    }
}

// TODO: Match Catalyst constant folding and NullPropagation before extracting opaque
//  Variant scalar calls, so a foldable NULL can eliminate them.
pub fn expr_contains_spark_cast_to_variant(body: &expr::Expr) -> PlanResult<bool> {
    Ok(body.exists(|expression| {
        Ok(matches!(
            expression,
            expr::Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkCastToVariant>()
        ))
    })?)
}

#[test]
fn comparison_float_simplifies_only_canonical_inputs() -> datafusion_common::Result<()> {
    use arrow::array::{
        ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, Float32Array,
        Float64Array, Int64Array, UInt64Array,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::{create_physical_expr, execution_props::ExecutionProps};
    use datafusion_common::{DFSchema, scalar::ScalarValue};
    use datafusion_expr::col;
    use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};

    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(1),
            Some(i64::MAX),
            None,
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(1),
            Some(u64::MAX),
            None,
        ])),
        Arc::new(Float32Array::from(vec![
            Some(-0.0),
            Some(f32::from_bits(0xffc00001)),
            Some(1.5),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(f64::from_bits(0xfff8000000000001)),
            Some(1.5),
            None,
        ])),
    ];
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
    ] {
        arrays.push(arrow::compute::cast(arrays[0].as_ref(), &data_type)?);
    }
    for scale in [-128, 0, 9] {
        arrays.push(Arc::new(
            Decimal32Array::from(vec![
                Some(-999_999_999),
                Some(-1),
                Some(0),
                Some(1),
                Some(999_999_999),
                None,
            ])
            .with_precision_and_scale(9, scale)?,
        ));
    }
    for scale in [-128, 0, 18] {
        arrays.push(Arc::new(
            Decimal64Array::from(vec![
                Some(-999_999_999_999_999_999),
                Some(-1),
                Some(0),
                Some(1),
                Some(999_999_999_999_999_999),
                None,
            ])
            .with_precision_and_scale(18, scale)?,
        ));
    }
    for scale in [-128, 0, 38] {
        let max = 10_i128.pow(38) - 1;
        arrays.push(Arc::new(
            Decimal128Array::from(vec![
                Some(-max),
                Some(-1),
                Some(0),
                Some(1),
                Some(max),
                None,
            ])
            .with_precision_and_scale(38, scale)?,
        ));
    }
    for scale in [-128, 0, 76] {
        let max = "9".repeat(76).parse::<arrow::datatypes::i256>().unwrap();
        arrays.push(Arc::new(
            Decimal256Array::from(vec![
                Some(-max),
                Some(arrow::datatypes::i256::from_i128(-1)),
                Some(arrow::datatypes::i256::ZERO),
                Some(arrow::datatypes::i256::ONE),
                Some(max),
                None,
            ])
            .with_precision_and_scale(76, scale)?,
        ));
    }
    for input in arrays {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            input.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![input.clone()])?;
        let schema = Arc::new(DFSchema::try_from(schema)?);
        let info = SimplifyContext::builder()
            .with_schema(schema.clone())
            .build();
        for data_type in [DataType::Float32, DataType::Float64] {
            for safe in [false, true] {
                let cast = if safe {
                    datafusion_expr::try_cast(col("v"), data_type.clone())
                } else {
                    cast(col("v"), data_type.clone())
                };
                for nested in [false, true] {
                    let arg = if nested {
                        ScalarUDF::from(SparkComparisonFloat { widen: false })
                            .call(vec![cast.clone()])
                    } else {
                        cast.clone()
                    };
                    for widen in [false, true] {
                        let function = ScalarUDF::from(SparkComparisonFloat { widen });
                        let original = function.call(vec![arg.clone()]);
                        let result = function.simplify(vec![arg.clone()], &info)?;
                        let should_simplify = nested
                            || input.data_type().is_integer()
                            || (input.data_type().is_decimal() && data_type == DataType::Float64);
                        assert_eq!(
                            matches!(&result, ExprSimplifyResult::Simplified(_)),
                            should_simplify
                        );
                        let simplified = match result {
                            ExprSimplifyResult::Simplified(e) => e,
                            ExprSimplifyResult::Original(args) => function.call(args),
                        };
                        assert_eq!(original.get_type(&schema)?, simplified.get_type(&schema)?);
                        assert_eq!(original.nullable(&schema)?, simplified.nullable(&schema)?);
                        let evaluate = |e: &expr::Expr| {
                            create_physical_expr(e, &schema, &ExecutionProps::new())?
                                .evaluate(&batch)?
                                .into_array(batch.num_rows())
                        };
                        let a = evaluate(&original)?;
                        let b = evaluate(&simplified)?;
                        for i in 0..batch.num_rows() {
                            let bits =
                                |array: &ArrayRef| -> datafusion_common::Result<Option<u64>> {
                                    let ScalarValue::Float64(value) =
                                        ScalarValue::try_from_array(array, i)?
                                            .cast_to(&DataType::Float64)?
                                    else {
                                        unreachable!()
                                    };
                                    Ok(value.map(f64::to_bits))
                                };
                            assert_eq!(
                                bits(&a)?,
                                bits(&b)?,
                                "input={}, target={data_type}, safe={safe}, nested={nested}, widen={widen}, row={i}",
                                input.data_type()
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
fn ansi_float_integer_predicates_preserve_the_single_precision_boundary() -> PlanResult<()> {
    use arrow::array::{AsArray, Float32Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::DFSchema;
    use datafusion_expr::{col, execution_props::ExecutionProps, simplify::SimplifyContext};

    let schema = Arc::new(Schema::new(vec![
        Field::new("f", DataType::Float32, true),
        Field::new("i", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from(vec![
                Some(16777216.),
                Some(-16777216.),
                Some(16777216.),
                Some(1.5),
                Some(-0.),
                None,
                Some(1.),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(16777217),
                Some(-16777217),
                Some(16777216),
                Some(1),
                Some(0),
                Some(1),
                None,
            ])),
        ],
    )?;
    let schema = Arc::new(DFSchema::try_from(schema)?);
    for ansi_mode in [false, true] {
        let expected = vec![
            Some(!ansi_mode),
            Some(!ansi_mode),
            Some(true),
            Some(false),
            Some(true),
            None,
            None,
        ];
        for (left, right) in [(col("f"), col("i")), (col("i"), col("f"))] {
            for expression in [
                spark_binary_expr(
                    left.clone(),
                    Operator::Eq,
                    right.clone(),
                    &schema,
                    &PlanConfig {
                        ansi_mode,
                        ..Default::default()
                    },
                )?,
                spark_in_list(left, vec![right], false, &schema, ansi_mode)?,
            ] {
                let simplifier = ExprSimplifier::new(
                    SimplifyContext::builder()
                        .with_schema(schema.clone())
                        .build(),
                );
                let expression = simplifier.coerce(expression, &schema)?;
                let actual = create_physical_expr(&expression, &schema, &ExecutionProps::new())?
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?;
                assert_eq!(
                    actual.as_boolean().iter().collect::<Vec<_>>(),
                    expected,
                    "ANSI={ansi_mode}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn exact_integer_predicates_match_explicit_double_coercion() -> PlanResult<()> {
    use arrow::array::{Array, ArrayRef, Float32Array, Int8Array, Int16Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::DFSchema;
    use datafusion_expr::{col, execution_props::ExecutionProps, simplify::SimplifyContext};

    let floats = vec![
        Some(f32::NEG_INFINITY),
        Some(-9223372036854775808.),
        Some(-16777218.),
        Some(-16777216.),
        Some(-32768.),
        Some(-128.),
        Some(-1.5),
        Some(-0.),
        Some(0.),
        Some(1.),
        Some(1.5),
        Some(127.),
        Some(32767.),
        Some(16777216.),
        Some(16777218.),
        Some(33554432.),
        Some(33554436.),
        Some(9007199254740992.),
        Some(9223372036854775808.),
        Some(f32::INFINITY),
        Some(f32::NAN),
        None,
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("f", DataType::Float32, true),
        Field::new("tiny", DataType::Int8, true),
        Field::new("small", DataType::Int16, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from(floats.clone())),
            Arc::new(Int8Array::from_iter((0..floats.len()).map(|i| {
                (i % 5 != 0).then_some(if i % 2 == 0 { i8::MIN } else { i8::MAX })
            }))),
            Arc::new(Int16Array::from_iter((0..floats.len()).map(|i| {
                (i % 7 != 0).then_some(if i % 2 == 0 { i16::MIN } else { i16::MAX })
            }))),
        ],
    )?;
    let schema = Arc::new(DFSchema::try_from(schema)?);
    let evaluate = |expression: expr::Expr| -> PlanResult<ArrayRef> {
        let simplifier = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(schema.clone())
                .build(),
        );
        let expression = simplifier.coerce(expression, &schema)?;
        Ok(
            create_physical_expr(&expression, &schema, &ExecutionProps::new())?
                .evaluate(&batch)?
                .into_array(batch.num_rows())?,
        )
    };
    let mut integers = vec![
        col("tiny"),
        col("small"),
        lit(ScalarValue::Int32(None)),
        lit(ScalarValue::Int64(None)),
    ];
    for value in [
        0_i64,
        1,
        -1,
        16777215,
        16777216,
        16777217,
        16777218,
        -16777217,
        33554431,
        33554432,
        33554434,
        33554436,
        9007199254740991,
        9007199254740992,
        9007199254740993,
        9007199254740994,
        i64::MIN,
        i64::MIN + 1,
        i64::MAX,
    ] {
        integers.push(lit(value));
        if let Ok(value) = i32::try_from(value) {
            integers.push(lit(value));
        }
    }
    for integer in integers {
        for (left, right) in [(col("f"), integer.clone()), (integer, col("f"))] {
            let wide = |expr: expr::Expr| -> PlanResult<expr::Expr> {
                Ok(if expr.get_type(&schema)? == DataType::Float32 {
                    ScalarUDF::from(SparkComparisonFloat { widen: true }).call(vec![expr])
                } else {
                    cast(expr, DataType::Float64)
                })
            };
            let (wide_left, wide_right) = (wide(left.clone())?, wide(right.clone())?);
            for op in [
                Operator::Eq,
                Operator::NotEq,
                Operator::Lt,
                Operator::LtEq,
                Operator::Gt,
                Operator::GtEq,
                Operator::IsDistinctFrom,
                Operator::IsNotDistinctFrom,
            ] {
                let candidate = spark_binary_expr(
                    left.clone(),
                    op,
                    right.clone(),
                    &schema,
                    &PlanConfig {
                        ansi_mode: true,
                        ..Default::default()
                    },
                )?;
                let reference = expr::Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(wide_left.clone()),
                    op,
                    Box::new(wide_right.clone()),
                ));
                assert_eq!(
                    evaluate(candidate)?.to_data(),
                    evaluate(reference)?.to_data()
                );
            }
            for negated in [false, true] {
                let candidate =
                    spark_in_list(left.clone(), vec![right.clone()], negated, &schema, true)?;
                let reference = expr::Expr::InList(expr::InList::new(
                    Box::new(wide_left.clone()),
                    vec![wide_right.clone()],
                    negated,
                ));
                assert_eq!(
                    evaluate(candidate)?.to_data(),
                    evaluate(reference)?.to_data()
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn metadata_annotations_preserve_expression_placement() {
    use datafusion_expr::ExpressionPlacement;

    for hidden_sort_output_type in [None, Some(DataType::Int32)] {
        let annotation = SparkResultMetadata {
            metadata: FieldMetadata::default(),
            lifetime: ResultMetadataLifetime::Permanent,
            hidden_sort_output_type,
        };
        for placement in [
            ExpressionPlacement::Literal,
            ExpressionPlacement::Column,
            ExpressionPlacement::MoveTowardsLeafNodes,
            ExpressionPlacement::KeepInPlace,
        ] {
            assert_eq!(annotation.placement(&[placement]), placement);
        }
    }
}

#[cfg(test)]
#[test]
fn metadata_value_analysis_uses_implementation_identity() {
    use datafusion_expr::{col, create_udf};

    let value = col("k");
    let annotated = set_result_metadata(
        drop_inherited_column_metadata(value.clone()),
        vec![(String::new(), "empty-key".into())],
    );
    assert_eq!(without_metadata_annotations(&annotated), &value);
    let user_function = create_udf(
        "spark_result_metadata",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|arguments| Ok(arguments[0].clone())),
    )
    .call(vec![value]);
    assert_eq!(without_metadata_annotations(&user_function), &user_function);
    assert_eq!(
        without_inherited_column_metadata(&user_function),
        &user_function
    );
}

#[cfg(test)]
#[test]
fn plain_metadata_clearing_peel_preserves_other_annotations() {
    use datafusion_expr::col;

    let value = col("k");
    let plain = drop_inherited_column_metadata(drop_inherited_column_metadata(value.clone()));
    assert_eq!(without_inherited_column_metadata(&plain), &value);
    let field = Arc::new(Field::new("k", DataType::Int32, true));
    for annotation in [
        set_result_metadata(value.clone(), vec![("source".into(), "stored".into())]),
        drop_inherited_column_metadata_for_analysis(value.clone()),
        drop_scalar_subquery_metadata(value.clone(), &field),
        preserve_hidden_sort_output_declaration(value, &field),
    ] {
        assert_eq!(without_inherited_column_metadata(&annotation), &annotation);
        let plain = drop_inherited_column_metadata(annotation.clone());
        assert_eq!(without_inherited_column_metadata(&plain), &annotation);
    }
}

#[cfg(test)]
#[test]
fn analysis_metadata_annotations_restore_the_original_value() -> datafusion_common::Result<()> {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion_expr::simplify::SimplifyContext;

    let source = HashMap::from([("source".into(), "provider".into())]);
    let schema = Arc::new(DFSchema::try_from(Schema::new(vec![
        Field::new("k", DataType::Int32, false).with_metadata(source.clone()),
    ]))?);
    let info = SimplifyContext::builder()
        .with_schema(Arc::clone(&schema))
        .build();
    let column = datafusion_expr::col("k");
    for value in [
        column.clone(),
        expr::Expr::Literal(ScalarValue::Int32(None), Some(source.into())),
    ] {
        let annotated = drop_inherited_column_metadata_for_analysis(value.clone());
        let field = annotated.to_field(schema.as_ref())?.1;
        assert!(field.metadata().is_empty());
        assert_eq!(field.data_type(), &value.get_type(schema.as_ref())?);
        assert_eq!(field.is_nullable(), value.nullable(schema.as_ref())?);
        assert_eq!(without_metadata_annotations(&annotated), &value);
        let prepared = materialize_metadata_literals(annotated.clone())?.data;
        if matches!(value, expr::Expr::Literal(..)) {
            assert_eq!(prepared, value);
        } else {
            assert_eq!(prepared, annotated);
        }
        assert_eq!(ExprSimplifier::new(info.clone()).simplify(prepared)?, value);
    }
    let permanent = drop_inherited_column_metadata(column);
    assert_eq!(
        ExprSimplifier::new(info).simplify(permanent.clone())?,
        permanent
    );
    Ok(())
}

#[cfg(test)]
#[test]
fn scalar_metadata_boundary_expires_only_after_inlining() -> datafusion_common::Result<()> {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion_expr::LogicalPlanBuilder;
    use datafusion_expr::simplify::SimplifyContext;

    let extensions = HashMap::from([
        (EXTENSION_TYPE_NAME_KEY.into(), "arrow.json".into()),
        (EXTENSION_TYPE_METADATA_KEY.into(), "".into()),
    ]);
    let mut source = extensions.clone();
    source.insert("origin".into(), "outer".into());
    let field = Arc::new(Field::new("k", DataType::Utf8, true).with_metadata(source.clone()));
    let schema = Arc::new(DFSchema::try_from(Schema::new(vec![Arc::clone(&field)]))?);
    let subquery = LogicalPlanBuilder::empty(true)
        .project(vec![lit("value").alias("k")])?
        .build()?;
    let scalar = datafusion_expr::expr_fn::scalar_subquery(Arc::new(subquery));
    let boundary = drop_scalar_subquery_metadata(scalar.clone(), &field);
    assert_eq!(
        boundary.to_field(schema.as_ref())?.1.metadata(),
        &extensions
    );
    assert_ne!(boundary, drop_inherited_column_metadata(scalar));
    let info = SimplifyContext::builder()
        .with_schema(Arc::clone(&schema))
        .build();
    assert_eq!(
        ExprSimplifier::new(info).simplify(boundary.clone())?,
        boundary
    );
    let unchanged = restore_inlined_scalar_metadata(boundary.clone());
    assert!(!unchanged.transformed);
    assert_eq!(unchanged.data, boundary);

    // Simulate the child returned by successful guarded one-row substitution.
    let mut inlined = boundary;
    let expr::Expr::ScalarFunction(function) = &mut inlined else {
        return datafusion_common::internal_err!("expected scalar metadata boundary");
    };
    function.args[0] = datafusion_expr::col("k");
    let restored = restore_inlined_scalar_metadata(inlined.clone());
    assert!(restored.transformed);
    assert_eq!(restored.data, datafusion_expr::col("k"));
    assert_eq!(
        restored.data.to_field(schema.as_ref())?.1.metadata(),
        &source
    );
    for supplied in [vec![], vec![("unit".into(), "reading".into())]] {
        let permanent = set_result_metadata(inlined.clone(), supplied.clone());
        let retained = permanent
            .transform_up(|expression| Ok(restore_inlined_scalar_metadata(expression)))?
            .data;
        let mut expected = extensions.clone();
        expected.extend(supplied);
        assert_eq!(retained.to_field(schema.as_ref())?.1.metadata(), &expected);
        assert!(!is_scalar_subquery_metadata_boundary(&retained));
        assert!(!restore_inlined_scalar_metadata(retained).transformed);
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn hidden_sort_output_preserves_nested_declarations() -> datafusion_common::Result<()> {
    use datafusion::arrow::array::{Array, ArrayRef, Int32Array, ListArray, StructArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::Schema;

    let source = HashMap::from([("source".into(), "nested".into())]);
    let leaf = Arc::new(Field::new("leaf", DataType::Int32, true).with_metadata(source.clone()));
    let child = StructArray::try_new(
        vec![Arc::clone(&leaf), leaf].into(),
        vec![
            Arc::new(Int32Array::from(vec![Some(7), None, Some(9)])),
            Arc::new(Int32Array::from(vec![Some(17), Some(18), None])),
        ],
        None,
    )?;
    let item = Arc::new(Field::new("item", child.data_type().clone(), true).with_metadata(source));
    let list = ListArray::try_new(
        item,
        OffsetBuffer::new(vec![0, 2, 3].into()),
        Arc::new(child),
        Some(NullBuffer::from(vec![true, false])),
    )?;
    let source: ArrayRef = Arc::new(StructArray::try_new(
        vec![Arc::new(Field::new(
            "items",
            list.data_type().clone(),
            true,
        ))]
        .into(),
        vec![Arc::new(list)],
        Some(NullBuffer::from(vec![true, false])),
    )?);
    let declared_leaf = Arc::new(Field::new("leaf", DataType::Int32, true));
    let declared_item = Arc::new(Field::new(
        "item",
        DataType::Struct(vec![Arc::clone(&declared_leaf), declared_leaf].into()),
        true,
    ));
    let declared_items = Arc::new(Field::new("items", DataType::List(declared_item), true));
    let declared = Arc::new(Field::new(
        "r",
        DataType::Struct(vec![declared_items].into()),
        true,
    ));
    let source_field = Arc::new(Field::new("k", source.data_type().clone(), true));
    let schema = DFSchema::try_from(Schema::new(vec![Arc::clone(&source_field)]))?;
    let expression = preserve_hidden_sort_output_declaration(datafusion_expr::col("k"), &declared);
    assert!(
        !expression
            .exists(|node| { Ok(matches!(node, expr::Expr::Cast(_) | expr::Expr::TryCast(_))) })?
    );
    let returned = expression.to_field(&schema)?.1;
    assert_eq!(returned.data_type(), declared.data_type());
    assert!(returned.is_nullable());
    let refined = DFSchema::try_from(Schema::new(vec![Field::new(
        "k",
        source.data_type().clone(),
        false,
    )]))?;
    assert!(!expression.to_field(&refined)?.1.is_nullable());
    let expr::Expr::ScalarFunction(function) = expression else {
        return datafusion_common::internal_err!("expected result metadata annotation");
    };
    let result = function
        .func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&source))],
            arg_fields: vec![source_field],
            number_rows: 2,
            return_field: returned,
            config_options: Arc::new(Default::default()),
        })?
        .into_array(2)?;
    assert_eq!(result.data_type(), declared.data_type());
    result.to_data().validate_full()?;
    assert_eq!(result.nulls(), source.nulls());
    let restored = arrow_cast(result.as_ref(), source.data_type())?;
    assert_eq!(restored.to_data(), source.to_data());
    for row in [0, 1] {
        let scalar = ScalarValue::try_from_array(source.as_ref(), row)?;
        let scalar_result = function.func.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(scalar.clone())],
            arg_fields: vec![Arc::new(Field::new(
                "k",
                source.data_type().clone(),
                row == 1,
            ))],
            number_rows: 1,
            return_field: Arc::clone(&declared),
            config_options: Arc::new(Default::default()),
        })?;
        let ColumnarValue::Scalar(scalar_result) = scalar_result else {
            return datafusion_common::internal_err!("expected scalar metadata result");
        };
        assert_eq!(&scalar_result.data_type(), declared.data_type());
        assert_eq!(
            redeclare_scalar_fields(&scalar_result, source.data_type())?,
            scalar
        );
        let annotated = preserve_hidden_sort_output_declaration(lit(scalar), &declared);
        let materialized = materialize_metadata_literals(annotated)?.data;
        let expr::Expr::Literal(value, _) = materialized else {
            return datafusion_common::internal_err!("expected metadata literal");
        };
        assert_eq!(&value.data_type(), declared.data_type());
        value.to_array()?.to_data().validate_full()?;
    }
    let original =
        DataType::Struct(vec![Arc::new(Field::new("leaf", DataType::Int32, true))].into());
    let schema = DFSchema::try_from(Schema::new(vec![Field::new("k", original.clone(), false)]))?;
    for field in [
        Field::new("renamed", DataType::Int32, true),
        Field::new("leaf", DataType::Int64, true),
        Field::new("leaf", DataType::Int32, false),
    ] {
        let changed = DataType::Struct(vec![Arc::new(field)].into());
        assert!(overlay_hidden_sort_nested_fields(&original, &changed).is_err());
        let declared = Arc::new(Field::new("r", changed, false));
        assert!(
            preserve_hidden_sort_output_declaration(datafusion_expr::col("k"), &declared)
                .to_field(&schema)
                .is_err()
        );
    }
    Ok(())
}

#[cfg(test)]
#[test]
// Metadata-only annotations refine nullability; hidden sort outputs retain their declaration.
fn metadata_overlay_preserves_actual_nested_nullability() -> datafusion_common::Result<()> {
    use datafusion::arrow::array::{Array, ArrayRef, Int32Array, StructArray};
    use datafusion::arrow::datatypes::Schema;

    let actual_child = Arc::new(
        Field::new("x", DataType::Int32, false)
            .with_metadata(HashMap::from([("source".into(), "computed".into())])),
    );
    let source: ArrayRef = Arc::new(StructArray::try_new(
        vec![actual_child].into(),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
        None,
    )?);
    let captured_type =
        DataType::Struct(vec![Arc::new(Field::new("x", DataType::Int32, true))].into());
    let captured = Arc::new(Field::new("r", captured_type, true));
    let source_field = Arc::new(Field::new("k", source.data_type().clone(), false));
    let schema = DFSchema::try_from(Schema::new(vec![Arc::clone(&source_field)]))?;
    for (expression, expected_type) in [
        (
            set_result_metadata(datafusion_expr::col("k"), vec![]),
            source.data_type().clone(),
        ),
        (
            preserve_hidden_sort_output_declaration(datafusion_expr::col("k"), &captured),
            captured.data_type().clone(),
        ),
    ] {
        let returned = expression.to_field(&schema)?.1;
        assert_eq!(returned.data_type(), &expected_type);
        assert!(!returned.is_nullable());
        let expr::Expr::ScalarFunction(function) = expression else {
            return datafusion_common::internal_err!("expected result metadata annotation");
        };
        let scalar = ScalarValue::try_from_array(source.as_ref(), 0)?;
        for input in [
            ColumnarValue::Array(Arc::clone(&source)),
            ColumnarValue::Scalar(scalar.clone()),
        ] {
            let result = function.func.invoke_with_args(ScalarFunctionArgs {
                args: vec![input],
                arg_fields: vec![Arc::clone(&source_field)],
                number_rows: 2,
                return_field: Arc::clone(&returned),
                config_options: Arc::new(Default::default()),
            })?;
            assert_eq!(result.data_type(), expected_type);
            let expected = match &result {
                ColumnarValue::Array(_) => Arc::clone(&source),
                ColumnarValue::Scalar(_) => scalar.to_array()?,
            };
            let result = result.into_array(expected.len())?;
            result.to_data().validate_full()?;
            let restored = arrow_cast(result.as_ref(), source.data_type())?;
            assert_eq!(restored.to_data(), expected.to_data());
        }
        let annotated = function.func.call(vec![lit(scalar)]);
        let materialized = materialize_metadata_literals(annotated)?.data;
        let expr::Expr::Literal(value, _) = materialized else {
            return datafusion_common::internal_err!("expected metadata literal");
        };
        assert_eq!(value.data_type(), expected_type);
        value.to_array()?.to_data().validate_full()?;
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn metadata_annotations_keep_null_propagation_and_fields() -> datafusion_common::Result<()> {
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion_expr::create_udf;
    use datafusion_expr::simplify::SimplifyContext;

    let info = SimplifyContext::builder()
        .with_schema(Arc::new(DFSchema::empty()))
        .build();
    let source_metadata = HashMap::from([
        (EXTENSION_TYPE_NAME_KEY.into(), "arrow.json".into()),
        (EXTENSION_TYPE_METADATA_KEY.into(), String::new()),
        ("source".into(), "provider".into()),
    ]);
    let null = expr::Expr::Literal(ScalarValue::Utf8(None), Some(source_metadata.into()));
    let annotated = set_result_metadata(
        drop_inherited_column_metadata(cast(null, DataType::Utf8)),
        vec![
            ("__autoGeneratedAlias".into(), "true".into()),
            (String::new(), "empty-key".into()),
        ],
    );
    let prepared = materialize_metadata_literals(annotated)?.data;
    let simplified = ExprSimplifier::new(info.clone()).simplify(prepared)?;
    let expr::Expr::Literal(value, Some(metadata)) = simplified else {
        return datafusion_common::internal_err!("expected annotated NULL literal");
    };
    assert_eq!(value, ScalarValue::Utf8(None));
    assert_eq!(
        metadata.to_hashmap(),
        HashMap::from([
            (EXTENSION_TYPE_NAME_KEY.into(), "arrow.json".into()),
            (EXTENSION_TYPE_METADATA_KEY.into(), String::new()),
            ("__autoGeneratedAlias".into(), "true".into()),
            (String::new(), "empty-key".into()),
        ])
    );

    let typed_null = drop_inherited_column_metadata(cast(lit(ScalarValue::Null), DataType::Int32));
    assert!(matches!(
        without_metadata_annotations(&typed_null),
        expr::Expr::Cast(_)
    ));
    assert!(matches!(
        materialize_metadata_literals(typed_null.clone())?.data,
        expr::Expr::Literal(ScalarValue::Int32(None), _)
    ));
    let invalid_null_cast = cast(
        lit(ScalarValue::Int32(None)),
        DataType::Struct(Default::default()),
    );
    let annotated_invalid_cast = drop_inherited_column_metadata(invalid_null_cast.clone());
    assert_eq!(
        without_metadata_annotations(&annotated_invalid_cast),
        &invalid_null_cast
    );
    assert_eq!(
        materialize_metadata_literals(annotated_invalid_cast.clone())?.data,
        annotated_invalid_cast
    );
    let bad_cast = cast(lit("bad"), DataType::Int32);
    for (left, right) in [
        (typed_null.clone(), bad_cast.clone()),
        (bad_cast, typed_null.clone()),
    ] {
        let expression = left + right;
        let expression = materialize_metadata_literals(expression)?.data;
        let simplified = ExprSimplifier::new(info.clone()).simplify(expression)?;
        assert!(matches!(
            simplified,
            expr::Expr::Literal(ScalarValue::Int32(None), _)
        ));
    }
    let user_function = create_udf(
        "spark_result_metadata",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(7))))),
    )
    .call(vec![typed_null]);
    let annotated_user_function = set_result_metadata(
        user_function.clone(),
        vec![("__autoGeneratedAlias".into(), "true".into())],
    );
    assert_eq!(
        without_metadata_annotations(&annotated_user_function),
        &user_function
    );
    Ok(())
}
