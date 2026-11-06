// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::functions::expr_fn;
use datafusion::functions::regex::expr_fn as regex_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::regex::regexpinstr::RegexpInstrFunc;
use datafusion::functions::string as native_string;
use datafusion::functions::string::{lower::LowerFunc, upper::UpperFunc};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, ExprSchemable, HigherOrderUDF, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, cast, expr, lit, try_cast, when,
};
use datafusion_functions_nested::expr_fn::array_element;
use datafusion_spark::function::math::expr_fn as math_fn;
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::expr_fn as string_fn;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use datafusion_spark::function::string::length::SparkLengthFunc;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::spark_cast_numeric_to_integer::SparkCastNumericToInteger;
use sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat;
use sail_function::scalar::spark_cast_string_to_integer::{
    SparkCastStringToInteger, StringIntegerCastMode,
};
use sail_function::scalar::spark_decimal_cast::{DecimalCastMode, SparkDecimalCast};
use sail_function::scalar::spark_to_string::SparkToUtf8;
use sail_function::scalar::string::format_number::FormatNumber;
use sail_function::scalar::string::levenshtein::Levenshtein;
use sail_function::scalar::string::make_valid_utf8::MakeValidUtf8;
use sail_function::scalar::string::randstr::Randstr;
use sail_function::scalar::string::soundex::Soundex;
use sail_function::scalar::string::spark_base64::{SparkBase64, SparkUnbase64};
use sail_function::scalar::string::spark_concat_ws::SparkConcatWs;
use sail_function::scalar::string::spark_encode_decode::{SparkDecode, SparkEncode};
use sail_function::scalar::string::spark_length::{SparkBitLength, SparkOctetLength};
use sail_function::scalar::string::spark_mask::SparkMask;
use sail_function::scalar::string::spark_quote::SparkQuote;
use sail_function::scalar::string::spark_regexp_extract_all::{
    SparkRegexpExtract, SparkRegexpExtractAll,
};
use sail_function::scalar::string::spark_regexp_replace::SparkRegexpReplace;
use sail_function::scalar::string::spark_sentences::SparkSentences;
use sail_function::scalar::string::spark_split::SparkSplit;
use sail_function::scalar::string::spark_substring::SparkSubstring;
use sail_function::scalar::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use sail_function::scalar::string::spark_to_char::SparkToChar;
use sail_function::scalar::string::spark_to_number::SparkToNumber;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    ScalarFunction, ScalarFunctionBuilder, ScalarFunctionInput, literal_value_without_evaluation,
    without_inherited_column_metadata, without_metadata_annotations,
};
use crate::function::scalar::datetime::date_format;
use crate::function::scalar::lambda::lambda_with_fresh_parameter;

fn regexp_extract(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    if arguments.len() != 3 {
        return Ok(ScalarUDF::from(SparkRegexpExtract::new()).call(arguments));
    }
    let asynchronous = arguments.iter().try_fold(false, |found, argument| {
        Ok::<_, datafusion_common::DataFusionError>(
            found
                || argument.exists(|expression| {
                    Ok(matches!(expression, expr::Expr::ScalarFunction(function)
                    if function.func.as_async().is_some()))
                })?,
        )
    })?;
    if asynchronous
        || !matches!(
            arguments[2].get_type(function_context.schema)?,
            DataType::Null
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
        )
    {
        // Preserve the existing native validation/extraction path for other types.
        return Ok(ScalarUDF::from(SparkRegexpExtract::new()).call(arguments));
    }
    let known_null = arguments.iter().any(|argument| {
        literal_value_without_evaluation(argument).is_some_and(|value| value.is_null())
    });
    let index = arguments
        .pop()
        .ok_or_else(|| PlanError::internal("missing regexp_extract index"))?;
    let index = cast_string_function_int(
        index,
        function_context.schema,
        function_context.plan_config.ansi_mode,
        "regexp_extract index",
    )?;
    // The existing value kernel consumes Int64 after Spark's INT conversion.
    arguments.push(cast(index, DataType::Int64));
    Ok(ScalarUDF::from(
        SparkRegexpExtract::new()
            .with_null_short_circuit(known_null)
            .with_local_index_check(function_context.plan_config.ansi_mode),
    )
    .call(arguments))
}

fn regexp_replace(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    if !(3..=4).contains(&arguments.len()) {
        return Err(PlanError::invalid(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] regexp_replace requires 3 or 4 arguments, got {}",
            arguments.len()
        )));
    }
    let position = if arguments.len() == 4 {
        arguments
            .pop()
            .ok_or_else(|| PlanError::internal("missing regexp_replace position"))?
    } else {
        lit(1i32)
    };
    let data_type = position.get_type(function_context.schema)?;
    let coerced_position = !matches!(data_type, DataType::Int32 | DataType::Null);
    let position = cast_string_function_int(
        position,
        function_context.schema,
        function_context.plan_config.ansi_mode,
        "regexp_replace position",
    )?;
    if position.is_volatile()
        || !position.column_refs().is_empty()
        || (coerced_position
            && position.exists(|expression| {
                Ok(matches!(expression, expr::Expr::ScalarFunction(function)
                    if !function.func.inner().supports_constant_folding()))
            })?)
    {
        return Err(PlanError::invalid(
            "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] regexp_replace position must be a foldable INT expression",
        ));
    }
    let value = LiteralEvaluator::new().evaluate(&position)?;
    let value = match value {
        ScalarValue::Int32(Some(value)) if value > 0 => ScalarValue::Int32(Some(value)),
        value if value.is_null() => ScalarValue::Int32(None),
        ScalarValue::Int32(Some(value)) => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] regexp_replace position must be in (0, 2147483647], got {value}"
            )));
        }
        value => {
            return Err(PlanError::internal(format!(
                "unexpected regexp_replace position {value:?}"
            )));
        }
    };
    // Keep the cast's analyzed declaration until ordinary optimization folds it.
    // Existing INT inputs retain their already-materialized position path.
    arguments.push(if coerced_position {
        position
    } else {
        lit(value)
    });
    Ok(ScalarUDF::from(SparkRegexpReplace::new()).call(arguments))
}

fn cast_string_function_int(
    argument: expr::Expr,
    schema: &DFSchema,
    ansi_mode: bool,
    parameter: &str,
) -> PlanResult<expr::Expr> {
    Ok(match argument.get_type(schema)? {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let mode = if ansi_mode {
                StringIntegerCastMode::Ansi
            } else {
                StringIntegerCastMode::Legacy
            };
            SparkCastStringToInteger::try_new(DataType::Int32, mode)?.into_expr(argument)
        }
        DataType::Decimal128(..) | DataType::Decimal256(..) => {
            let mode = if ansi_mode {
                DecimalCastMode::Ansi
            } else {
                DecimalCastMode::Legacy
            };
            ScalarUDF::from(SparkDecimalCast::to_integer(DataType::Int32, mode)?)
                .call(vec![argument])
        }
        DataType::Int64 | DataType::Float32 | DataType::Float64 => ScalarUDF::from(
            SparkCastNumericToInteger::try_new(DataType::Int32, ansi_mode, false)?,
        )
        .call(vec![argument]),
        DataType::Null | DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            cast(argument, DataType::Int32)
        }
        data_type => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] {parameter} requires INT, got {data_type}"
            )));
        }
    })
}

fn regexp_substr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (string, pattern) = input
        .arguments
        .two()
        .map_err(|_| PlanError::invalid("regexp_substr requires 2 arguments"))?;
    let wrapped_pattern = expr_fn::concat_ws(lit(""), vec![lit("("), pattern, lit(")")]);
    let matches = regex_fn::regexp_match(string, wrapped_pattern, None);
    Ok(array_element(matches, lit(1i64)))
}

fn substr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let known_null = arguments.iter().any(|argument| {
        literal_value_without_evaluation(argument).is_some_and(|value| value.is_null())
    });
    let length_opt = (arguments.len() == 3).then(|| arguments.pop()).flatten();
    let (value, position) = arguments
        .two()
        .map_err(|_| PlanError::invalid("substr requires 2 or 3 arguments"))?;
    let value = match value.get_type(function_context.schema)? {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => value,
        DataType::FixedSizeBinary(_) => cast(value, DataType::Binary),
        _ => cast_to_logical_string_or_try(value, function_context.schema, false)?,
    };
    let length = length_opt.unwrap_or_else(|| lit(i32::MAX));
    let arguments = [value, position, length]
        .into_iter()
        .enumerate()
        .map(|(index, argument)| {
            let argument = if index == 0 {
                argument
            } else {
                let parameter = if index == 1 {
                    "substring position"
                } else {
                    "substring length"
                };
                let converted = cast_string_function_int(
                    argument,
                    function_context.schema,
                    function_context.plan_config.ansi_mode,
                    parameter,
                )?;
                // The value kernel consumes Int64 after Spark's INT conversion.
                cast(converted, DataType::Int64)
            };
            Ok(argument)
        })
        .collect::<PlanResult<Vec<_>>>()?;
    let async_arguments = arguments.iter().try_fold(false, |found, argument| {
        Ok::<_, datafusion_common::DataFusionError>(
            found
                || argument.exists(|expression| {
                    Ok(matches!(expression, expr::Expr::ScalarFunction(function)
                if function.func.as_async().is_some()))
                })?,
        )
    })?;
    let logical = ScalarUDF::from(
        SparkSubstring::new()
            .with_logical_arguments()
            .with_known_null_argument(known_null)
            .with_async_arguments(async_arguments),
    )
    .call(arguments);
    // Whole constants retain conditional folding; ordinary scalar calls use
    // native NULL selection. Async calls keep their existing extraction path.
    if !async_arguments && !known_null && crate::expression_errors::row_expression(&logical, false)?
    {
        return substring_lambdas(logical, false).map(|value| value.data);
    }
    Ok(logical)
}

fn substring_lambdas(
    expression: expr::Expr,
    pruned_empty_input: bool,
) -> PlanResult<Transformed<expr::Expr>> {
    let expr::Expr::ScalarFunction(function) = &expression else {
        return Ok(Transformed::no(expression));
    };
    if function.args.len() != 3
        || !function
            .func
            .inner()
            .downcast_ref::<SparkSubstring>()
            .is_some_and(SparkSubstring::has_logical_arguments)
    {
        return Ok(Transformed::no(expression));
    }
    // Native lambdas cannot contain async UDFs. Keep their existing physical
    // extraction path instead of introducing an unsupported lambda body.
    if expression.exists(|value| {
        Ok(matches!(value, expr::Expr::ScalarFunction(function)
            if function.func.as_async().is_some()))
    })? {
        return Ok(Transformed::no(expression));
    }
    if !pruned_empty_input && literal_substring_overflow(&function.args)? {
        // Keep the checked cast visible to ordinary optimization. Resolving
        // the call must still return its real public declaration first.
        return Ok(Transformed::no(expression));
    }
    let arguments = function
        .args
        .iter()
        .cloned()
        .map(|argument| lambda_with_fresh_parameter(argument, "_substring"))
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(Transformed::yes(expr::Expr::HigherOrderFunction(
        expr::HigherOrderFunction::new(
            std::sync::Arc::new(HigherOrderUDF::new_from_impl(SparkSubstring::new())),
            arguments,
        ),
    )))
}

fn literal_substring_overflow(arguments: &[expr::Expr]) -> PlanResult<bool> {
    fn literal_cast(expression: &expr::Expr) -> bool {
        // CAST resolution adds a private metadata envelope. Inspect its value
        // while retaining the original argument and declaration in the plan.
        match without_metadata_annotations(expression) {
            expr::Expr::Literal(_, _) => true,
            expr::Expr::Cast(cast)
                if matches!(
                    cast.field.data_type(),
                    DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
                ) =>
            {
                literal_cast(&cast.expr)
            }
            expr::Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkCastStringToFloat>() =>
            {
                matches!(function.args.as_slice(), [input] if literal_cast(input))
            }
            expr::Expr::ScalarFunction(function)
                if function
                    .func
                    .inner()
                    .downcast_ref::<SparkCastNumericToInteger>()
                    .is_some_and(|cast| {
                        !cast.is_try() && cast.target_type() == &DataType::Int32
                    }) =>
            {
                matches!(function.args.as_slice(), [input] if literal_cast(input)
                    && matches!(input.get_type(&DFSchema::empty()), Ok(DataType::Float32 | DataType::Float64)))
            }
            _ => false,
        }
    }
    let [source, position, length] = arguments else {
        return Ok(false);
    };
    let expr::Expr::Literal(source, _) = without_metadata_annotations(source) else {
        return Ok(false);
    };
    if source.is_null()
        || !matches!(
            source.data_type(),
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        )
        || !literal_cast(position)
        || !literal_cast(length)
    {
        return Ok(false);
    }
    if ![position, length]
        .into_iter()
        .try_fold(false, |found, argument| {
            Ok::<_, datafusion_common::DataFusionError>(
                found
                    || argument.exists(|value| {
                        Ok(matches!(value, expr::Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkCastNumericToInteger>()))
                    })?,
            )
        })?
    {
        return Ok(false);
    }
    // Preview only these built-in literal casts. Integer TRY_CAST, callbacks,
    // conditional bodies and column inputs retain their existing path.
    let evaluator = LiteralEvaluator::new();
    let mut overflow = false;
    for argument in [position, length] {
        match evaluator.evaluate(argument) {
            Ok(value) if !value.is_null() => {}
            Err(error)
                if error
                    .to_string()
                    .starts_with("[CAST_OVERFLOW]\ncaused by\n")
                    && matches!(error.find_root(), datafusion_common::DataFusionError::ArrowError(cause, _)
                        if matches!(cause.as_ref(), datafusion::arrow::error::ArrowError::CastError(_)
                            | datafusion::arrow::error::ArrowError::ArithmeticOverflow(_))) =>
            {
                overflow = true;
            }
            _ => return Ok(false),
        }
    }
    Ok(overflow)
}

pub(crate) fn lower_runtime_substring(
    expression: expr::Expr,
    pruned_empty_input: bool,
) -> PlanResult<Transformed<expr::Expr>> {
    if pruned_empty_input {
        let expr::Expr::ScalarFunction(function) = &expression else {
            return Ok(Transformed::no(expression));
        };
        if !function
            .func
            .inner()
            .downcast_ref::<SparkSubstring>()
            .is_some_and(SparkSubstring::has_logical_arguments)
            || !literal_substring_overflow(&function.args)?
        {
            return Ok(Transformed::no(expression));
        }
    }
    substring_lambdas(expression, pruned_empty_input)
}

pub(crate) fn lower_runtime_regexp_extract(
    expression: expr::Expr,
    local_values: bool,
) -> PlanResult<Transformed<expr::Expr>> {
    let expr::Expr::ScalarFunction(function) = &expression else {
        return Ok(Transformed::no(expression));
    };
    // Bare calls retain scalar NULL pruning; local VALUES preserve reached arguments.
    let Some(extract) = function
        .func
        .inner()
        .downcast_ref::<SparkRegexpExtract>()
        .filter(|extract| {
            extract.has_null_short_circuit() && (local_values || !extract.has_known_null_argument())
        })
    else {
        return Ok(Transformed::no(expression));
    };
    if function.args.len() != 3 {
        return Ok(Transformed::no(expression));
    }
    let literal_int_cast = match &function.args[2] {
        expr::Expr::Cast(index) if index.field.data_type() == &DataType::Int64 => {
            let index = without_metadata_annotations(&index.expr);
            let index = match index {
                expr::Expr::Cast(identity) if identity.field.data_type() == &DataType::Int32 => {
                    without_metadata_annotations(&identity.expr)
                }
                _ => index,
            };
            matches!(index, expr::Expr::ScalarFunction(conversion)
            if conversion.func.inner().downcast_ref::<SparkCastNumericToInteger>()
                .is_some_and(|conversion| {
                    conversion.target_type() == &DataType::Int32 && !conversion.is_try()
                })
            && matches!(conversion.args.as_slice(), [expr::Expr::Literal(ScalarValue::Int64(Some(_)), _)]))
        }
        _ => false,
    };
    if !local_values
        && extract.requires_local_index_check()
        && literal_int_cast
        && function.args[..2].iter().all(|argument| {
            matches!(
                literal_value_without_evaluation(argument),
                Some(
                    ScalarValue::Utf8(Some(_))
                        | ScalarValue::LargeUtf8(Some(_))
                        | ScalarValue::Utf8View(Some(_))
                )
            )
        })
    {
        // These non-NULL literals cannot discard the index. Keep the shared cast's
        // planning failure outside the conditional argument lambda.
        return Ok(Transformed::no(expression));
    }
    fn literal_cast_tree(expression: &expr::Expr) -> bool {
        match without_metadata_annotations(expression) {
            expr::Expr::Literal(value, _) => {
                !value.is_null()
                    && matches!(
                        value.data_type(),
                        DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Utf8View
                            | DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::Float32
                            | DataType::Float64
                            | DataType::Decimal128(..)
                            | DataType::Decimal256(..)
                    )
            }
            expr::Expr::Alias(alias) => literal_cast_tree(&alias.expr),
            expr::Expr::Cast(expr::Cast { expr, .. })
            | expr::Expr::TryCast(expr::TryCast { expr, .. }) => literal_cast_tree(expr),
            expr::Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkCastNumericToInteger>()
                    || function.func.inner().is::<SparkCastStringToFloat>()
                    || function.func.inner().is::<SparkCastStringToInteger>()
                    || function.func.inner().is::<SparkDecimalCast>() =>
            {
                matches!(function.args.as_slice(), [argument] if literal_cast_tree(argument))
            }
            _ => false,
        }
    }
    let literal_index = match &function.args[2] {
        expr::Expr::Literal(ScalarValue::Int64(Some(_)), _) => true,
        expr::Expr::Cast(index) if index.field.data_type() == &DataType::Int64 => {
            let index = without_metadata_annotations(&index.expr);
            let integer_conversion = match index {
                expr::Expr::Literal(ScalarValue::Int32(Some(_)), _) => true,
                expr::Expr::Cast(cast) => cast.field.data_type() == &DataType::Int32,
                expr::Expr::ScalarFunction(function) => {
                    function
                        .func
                        .inner()
                        .downcast_ref::<SparkCastNumericToInteger>()
                        .is_some_and(|cast| {
                            cast.target_type() == &DataType::Int32 && !cast.is_try()
                        })
                        || function
                            .func
                            .inner()
                            .downcast_ref::<SparkCastStringToInteger>()
                            .is_some_and(|cast| cast.mode() != StringIntegerCastMode::Try)
                        || function
                            .func
                            .inner()
                            .downcast_ref::<SparkDecimalCast>()
                            .is_some_and(|cast| cast.mode() != DecimalCastMode::Try)
                }
                _ => false,
            };
            integer_conversion
                && literal_cast_tree(index)
                && index
                    .get_type(&DFSchema::empty())
                    .is_ok_and(|data_type| data_type == DataType::Int32)
        }
        _ => false,
    };
    if !local_values
        && literal_index
        && function.args[..2].iter().all(|argument| {
            matches!(
                literal_value_without_evaluation(argument),
                Some(
                    ScalarValue::Utf8(Some(_))
                        | ScalarValue::LargeUtf8(Some(_))
                        | ScalarValue::Utf8View(Some(_))
                )
            )
        })
    {
        // Keep closed literal casts visible to ordinary Spark preparation. This
        // classification never invokes operands or compiles the pattern.
        return Ok(Transformed::no(expression));
    }
    let arguments = function
        .args
        .iter()
        .cloned()
        .map(|argument| lambda_with_fresh_parameter(argument, "_regexp_extract"))
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(Transformed::yes(expr::Expr::HigherOrderFunction(
        expr::HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(extract.clone())),
            arguments,
        ),
    )))
}

fn overlay(mut args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    if args.len() == 4
        && matches!(
            args[3],
            expr::Expr::Literal(ScalarValue::Int64(Some(-1)), _)
                | expr::Expr::Literal(ScalarValue::Int32(Some(-1)), _)
        )
    {
        args.pop();
    }
    Ok(expr_fn::overlay(args))
}

fn position(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let start_opt = (arguments.len() == 3).then(|| arguments.pop()).flatten();
    let (substr, str) = arguments
        .into_iter()
        .map(|expr| cast_to_logical_string_or_try(expr, function_context.schema, false))
        .collect::<PlanResult<Vec<_>>>()?
        .two()
        .map_err(|_| PlanError::invalid("position requires 2 or 3 arguments"))?;
    Ok(match start_opt {
        Some(start) => {
            let str_from_pos = expr_fn::substr(str, start.clone());
            let pos = expr_fn::strpos(str_from_pos, substr);
            when(start.clone().lt_eq(lit(0)), lit(0))
                .when(pos.clone().eq(lit(0)), lit(0))
                .when(pos.clone().gt(lit(0)), start + pos - lit(1))
                .end()?
        }
        None => expr_fn::strpos(str, substr),
    })
}

fn space(n: expr::Expr) -> expr::Expr {
    expr_fn::repeat(lit(" "), n)
}

fn replace(mut args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    let replacement = (args.len() == 3)
        .then(|| args.pop())
        .flatten()
        .unwrap_or_else(|| lit(""));
    let (str, substr) = args
        .two()
        .map_err(|_| PlanError::invalid("replace requires 2 or 3 arguments"))?;
    Ok(expr_fn::replace(str, substr, replacement))
}

/// Field policy for TRIM/LTRIM/RTRIM/LOWER/UPPER: every supplied argument can
/// propagate NULL. Signatures, output types, metadata and value kernels remain
/// native; nested case calls follow Spark simplification and local-phase policy.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NullPropagatingStringFunction {
    function: Arc<ScalarUDF>,
    preserve_intermediate_case: bool,
}

impl NullPropagatingStringFunction {
    fn is_case_conversion(&self) -> bool {
        self.function.inner().is::<LowerFunc>() || self.function.inner().is::<UpperFunc>()
    }
}

// Retain unary value shells while locating a case call or its column input.
// The shared metadata classifier admits only permanent empty clearing shells.
fn case_conversion_shells<'a>(
    mut value: &'a expr::Expr,
    schema: &DFSchema,
) -> datafusion_common::Result<(&'a expr::Expr, Vec<&'a expr::Expr>)> {
    let mut shells = Vec::new();
    loop {
        let child = match value {
            expr::Expr::Alias(alias) => alias.expr.as_ref(),
            expr::Expr::Cast(cast)
                if cast.field.data_type().is_string()
                    && cast.expr.get_type(schema)?.is_string() =>
            {
                cast.expr.as_ref()
            }
            expr::Expr::TryCast(cast)
                if cast.field.data_type().is_string()
                    && cast.expr.get_type(schema)?.is_string() =>
            {
                cast.expr.as_ref()
            }
            expr::Expr::ScalarFunction(function)
                if !std::ptr::eq(value, without_inherited_column_metadata(value)) =>
            {
                &function.args[0]
            }
            _ => break,
        };
        shells.push(value);
        value = child;
    }
    Ok((value, shells))
}

fn rebuild_case_conversion_shells(
    shells: Vec<&expr::Expr>,
    mut value: expr::Expr,
) -> datafusion_common::Result<expr::Expr> {
    for shell in shells.into_iter().rev() {
        // Each shell is unary; cloning retains its UDF and exact cast FieldRef Arc.
        value = (*shell)
            .clone()
            .map_children(|_| Ok(Transformed::yes(value.clone())))?
            .data;
    }
    Ok(value)
}

impl ScalarUDFImpl for NullPropagatingStringFunction {
    fn name(&self) -> &str {
        self.function.name()
    }

    fn aliases(&self) -> &[String] {
        self.function.aliases()
    }

    fn signature(&self) -> &Signature {
        self.function.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        self.function.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion_common::Result<FieldRef> {
        // The resolver asks for metadata before ordinary signature validation.
        if args.arg_fields.is_empty() {
            return datafusion_common::plan_err!("{} requires at least one argument", self.name());
        }
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        let field = self.function.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(nullable)))
    }

    fn simplify(
        &self,
        args: Vec<expr::Expr>,
        info: &SimplifyContext,
    ) -> datafusion_common::Result<ExprSimplifyResult> {
        if !self.preserve_intermediate_case
            && self.is_case_conversion()
            && let [argument] = args.as_slice()
        {
            // Spark removes STRING casts first. Keep Arrow representation conversions
            // and their fields when bypassing the nested case conversion.
            let (argument, shells) = case_conversion_shells(argument, info.schema())?;
            if let expr::Expr::ScalarFunction(nested) = argument
                && nested
                    .func
                    .inner()
                    .downcast_ref::<Self>()
                    .is_some_and(|adapter| {
                        adapter.is_case_conversion() && !adapter.preserve_intermediate_case
                    })
                && nested.args.len() == 1
            {
                // Child UDF hooks can expose constants after the evaluator's first pass.
                if crate::expression_errors::row_expression(argument, false)?
                    && let Ok(literal @ expr::Expr::Literal(_, _)) =
                        ExprSimplifier::new(info.clone()).simplify(argument.clone())
                {
                    let argument = rebuild_case_conversion_shells(shells, literal)?;
                    return Ok(ExprSimplifyResult::Simplified(
                        call_null_propagating_string(Arc::clone(&self.function), vec![argument]),
                    ));
                }
                let argument = rebuild_case_conversion_shells(shells, nested.args[0].clone())?;
                return Ok(ExprSimplifyResult::Simplified(
                    call_null_propagating_string(Arc::clone(&self.function), vec![argument]),
                ));
            }
        }
        self.function.simplify(args, info)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        self.function.invoke_with_args(args)
    }
}

pub(crate) fn preserve_local_case_conversion(
    expression: expr::Expr,
) -> datafusion_common::Result<Transformed<expr::Expr>> {
    expression.transform_up(|mut expression| {
        if let expr::Expr::ScalarFunction(function) = &mut expression
            && let Some(adapter) = function
                .func
                .inner()
                .downcast_ref::<NullPropagatingStringFunction>()
            && adapter.is_case_conversion()
            && !adapter.preserve_intermediate_case
        {
            function.func = Arc::new(ScalarUDF::from(NullPropagatingStringFunction {
                function: Arc::clone(&adapter.function),
                preserve_intermediate_case: true,
            }));
            return Ok(Transformed::yes(expression));
        }
        Ok(Transformed::no(expression))
    })
}

// Native projection merging stops at subquery aliases. Rebind only a private
// case producer with one direct case consumer; repeating the outer conversion
// is idempotent and keeps the existing projections, aliases and fields intact.
pub(crate) fn simplify_projected_case_conversions(
    mut plan: LogicalPlan,
    column_uses: &std::collections::HashMap<String, usize>,
) -> datafusion_common::Result<Transformed<LogicalPlan>> {
    fn unalias(expression: &expr::Expr) -> &expr::Expr {
        // Inspect output annotations without removing or rebuilding any wrapper.
        match without_metadata_annotations(expression) {
            expr::Expr::Alias(alias) => unalias(&alias.expr),
            expression => expression,
        }
    }
    fn producer(plan: &LogicalPlan) -> Option<&datafusion_expr::logical_plan::Projection> {
        match plan {
            LogicalPlan::SubqueryAlias(alias) => producer(&alias.input),
            LogicalPlan::Projection(project) => Some(project),
            _ => None,
        }
    }
    fn replace_producer(input: &mut Arc<LogicalPlan>, replacements: &[(usize, expr::Expr)]) {
        match Arc::make_mut(input) {
            LogicalPlan::SubqueryAlias(alias) => replace_producer(&mut alias.input, replacements),
            LogicalPlan::Projection(project) => {
                for (index, expression) in replacements {
                    project.expr[*index] = expression.clone();
                }
            }
            _ => {}
        }
    }
    let LogicalPlan::Projection(project) = &mut plan else {
        return Ok(Transformed::no(plan));
    };
    let Some(input) = producer(&project.input) else {
        return Ok(Transformed::no(plan));
    };
    // Spark permits reused foldable siblings such as 1+1. Only a reused
    // nontrivial, nonfoldable sibling retains the whole producer boundary.
    for (field, expression) in input.schema.fields().iter().zip(&input.expr) {
        if column_uses.get(field.name()).copied().unwrap_or(0) > 1
            && !expression.placement().should_push_to_leaves()
            && !crate::expression_errors::row_expression(expression, false)?
        {
            return Ok(Transformed::no(plan));
        }
    }
    let mut replacements = Vec::new();
    for expression in &project.expr {
        let expr::Expr::ScalarFunction(consumer) = unalias(expression) else {
            continue;
        };
        let Some(outer) = consumer
            .func
            .inner()
            .downcast_ref::<NullPropagatingStringFunction>()
        else {
            continue;
        };
        if !outer.is_case_conversion()
            || outer.preserve_intermediate_case
            || consumer.args.len() != 1
        {
            continue;
        }
        let (argument, _) = case_conversion_shells(&consumer.args[0], project.input.schema())?;
        let expr::Expr::Column(column) = argument else {
            continue;
        };
        if column_uses.get(&column.name) != Some(&1) {
            continue;
        }
        let index = project.input.schema().index_of_column(column)?;
        // A single use still retains Spark's nondeterministic producer boundary.
        if input.expr[index].is_volatile() {
            continue;
        }
        let (function, shells) = case_conversion_shells(&input.expr[index], input.input.schema())?;
        let expr::Expr::ScalarFunction(function) = function else {
            continue;
        };
        let Some(inner) = function
            .func
            .inner()
            .downcast_ref::<NullPropagatingStringFunction>()
        else {
            continue;
        };
        if inner.is_case_conversion()
            && !inner.preserve_intermediate_case
            && function.args.len() == 1
            && inner.function.inner().is::<LowerFunc>() != outer.function.inner().is::<LowerFunc>()
        {
            let mut replacement = function.clone();
            replacement.func = Arc::new(ScalarUDF::from(NullPropagatingStringFunction {
                function: Arc::clone(&outer.function),
                preserve_intermediate_case: false,
            }));
            replacements.push((
                index,
                rebuild_case_conversion_shells(shells, expr::Expr::ScalarFunction(replacement))?,
            ));
        }
    }
    if replacements.is_empty() {
        return Ok(Transformed::no(plan));
    }
    replace_producer(&mut project.input, &replacements);
    Ok(Transformed::yes(plan))
}

fn call_null_propagating_string(
    function: Arc<ScalarUDF>,
    arguments: Vec<expr::Expr>,
) -> expr::Expr {
    ScalarUDF::from(NullPropagatingStringFunction {
        function,
        preserve_intermediate_case: false,
    })
    .call(arguments)
}

fn lower(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(lower_expr(validate_utf8(input)?))
}

pub(super) fn lower_expr(argument: expr::Expr) -> expr::Expr {
    call_null_propagating_string(native_string::lower(), vec![argument])
}

fn upper(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(call_null_propagating_string(
        native_string::upper(),
        vec![validate_utf8(input)?],
    ))
}

fn startswith(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_str_out_bool(expr_fn::starts_with)(input)
}

fn endswith(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_str_out_bool(expr_fn::ends_with)(input)
}

fn contains(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_str_out_bool(expr_fn::contains)(input)
}

/// Spark measures the character length of string data and the byte length of binary data,
/// so binary must reach the function as-is. Any other type is measured as its string form,
/// via the Spark-compatible cast rather than the Arrow one, which renders a timestamp with
/// a time zone suffix. Collections have no string form in Spark and are rejected.
fn length_argument(input: ScalarFunctionInput, name: &str) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    let data_type = arg.get_type(function_context.schema)?;
    match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => Ok(arg),
        DataType::FixedSizeBinary(_) => Ok(cast(arg, DataType::Binary)),
        DataType::Null => Ok(cast(arg, DataType::Utf8)),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => Err(PlanError::invalid(format!(
            "`{name}` does not support {data_type} input"
        ))),
        _ => Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![arg])),
    }
}

fn length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = length_argument(input, "length")?;
    Ok(ScalarUDF::from(SparkLengthFunc::new()).call(vec![arg]))
}

fn bit_length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = length_argument(input, "bit_length")?;
    Ok(ScalarUDF::from(SparkBitLength::new()).call(vec![arg]))
}

fn octet_length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = length_argument(input, "octet_length")?;
    Ok(ScalarUDF::from(SparkOctetLength::new()).call(vec![arg]))
}

fn ascii(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_out_i32(expr_fn::ascii)(input)
}

fn cast_to_logical_string_or_try(
    arg: expr::Expr,
    schema: &DFSchema,
    is_try: bool,
) -> PlanResult<expr::Expr> {
    let data_type = match arg.get_type(schema)? {
        DataType::LargeBinary | DataType::LargeUtf8 => DataType::LargeUtf8,
        DataType::Utf8View => DataType::Utf8View,
        _ => DataType::Utf8,
    };
    Ok(if is_try {
        try_cast(arg, data_type)
    } else {
        cast(arg, data_type)
    })
}

fn validate_utf8_or_try(input: ScalarFunctionInput, is_try: bool) -> PlanResult<expr::Expr> {
    cast_to_logical_string_or_try(
        input.arguments.one()?,
        input.function_context.schema,
        is_try,
    )
}

fn validate_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    validate_utf8_or_try(input, false)
}

fn try_validate_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    validate_utf8_or_try(input, true)
}

fn is_valid_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(try_validate_utf8(input)?.is_not_null())
}

fn in_str_str_out_bool(
    func: impl Fn(expr::Expr, expr::Expr) -> expr::Expr,
) -> impl Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> {
    move |input: ScalarFunctionInput| {
        let (arg1, arg2) = input
            .arguments
            .into_iter()
            .map(|expr| cast_to_logical_string_or_try(expr, input.function_context.schema, false))
            .collect::<PlanResult<Vec<_>>>()?
            .two()?;
        Ok(func(arg1, arg2))
    }
}

fn in_str_out_i32(
    func: impl Fn(expr::Expr) -> expr::Expr,
) -> impl Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> {
    move |input: ScalarFunctionInput| Ok(cast(func(validate_utf8(input)?), DataType::Int32))
}

fn build_trim_function(
    function_name: &'static str,
    native_function: Arc<ScalarUDF>,
) -> ScalarFunction {
    ScalarFunctionBuilder::var_arg(move |mut arguments| {
        let argument_count = arguments.len();
        if !(1..=2).contains(&argument_count) {
            return Err(PlanError::invalid(format!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{function_name}` requires [1, 2] parameters but the actual number is {argument_count}."
            )));
        }
        arguments.reverse();
        Ok(call_null_propagating_string(
            Arc::clone(&native_function),
            arguments,
        ))
    })
}

/// Dispatch for `to_char(expr, format)` and its alias `to_varchar`, following Spark's
/// `ToCharacterBuilder`: datetime input formats like `date_format`, binary input is
/// converted to a base64, hexadecimal, or UTF-8 string, and any other input is
/// formatted as a decimal value according to a number format.
fn to_char(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (value, format) = arguments
        .two()
        .map_err(|_| PlanError::invalid("to_char requires 2 arguments"))?;
    match value.get_type(function_context.schema)? {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            let timezone = function_context.plan_config.session_timezone.clone();
            Ok(date_format(value, format, timezone.to_string()))
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => {
            // Spark requires a foldable format for binary input since the format
            // determines the conversion function.
            let expr::Expr::Literal(scalar, _) = &format else {
                return Err(PlanError::invalid(
                    "to_char: the `format` parameter must be a string literal for binary input",
                ));
            };
            match scalar.try_as_str() {
                Some(Some(name)) => match name.trim().to_lowercase().as_str() {
                    "base64" => Ok(ScalarUDF::from(SparkBase64::new()).call(vec![value])),
                    "hex" => Ok(math_fn::hex(value)),
                    "utf-8" => Ok(ScalarUDF::from(SparkDecode::new()).call(vec![value, format])),
                    invalid => Err(PlanError::invalid(format!(
                        "to_char: the value of the `format` parameter expects one of binary formats 'base64', 'hex', 'utf-8', but got '{invalid}'"
                    ))),
                },
                Some(None) => Err(PlanError::invalid(
                    "to_char: the `format` parameter expects a non-NULL value for binary input",
                )),
                None => Err(PlanError::invalid(
                    "to_char: the `format` parameter must be a string literal for binary input",
                )),
            }
        }
        _ => {
            let ansi_mode = function_context.plan_config.ansi_mode;
            Ok(ScalarUDF::from(SparkToChar::new(ansi_mode)).call(vec![value, format]))
        }
    }
}

pub(super) fn list_built_in_string_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("ascii", F::custom(ascii)),
        ("base64", F::udf(SparkBase64::new())),
        ("bit_length", F::custom(bit_length)),
        ("btrim", F::var_arg(expr_fn::btrim)),
        ("char", F::unary(expr_fn::chr)),
        ("char_length", F::custom(length)),
        ("character_length", F::custom(length)),
        ("chr", F::unary(expr_fn::chr)),
        ("collate", F::unknown("collate")),
        ("collation", F::unknown("collation")),
        ("concat_ws", F::udf(SparkConcatWs::new())),
        ("contains", F::custom(contains)),
        ("decode", F::udf(SparkDecode::new())),
        ("elt", F::udf(SparkElt::new())),
        ("encode", F::udf(SparkEncode::new())),
        ("endswith", F::custom(endswith)),
        ("find_in_set", F::binary(expr_fn::find_in_set)),
        ("format_number", F::udf(FormatNumber::new())),
        ("format_string", F::udf(FormatStringFunc::new())),
        ("initcap", F::unary(expr_fn::initcap)),
        ("instr", F::binary(expr_fn::instr)),
        ("is_valid_utf8", F::custom(is_valid_utf8)),
        ("lcase", F::custom(lower)),
        ("left", F::binary(expr_fn::left)),
        ("len", F::custom(length)),
        ("length", F::custom(length)),
        ("levenshtein", F::udf(Levenshtein::new())),
        ("locate", F::custom(position)),
        ("lower", F::custom(lower)),
        ("lpad", F::var_arg(expr_fn::lpad)),
        (
            "ltrim",
            build_trim_function("ltrim", native_string::ltrim()),
        ),
        ("luhn_check", F::unary(string_fn::luhn_check)),
        ("make_valid_utf8", F::udf(MakeValidUtf8::new())),
        ("mask", F::udf(SparkMask::new())),
        ("octet_length", F::custom(octet_length)),
        ("overlay", F::var_arg(overlay)),
        ("position", F::custom(position)),
        ("printf", F::udf(FormatStringFunc::new())),
        ("quote", F::udf(SparkQuote::new())),
        ("randstr", F::udf(Randstr::new())),
        ("regexp_count", F::udf(RegexpCountFunc::new())),
        ("regexp_extract", F::custom(regexp_extract)),
        ("regexp_extract_all", F::udf(SparkRegexpExtractAll::new())),
        ("regexp_instr", F::udf(RegexpInstrFunc::new())),
        ("regexp_replace", F::custom(regexp_replace)),
        ("regexp_substr", F::custom(regexp_substr)),
        ("repeat", F::binary(expr_fn::repeat)),
        ("replace", F::var_arg(replace)),
        ("right", F::binary(expr_fn::right)),
        ("rpad", F::var_arg(expr_fn::rpad)),
        (
            "rtrim",
            build_trim_function("rtrim", native_string::rtrim()),
        ),
        ("sentences", F::udf(SparkSentences::new())),
        ("soundex", F::udf(Soundex::new())),
        ("space", F::unary(space)),
        ("split", F::udf(SparkSplit::new())),
        ("split_part", F::ternary(expr_fn::split_part)),
        ("startswith", F::custom(startswith)),
        ("substr", F::custom(substr)),
        ("substring", F::custom(substr)),
        ("substring_index", F::ternary(expr_fn::substr_index)),
        ("to_binary", F::udf(SparkToBinary::new())),
        ("to_char", F::custom(to_char)),
        ("to_number", F::udf(SparkToNumber::new(false))),
        ("to_varchar", F::custom(to_char)),
        ("translate", F::ternary(expr_fn::translate)),
        ("trim", build_trim_function("trim", native_string::btrim())),
        ("try_to_binary", F::udf(SparkTryToBinary::new())),
        ("try_to_number", F::udf(SparkToNumber::new(true))),
        ("try_validate_utf8", F::custom(try_validate_utf8)),
        ("ucase", F::custom(upper)),
        ("unbase64", F::udf(SparkUnbase64::new())),
        ("upper", F::custom(upper)),
        ("validate_utf8", F::custom(validate_utf8)),
        ("strpos", F::binary(expr_fn::strpos)),
    ]
}

#[test]
fn regexp_extract_mandatory_literal_int_cast_keeps_preparation_error() -> PlanResult<()> {
    for explicit in [true, false] {
        let index = ScalarUDF::from(SparkCastNumericToInteger::try_new(
            DataType::Int32,
            true,
            false,
        )?)
        .call(vec![lit(4294967296_i64)]);
        let index = cast(
            if explicit {
                cast(index, DataType::Int32)
            } else {
                index
            },
            DataType::Int64,
        );
        let extract = SparkRegexpExtract::new()
            .with_null_short_circuit(false)
            .with_local_index_check(true);
        let expression =
            ScalarUDF::from(extract.clone()).call(vec![lit("ab"), lit("(a)"), index.clone()]);
        let bare = lower_runtime_regexp_extract(expression.clone(), false)?;
        assert!(!bare.transformed);
        let info = SimplifyContext::builder()
            .with_schema(Arc::new(DFSchema::empty()))
            .build();
        let error = ExprSimplifier::new(info)
            .simplify(bare.data)
            .unwrap_err()
            .to_string();
        assert!(
            error.ends_with("Arrow error: Cast error: Can't cast value 4294967296 to type Int32"),
            "{error}"
        );
        assert!(lower_runtime_regexp_extract(expression, true)?.transformed);
        for (target, value) in [
            (DataType::Int32, ScalarValue::Float32(Some(f32::NAN))),
            (DataType::Int32, ScalarValue::Float64(Some(f64::NAN))),
            (DataType::Int8, ScalarValue::Float64(Some(f64::NAN))),
            (DataType::Int16, ScalarValue::Float64(Some(f64::NAN))),
            (DataType::Int64, ScalarValue::Float64(Some(f64::NAN))),
        ] {
            let closed_literal = target == DataType::Int32;
            let index = ScalarUDF::from(SparkCastNumericToInteger::try_new(target, true, false)?)
                .call(vec![lit(value)]);
            let expression = ScalarUDF::from(extract.clone()).call(vec![
                lit("ab"),
                lit("(a)"),
                cast(index, DataType::Int64),
            ]);
            assert_eq!(
                lower_runtime_regexp_extract(expression, false)?.transformed,
                !closed_literal
            );
        }
        for arguments in [
            vec![datafusion_expr::col("s"), lit("(a)"), index],
            vec![lit("ab"), lit("(a)"), cast(lit(f64::NAN), DataType::Int64)],
            vec![
                lit("ab"),
                lit("(a)"),
                cast(
                    ScalarUDF::from(SparkCastNumericToInteger::try_new(
                        DataType::Int32,
                        true,
                        true,
                    )?)
                    .call(vec![lit(4294967296_i64)]),
                    DataType::Int64,
                ),
            ],
            vec![
                lit("ab"),
                lit("(a)"),
                cast(
                    ScalarUDF::from(SparkCastNumericToInteger::try_new(
                        DataType::Int32,
                        true,
                        false,
                    )?)
                    .call(vec![datafusion_expr::col("i")]),
                    DataType::Int64,
                ),
            ],
        ] {
            let expression = ScalarUDF::from(extract.clone()).call(arguments);
            assert!(lower_runtime_regexp_extract(expression, false)?.transformed);
        }
    }
    Ok(())
}

#[test]
fn regexp_extract_literal_simplification_requires_the_spark_marker() -> PlanResult<()> {
    use datafusion_common::config::ConfigOptions;

    let extract = SparkRegexpExtract::new().with_null_short_circuit(false);
    let plain = SimplifyContext::default();
    let mut options = ConfigOptions::default();
    options
        .extensions
        .insert(crate::expression_errors::SparkSqlExpressionSemantics::default());
    let spark = SimplifyContext::builder()
        .with_config_options(Arc::new(options))
        .build();
    for (source, pattern, index, count, expected_index) in [
        ("a", "(a)", Some(-1_i64), 1, -1),
        ("a", "(a)", Some(2), 1, 2),
        (
            "a",
            "(a)",
            Some(i64::from(i32::MAX)),
            1,
            i64::from(i32::MAX),
        ),
        (
            "a",
            "(a)",
            Some(i64::from(i32::MIN)),
            1,
            i64::from(i32::MIN),
        ),
        ("abc", "abc", None, 0, 1),
    ] {
        let mut arguments = vec![lit(source), lit(pattern)];
        if let Some(index) = index {
            arguments.push(lit(index));
        }
        assert!(matches!(
            extract.simplify(arguments.clone(), &plain)?,
            ExprSimplifyResult::Original(original) if original == arguments
        ));
        let error = extract.simplify(arguments, &spark).unwrap_err().to_string();
        assert_eq!(
            error,
            format!(
                "Execution error: Spark `regexp_extract` function: [INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] Expects group index between 0 and {count}, but got {expected_index}"
            )
        );
    }
    for arguments in [
        vec![lit("b"), lit("(a)"), lit(-1_i64)],
        vec![lit("a"), lit("(a)"), lit(0_i64)],
        vec![lit("a"), lit("["), lit(1_i64)],
        vec![lit("ab"), lit("(?<=a)(b)"), lit(1_i64)],
        vec![lit("a"), lit("[")],
        vec![lit("b"), lit("(a)")],
    ] {
        assert!(matches!(
            extract.simplify(arguments.clone(), &spark)?,
            ExprSimplifyResult::Original(original) if original == arguments
        ));
    }
    Ok(())
}

#[test]
fn regexp_extract_literal_routing_keeps_nonliteral_arguments_lazy() -> PlanResult<()> {
    use datafusion_expr::{Volatility, col, create_udf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let extract = ScalarUDF::from(
        SparkRegexpExtract::new()
            .with_null_short_circuit(false)
            .with_local_index_check(true),
    );
    for index in [
        lit(2_i64),
        cast(lit(2_i32), DataType::Int64),
        cast(cast(lit(2_i32), DataType::Int32), DataType::Int64),
    ] {
        let expression = extract.call(vec![lit("a"), lit("(a)"), index]);
        assert!(!lower_runtime_regexp_extract(expression.clone(), false)?.transformed);
        assert!(lower_runtime_regexp_extract(expression, true)?.transformed);
    }
    for ansi in [true, false] {
        for argument in [
            lit(2147483648_i64),
            ScalarUDF::from(SparkCastStringToFloat::try_new(DataType::Float64, false)?)
                .call(vec![lit("-1.9")]),
            ScalarUDF::from(SparkCastStringToFloat::try_new(DataType::Float32, false)?)
                .call(vec![lit("NaN")]),
            ScalarUDF::from(SparkCastStringToFloat::try_new(DataType::Float64, false)?)
                .call(vec![lit("Infinity")]),
            cast(lit(2147483648_i64), DataType::Decimal128(10, 0)),
        ] {
            let index = cast_string_function_int(argument, &DFSchema::empty(), ansi, "index")?;
            let expression = extract.call(vec![lit("a"), lit("(a)"), cast(index, DataType::Int64)]);
            assert!(!lower_runtime_regexp_extract(expression.clone(), false)?.transformed);
            assert!(lower_runtime_regexp_extract(expression, true)?.transformed);
        }
    }
    for volatility in [Volatility::Immutable, Volatility::Volatile] {
        let observed = Arc::clone(&calls);
        let opaque = create_udf(
            "regexp_literal_index_callback",
            vec![DataType::Float64],
            DataType::Float64,
            volatility,
            Arc::new(move |arguments| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(arguments[0].clone())
            }),
        );
        for argument in [
            opaque.call(vec![lit(1.9_f64)]),
            lit(1.0_f64) + lit(0.9_f64),
            col("i"),
            expr_fn::random(),
            array_element(
                datafusion_functions_nested::expr_fn::make_array(vec![lit(1.9_f64)]),
                lit(1_i64),
            ),
        ] {
            let index = ScalarUDF::from(SparkCastNumericToInteger::try_new(
                DataType::Int32,
                true,
                false,
            )?)
            .call(vec![argument]);
            let expression = extract.call(vec![lit("a"), lit("(a)"), cast(index, DataType::Int64)]);
            assert!(lower_runtime_regexp_extract(expression, false)?.transformed);
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn regexp_extract_known_null_pruning_keeps_local_order_and_type_validation() -> PlanResult<()> {
    use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_expr::execution_props::ExecutionProps;

    let schema = Arc::new(DFSchema::empty());
    let info = SimplifyContext::builder()
        .with_schema(Arc::clone(&schema))
        .build();
    let batch = RecordBatch::try_new_with_options(
        Arc::clone(schema.inner()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    for null_index in [true, false] {
        let extract = SparkRegexpExtract::new()
            .with_null_short_circuit(true)
            .with_local_index_check(true);
        let source = cast(cast(lit("bad"), DataType::Int32), DataType::Utf8);
        let arguments = vec![
            source,
            if null_index {
                lit("(a)")
            } else {
                lit(ScalarValue::Utf8(None))
            },
            if null_index {
                lit(ScalarValue::Int64(None))
            } else {
                lit(1_i64)
            },
        ];
        let expression = ScalarUDF::from(extract.clone()).call(arguments);
        let local = lower_runtime_regexp_extract(expression.clone(), true)?;
        assert!(local.transformed);
        let error = ExprSimplifier::new(info.clone())
            .simplify(local.data)
            .and_then(|expr| create_physical_expr(&expr, &schema, &ExecutionProps::new()))
            .and_then(|expr| expr.evaluate(&batch))
            .unwrap_err()
            .to_string();
        assert!(
            error.ends_with(
                "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type"
            ),
            "{error}"
        );
        let lowered = lower_runtime_regexp_extract(expression, false)?;
        assert!(!lowered.transformed);
        let simplified = ExprSimplifier::new(info.clone()).simplify(lowered.data)?;
        assert!(matches!(
            simplified,
            expr::Expr::Literal(ScalarValue::Utf8(None), _)
        ));
        assert!(
            extract
                .simplify(
                    vec![lit(1_i32), lit("(a)"), lit(ScalarValue::Int64(None))],
                    &info
                )
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn substring_logical_visibility_and_runtime_lowering_do_not_invoke_operands()
-> datafusion_common::Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion_expr::{Expr, ScalarUDFImpl, Volatility, col, create_udf};
    use sail_function::scalar::collection::spark_concat::SparkConcat;

    use crate::function::common::drop_inherited_column_metadata;

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let permitted = create_udf(
        "substring_permitted_position",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(move |args| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(args[0].clone())
        }),
    );
    let position = permitted.call(vec![lit(1_i64) / lit(1_i64)]);
    let args = vec![col("s"), position, lit(2_i64)];
    let function = ScalarUDF::from(SparkSubstring::new().with_logical_arguments());
    let logical = function.call(args.clone());
    // Logical CSE stays ordinary while physical evaluation selects live rows.
    assert!(!function.short_circuits());
    assert!(function.conditional_arguments(&args).is_none());
    assert!(function.inner().short_circuits_on_any_null_argument());
    let asynchronous = SparkSubstring::new()
        .with_logical_arguments()
        .with_async_arguments(true);
    assert!(!ScalarUDFImpl::short_circuits_on_any_null_argument(
        &asynchronous
    ));
    assert!(!ScalarUDFImpl::short_circuits(&asynchronous));
    assert!(ScalarUDFImpl::conditional_arguments(&asynchronous, &args).is_none());
    assert!(!crate::expression_errors::row_expression(&logical, false)?);
    let info = SimplifyContext::builder()
        .with_schema(Arc::new(DFSchema::try_from(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
        ]))?))
        .build();
    for arguments in [
        vec![lit(ScalarValue::Utf8(None)), logical.clone()],
        vec![logical.clone(), lit(ScalarValue::Utf8(None))],
    ] {
        assert!(matches!(
            SparkConcat::new().simplify(arguments, &info)?,
            ExprSimplifyResult::Original(_)
        ));
    }
    let malformed = function.call(vec![col("s"), lit(1_i64)]);
    assert!(
        !substring_lambdas(malformed, false)
            .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?
            .transformed
    );
    let lowered = substring_lambdas(logical, false)
        .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
    assert!(lowered.transformed);
    let Expr::HigherOrderFunction(runtime) = &lowered.data else {
        panic!("substring was not lowered to its runtime HOF");
    };
    assert!(runtime.func.short_circuits());
    assert!(runtime.func.conditional_arguments(&runtime.args).is_some());
    let bodies = SparkSubstring::closed_argument_bodies(&lowered.data)?.unwrap();
    assert_eq!(bodies, [&args[0], &args[1], &args[2]]);
    assert!(crate::expression_errors::row_expression(
        &lowered.data,
        true
    )?);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    for (source_type, text) in [(DataType::Float32, "NaN"), (DataType::Float64, "Infinity")] {
        let floating = drop_inherited_column_metadata(
            ScalarUDF::from(SparkCastStringToFloat::try_new(source_type, false)?)
                .call(vec![lit(text)]),
        );
        let integer = |ansi, is_try| -> datafusion_common::Result<Expr> {
            Ok(cast(
                ScalarUDF::from(SparkCastNumericToInteger::try_new(
                    DataType::Int32,
                    ansi,
                    is_try,
                )?)
                .call(vec![floating.clone()]),
                DataType::Int64,
            ))
        };
        let position = integer(true, false)?;
        let length = cast(cast(lit(2_i32), DataType::Int32), DataType::Int64);
        let literal_args = vec![lit("abcdef"), position.clone(), length.clone()];
        let literal = function.call(literal_args.clone());
        let lowered = substring_lambdas(literal.clone(), false)
            .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
        assert!(!lowered.transformed);
        assert_eq!(lowered.data, literal);
        let empty = lower_runtime_substring(literal.clone(), true)
            .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
        assert!(empty.transformed);
        assert!(matches!(empty.data, Expr::HigherOrderFunction(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let length_overflow = function.call(vec![lit("abcdef"), length.clone(), position.clone()]);
        assert!(
            !substring_lambdas(length_overflow, false)
                .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?
                .transformed
        );
        let nullable_float =
            ScalarUDF::from(SparkCastStringToFloat::try_new(DataType::Float32, true)?)
                .call(vec![lit("bad")]);
        let nullable_position = cast(
            ScalarUDF::from(SparkCastNumericToInteger::try_new(
                DataType::Int32,
                true,
                false,
            )?)
            .call(vec![nullable_float]),
            DataType::Int64,
        );
        for arguments in [
            vec![
                lit(ScalarValue::Utf8(None)),
                position.clone(),
                length.clone(),
            ],
            vec![
                lit("abcdef"),
                lit(ScalarValue::Int64(None)),
                position.clone(),
            ],
            vec![
                lit("abcdef"),
                position.clone(),
                lit(ScalarValue::Int64(None)),
            ],
            vec![col("s"), position.clone(), length.clone()],
            vec![lit("abcdef"), integer(false, false)?, length.clone()],
            vec![lit("abcdef"), integer(true, true)?, length.clone()],
            vec![lit("abcdef"), nullable_position, position.clone()],
            vec![
                lit("abcdef"),
                position,
                drop_inherited_column_metadata(permitted.call(vec![lit(2_i64)])),
            ],
        ] {
            assert!(
                substring_lambdas(function.call(arguments), false)
                    .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?
                    .transformed
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    let known_null = SparkSubstring::new()
        .with_logical_arguments()
        .with_known_null_argument(true);
    assert!(matches!(
        known_null.simplify(args.clone(), &info)?,
        ExprSimplifyResult::Simplified(Expr::Literal(value, _)) if value.is_null()
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for source_type in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        for source_nullable in [false, true] {
            let source_field = Arc::new(
                Field::new("s", source_type.clone(), source_nullable).with_metadata(
                    std::collections::HashMap::from([("source".into(), "metadata".into())]),
                ),
            );
            let info = SimplifyContext::builder()
                .with_schema(Arc::new(DFSchema::try_from(Schema::new(vec![
                    Arc::clone(&source_field),
                ]))?))
                .build();
            let arguments = vec![col("s"), lit(ScalarValue::Int64(None)), lit(2_i64)];
            let fields = arguments
                .iter()
                .map(|argument| {
                    argument
                        .to_field(info.schema().as_ref())
                        .map(|(_, field)| field)
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            let native_field = ScalarUDFImpl::return_field_from_args(
                &SparkSubstring::new(),
                datafusion_expr::ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &[
                        None,
                        Some(&ScalarValue::Int64(None)),
                        Some(&ScalarValue::Int64(Some(2))),
                    ],
                },
            )?;
            let ExprSimplifyResult::Simplified(value) = known_null.simplify(arguments, &info)?
            else {
                panic!("known NULL substring was not simplified");
            };
            assert!(matches!(&value, Expr::Literal(value, _) if value.is_null()));
            let (_, field) = value.to_field(info.schema().as_ref())?;
            assert_eq!(field.data_type(), native_field.data_type());
            assert_eq!(field.is_nullable(), native_field.is_nullable());
            assert_eq!(field.metadata(), native_field.metadata());
            assert_eq!(native_field.data_type(), &source_type);
            assert!(native_field.is_nullable());
            assert!(native_field.metadata().is_empty());
        }
    }
    let ordinary = SparkSubstring::new();
    assert!(!ScalarUDFImpl::short_circuits(&ordinary));
    assert!(!ScalarUDFImpl::short_circuits_on_any_null_argument(
        &ordinary
    ));
    assert!(matches!(
        ordinary.simplify(args, &info)?,
        ExprSimplifyResult::Original(_)
    ));
    Ok(())
}

#[test]
fn nested_case_simplification_preserves_cast_fields_and_arrow_types()
-> datafusion_common::Result<()> {
    use std::collections::HashMap;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::compute::cast as cast_array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::DataFusionError;
    use datafusion_expr::{Expr, col, execution_props::ExecutionProps};

    for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let schema = Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
            "s",
            data_type.clone(),
            true,
        )]))?);
        let info = SimplifyContext::builder()
            .with_schema(Arc::clone(&schema))
            .build();
        let values = Arc::new(StringArray::from(vec![Some("Straße"), None])) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::clone(schema.inner()),
            vec![cast_array(&values, &data_type)?],
        )?;
        for inner_type in [DataType::LargeUtf8, DataType::Utf8View] {
            for inner_try in [false, true] {
                for outer_try in [false, true] {
                    for metadata in [false, true] {
                        let field = |name: &str, data_type: DataType, nullable, role: &str| {
                            let field = Field::new(name, data_type, nullable);
                            Arc::new(if metadata {
                                field.with_metadata(HashMap::from([("cast".into(), role.into())]))
                            } else {
                                field
                            })
                        };
                        let inner_field =
                            field("representation_cast", inner_type.clone(), true, "inner");
                        let outer_field = field("utf8_cast", DataType::Utf8, false, "outer");
                        let wrap = |argument, field: &FieldRef, is_try| {
                            if is_try {
                                Expr::TryCast(expr::TryCast::new_from_field(
                                    Box::new(argument),
                                    Arc::clone(field),
                                ))
                            } else {
                                Expr::Cast(expr::Cast::new_from_field(
                                    Box::new(argument),
                                    Arc::clone(field),
                                ))
                            }
                        };
                        let upper =
                            call_null_propagating_string(native_string::upper(), vec![col("s")]);
                        let argument = wrap(
                            wrap(upper, &inner_field, inner_try),
                            &outer_field,
                            outer_try,
                        );
                        let original = call_null_propagating_string(
                            native_string::lower(),
                            vec![argument.clone()],
                        );
                        let adapter = NullPropagatingStringFunction {
                            function: native_string::lower(),
                            preserve_intermediate_case: false,
                        };
                        let ExprSimplifyResult::Simplified(simplified) =
                            adapter.simplify(vec![argument], &info)?
                        else {
                            panic!("nested case conversion was not simplified");
                        };
                        let Expr::ScalarFunction(function) = &simplified else {
                            panic!("outer LOWER was not retained");
                        };
                        let (inner, field) = match &function.args[0] {
                            Expr::Cast(cast) if !outer_try => (&cast.expr, &cast.field),
                            Expr::TryCast(cast) if outer_try => (&cast.expr, &cast.field),
                            other => panic!("outer cast variant changed: {other:?}"),
                        };
                        assert!(Arc::ptr_eq(field, &outer_field));
                        assert_eq!(field, &outer_field);
                        let (source, field) = match inner.as_ref() {
                            Expr::Cast(cast) if !inner_try => (&cast.expr, &cast.field),
                            Expr::TryCast(cast) if inner_try => (&cast.expr, &cast.field),
                            other => panic!("inner cast variant changed: {other:?}"),
                        };
                        assert!(Arc::ptr_eq(field, &inner_field));
                        assert_eq!(field, &inner_field);
                        assert_eq!(source.as_ref(), &col("s"));
                        assert_eq!(original.get_type(&schema)?, DataType::Utf8);
                        assert_eq!(simplified.get_type(&schema)?, DataType::Utf8);
                        let before =
                            create_physical_expr(&original, &schema, &ExecutionProps::new());
                        let after =
                            create_physical_expr(&simplified, &schema, &ExecutionProps::new());
                        if metadata && (inner_try || outer_try) {
                            // Native TRY_CAST rejects target metadata before evaluating either form.
                            let expected = if outer_try {
                                format!(
                                    "TryCast from {inner_type} to Utf8<{{\"cast\": \"outer\"}}> is not supported"
                                )
                            } else {
                                format!(
                                    "TryCast from {data_type} to {inner_type}<{{\"cast\": \"inner\"}}> is not supported"
                                )
                            };
                            for actual in [before, after] {
                                let Err(DataFusionError::Plan(message)) = actual else {
                                    panic!("native target-metadata rejection changed: {actual:?}");
                                };
                                assert_eq!(message, expected);
                            }
                        } else {
                            for (physical, expected) in [(before, "strasse"), (after, "straße")] {
                                let physical = physical?;
                                assert_eq!(physical.data_type(schema.inner())?, DataType::Utf8);
                                let actual = physical.evaluate(&batch)?.into_array(2)?;
                                assert_eq!(actual.data_type(), &DataType::Utf8);
                                assert_eq!(
                                    actual.as_ref(),
                                    &StringArray::from(vec![Some(expected), None])
                                );
                            }
                        }
                    }
                }
            }
        }
        let frozen = preserve_local_case_conversion(call_null_propagating_string(
            native_string::upper(),
            vec![col("s")],
        ))?
        .data;
        let adapter = NullPropagatingStringFunction {
            function: native_string::lower(),
            preserve_intermediate_case: false,
        };
        let ExprSimplifyResult::Original(args) = adapter.simplify(vec![frozen.clone()], &info)?
        else {
            panic!("case conversion crossed the early local-relation boundary");
        };
        assert_eq!(args, vec![frozen]);
    }
    Ok(())
}

#[test]
fn nested_case_simplification_preserves_cast_metadata_shells() -> datafusion_common::Result<()> {
    use std::collections::HashMap;

    use arrow::datatypes::{Field, Schema};
    use datafusion_expr::{Expr, col};

    use crate::function::common::{
        drop_inherited_column_metadata, drop_inherited_column_metadata_for_analysis,
        drop_scalar_subquery_metadata, preserve_hidden_sort_output_declaration,
        set_result_metadata, without_inherited_column_metadata,
    };

    for source_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let schema = Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
            "s",
            source_type,
            true,
        )]))?);
        let info = SimplifyContext::builder().with_schema(schema).build();
        for target_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let field = Arc::new(
                Field::new("cast_target", target_type, false)
                    .with_metadata(HashMap::from([("cast".into(), "kept".into())])),
            );
            for is_try in [false, true] {
                let wrap = |value| {
                    drop_inherited_column_metadata(if is_try {
                        Expr::TryCast(expr::TryCast::new_from_field(
                            Box::new(value),
                            Arc::clone(&field),
                        ))
                    } else {
                        Expr::Cast(expr::Cast::new_from_field(
                            Box::new(value),
                            Arc::clone(&field),
                        ))
                    })
                };
                let argument = wrap(drop_inherited_column_metadata(
                    call_null_propagating_string(native_string::upper(), vec![col("s")]),
                ));
                let adapter = NullPropagatingStringFunction {
                    function: native_string::lower(),
                    preserve_intermediate_case: false,
                };
                let ExprSimplifyResult::Simplified(actual) =
                    adapter.simplify(vec![argument.clone()], &info)?
                else {
                    panic!("CAST metadata hid the nested case conversion");
                };
                assert_eq!(
                    actual,
                    call_null_propagating_string(
                        native_string::lower(),
                        vec![wrap(drop_inherited_column_metadata(col("s")))],
                    )
                );
                let Expr::ScalarFunction(function) = &actual else {
                    panic!("outer LOWER was not retained");
                };
                let target = match without_inherited_column_metadata(&function.args[0]) {
                    Expr::Cast(cast) if !is_try => &cast.field,
                    Expr::TryCast(cast) if is_try => &cast.field,
                    other => panic!("CAST variant changed: {other:?}"),
                };
                assert!(Arc::ptr_eq(target, &field));

                for boundary in [
                    set_result_metadata(argument.clone(), vec![("owner".into(), "kept".into())]),
                    drop_inherited_column_metadata_for_analysis(argument.clone()),
                    drop_scalar_subquery_metadata(argument.clone(), &field),
                    preserve_hidden_sort_output_declaration(argument.clone(), &field),
                ] {
                    let ExprSimplifyResult::Original(args) =
                        adapter.simplify(vec![boundary.clone()], &info)?
                    else {
                        panic!("case conversion crossed a separate metadata boundary");
                    };
                    assert_eq!(args, vec![boundary]);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn projected_case_simplification_preserves_cast_shells_and_schemas() -> datafusion_common::Result<()>
{
    use std::collections::HashMap;

    use arrow::datatypes::{Field, Schema};
    use datafusion_expr::logical_plan::{EmptyRelation, Projection, SubqueryAlias};
    use datafusion_expr::{Volatility, col, create_udf};

    use crate::function::common::{drop_inherited_column_metadata, set_result_metadata};

    let source = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8,
            false,
        )]))?),
    }));
    let volatile_identity = create_udf(
        "case_volatile_identity",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Volatile,
        Arc::new(|_| {
            datafusion_common::internal_err!("case preparation must not execute operands")
        }),
    );
    for is_try in [false, true] {
        let field = Arc::new(
            Field::new("cast_target", DataType::LargeUtf8, true)
                .with_metadata(HashMap::from([("cast".into(), "kept".into())])),
        );
        let wrap = |value| {
            drop_inherited_column_metadata(if is_try {
                expr::Expr::TryCast(expr::TryCast::new_from_field(
                    Box::new(value),
                    Arc::clone(&field),
                ))
            } else {
                expr::Expr::Cast(expr::Cast::new_from_field(
                    Box::new(value),
                    Arc::clone(&field),
                ))
            })
        };
        for (inner, outer) in [
            (native_string::upper(), native_string::lower()),
            (native_string::lower(), native_string::upper()),
        ] {
            for (wrap_producer, volatile_argument, uses) in [
                (false, false, 1),
                (true, false, 1),
                (true, true, 1),
                (true, false, 2),
            ] {
                for wrap_consumer in 0..4 {
                    let argument = if volatile_argument {
                        volatile_identity.call(vec![col("s")])
                    } else {
                        col("s")
                    };
                    let producer_expr =
                        call_null_propagating_string(Arc::clone(&inner), vec![argument]);
                    let producer_expr = if wrap_producer {
                        wrap(producer_expr)
                    } else {
                        producer_expr
                    }
                    .alias("u");
                    let producer =
                        Projection::try_new(vec![producer_expr.clone()], Arc::clone(&source))?;
                    let producer_schema = Arc::clone(&producer.schema);
                    let alias =
                        SubqueryAlias::try_new(Arc::new(LogicalPlan::Projection(producer)), "q")?;
                    let alias_schema = Arc::clone(&alias.schema);
                    let argument = if wrap_consumer != 0 {
                        wrap(col("u"))
                    } else {
                        col("u")
                    };
                    let consumer_expr =
                        call_null_propagating_string(Arc::clone(&outer), vec![argument]);
                    let metadata = vec![
                        ("__autoGeneratedAlias".into(), "true".into()),
                        ("owner".into(), "kept".into()),
                    ];
                    let consumer_expr = match wrap_consumer {
                        2 => set_result_metadata(consumer_expr, metadata).alias("r"),
                        3 => set_result_metadata(consumer_expr.alias("r"), metadata),
                        _ => consumer_expr.alias("r"),
                    };
                    let consumer = Projection::try_new(
                        vec![consumer_expr.clone()],
                        Arc::new(LogicalPlan::SubqueryAlias(alias)),
                    )?;
                    let consumer_schema = Arc::clone(&consumer.schema);
                    let original = LogicalPlan::Projection(consumer);
                    let actual = simplify_projected_case_conversions(
                        original.clone(),
                        &HashMap::from([("u".into(), uses)]),
                    )?;
                    let should_rebind = !volatile_argument && uses == 1;
                    assert_eq!(actual.transformed, should_rebind);
                    if !should_rebind {
                        assert_eq!(actual.data, original);
                        continue;
                    }
                    let LogicalPlan::Projection(consumer) = actual.data else {
                        panic!("consumer projection changed")
                    };
                    assert!(Arc::ptr_eq(&consumer.schema, &consumer_schema));
                    assert_eq!(consumer.expr, vec![consumer_expr]);
                    let LogicalPlan::SubqueryAlias(alias) = consumer.input.as_ref() else {
                        panic!("subquery alias changed")
                    };
                    assert!(Arc::ptr_eq(&alias.schema, &alias_schema));
                    let LogicalPlan::Projection(producer) = alias.input.as_ref() else {
                        panic!("producer projection changed")
                    };
                    assert!(Arc::ptr_eq(&producer.schema, &producer_schema));
                    assert!(Arc::ptr_eq(&producer.input, &source));
                    let expected = call_null_propagating_string(Arc::clone(&outer), vec![col("s")]);
                    let expected = if wrap_producer {
                        wrap(expected)
                    } else {
                        expected
                    }
                    .alias("u");
                    assert_eq!(producer.expr, vec![expected]);
                    let (_, original_shells) =
                        case_conversion_shells(&producer_expr, source.schema())?;
                    let (_, actual_shells) =
                        case_conversion_shells(&producer.expr[0], source.schema())?;
                    assert_eq!(actual_shells.len(), original_shells.len());
                    for (actual, original) in actual_shells.iter().zip(original_shells) {
                        match (actual, original) {
                            (expr::Expr::Cast(a), expr::Expr::Cast(b)) => {
                                assert!(Arc::ptr_eq(&a.field, &b.field))
                            }
                            (expr::Expr::TryCast(a), expr::Expr::TryCast(b)) => {
                                assert!(Arc::ptr_eq(&a.field, &b.field))
                            }
                            (expr::Expr::ScalarFunction(a), expr::Expr::ScalarFunction(b)) => {
                                assert!(Arc::ptr_eq(&a.func, &b.func))
                            }
                            (expr::Expr::Alias(a), expr::Expr::Alias(b)) => {
                                assert_eq!(a.name, b.name);
                                assert_eq!(a.metadata, b.metadata);
                            }
                            _ => panic!("producer shell variant changed"),
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
