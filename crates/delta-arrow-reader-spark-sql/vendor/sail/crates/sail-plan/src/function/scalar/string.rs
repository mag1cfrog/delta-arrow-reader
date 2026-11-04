// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::functions::expr_fn;
use datafusion::functions::regex::expr_fn as regex_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::regex::regexpinstr::RegexpInstrFunc;
use datafusion::functions::string as native_string;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ExprSchemable, HigherOrderUDF, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, cast, expr, lit, try_cast, when,
};
use datafusion_functions_nested::expr_fn::array_element;
use datafusion_spark::function::math::expr_fn as math_fn;
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::expr_fn as string_fn;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use datafusion_spark::function::string::length::SparkLengthFunc;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
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
    ScalarFunction, ScalarFunctionInput, literal_value_without_evaluation,
};
use crate::function::scalar::datetime::date_format;
use crate::function::scalar::lambda::lambda_with_fresh_parameter;

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
    if position.is_volatile() || !position.column_refs().is_empty() {
        return Err(PlanError::invalid(
            "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] regexp_replace position must be a foldable INT expression",
        ));
    }
    if !matches!(
        position.get_type(function_context.schema)?,
        DataType::Int32 | DataType::Null
    ) {
        return Err(PlanError::invalid(
            "regexp_replace position coercion to INT is outside the supported position contract",
        ));
    }
    let position = LiteralEvaluator::new().evaluate(&position)?;
    let position = match position {
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
    arguments.push(lit(position));
    Ok(ScalarUDF::from(SparkRegexpReplace::new()).call(arguments))
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
                let data_type = argument.get_type(function_context.schema)?;
                let converted = match data_type {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        let mode = if function_context.plan_config.ansi_mode {
                            StringIntegerCastMode::Ansi
                        } else {
                            StringIntegerCastMode::Legacy
                        };
                        SparkCastStringToInteger::try_new(DataType::Int32, mode)?
                            .into_expr(argument)
                    }
                    DataType::Decimal128(..) | DataType::Decimal256(..) => {
                        let mode = if function_context.plan_config.ansi_mode {
                            DecimalCastMode::Ansi
                        } else {
                            DecimalCastMode::Legacy
                        };
                        ScalarUDF::from(SparkDecimalCast::to_integer(DataType::Int32, mode)?)
                            .call(vec![argument])
                    }
                    // Retain legacy BIGINT behavior until the shared CAST policy
                    // supplies Spark INT narrowing (#813).
                    DataType::Int64 if !function_context.plan_config.ansi_mode => argument,
                    DataType::Null
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64 => cast(argument, DataType::Int32),
                    _ => {
                        let parameter = if index == 1 { "position" } else { "length" };
                        return Err(PlanError::invalid(format!(
                            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] substring {parameter} requires INT, got {data_type}"
                        )));
                    }
                };
                // The value kernel consumes Int64 after Spark's INT conversion.
                cast(converted, DataType::Int64)
            };
            Ok(argument)
        })
        .collect::<PlanResult<Vec<_>>>()?;
    let logical = ScalarUDF::from(
        SparkSubstring::new()
            .with_logical_arguments()
            .with_known_null_argument(known_null),
    )
    .call(arguments);
    // Constant parents keep their cumulative NULL evaluation. Nonconstant calls
    // expose ordinary constant descendants until physical runtime lowering.
    if !known_null && crate::expression_errors::row_expression(&logical, false)? {
        return substring_lambdas(logical).map(|value| value.data);
    }
    Ok(logical)
}

fn substring_lambdas(expression: expr::Expr) -> PlanResult<Transformed<expr::Expr>> {
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

pub(crate) fn lower_runtime_substring(
    expression: expr::Expr,
) -> PlanResult<Transformed<expr::Expr>> {
    substring_lambdas(expression)
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
/// propagate NULL. Signature, output type, metadata and values remain native.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NullPropagatingStringFunction(Arc<ScalarUDF>);

impl ScalarUDFImpl for NullPropagatingStringFunction {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn aliases(&self) -> &[String] {
        self.0.aliases()
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        self.0.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion_common::Result<FieldRef> {
        // The resolver asks for metadata before ordinary signature validation.
        if args.arg_fields.is_empty() {
            return datafusion_common::plan_err!("{} requires at least one argument", self.name());
        }
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

fn call_null_propagating_string(
    function: Arc<ScalarUDF>,
    arguments: Vec<expr::Expr>,
) -> expr::Expr {
    ScalarUDF::from(NullPropagatingStringFunction(function)).call(arguments)
}

fn lower(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(call_null_propagating_string(
        native_string::lower(),
        vec![validate_utf8(input)?],
    ))
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

fn rev_args(
    func: impl Fn(Vec<expr::Expr>) -> expr::Expr,
) -> impl Fn(Vec<expr::Expr>) -> expr::Expr {
    move |args: Vec<expr::Expr>| func(args.into_iter().rev().collect())
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
            F::var_arg(rev_args(|args| {
                call_null_propagating_string(native_string::ltrim(), args)
            })),
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
        ("regexp_extract", F::udf(SparkRegexpExtract::new())),
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
            F::var_arg(rev_args(|args| {
                call_null_propagating_string(native_string::rtrim(), args)
            })),
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
        (
            "trim",
            F::var_arg(rev_args(|args| {
                call_null_propagating_string(native_string::btrim(), args)
            })),
        ),
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
fn substring_logical_visibility_and_runtime_lowering_do_not_invoke_operands()
-> datafusion_common::Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion_expr::{Expr, ScalarUDFImpl, Volatility, col, create_udf};
    use sail_function::scalar::collection::spark_concat::SparkConcat;

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
    // Spark's logical substring permits common-subexpression extraction before
    // the runtime HOF supplies cumulative NULL selection.
    assert!(!function.short_circuits());
    assert!(function.conditional_arguments(&args).is_none());
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
        !lower_runtime_substring(malformed)
            .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?
            .transformed
    );
    let lowered = lower_runtime_substring(logical)
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
    assert!(matches!(
        ordinary.simplify(args, &info)?,
        ExprSimplifyResult::Original(_)
    ));
    Ok(())
}
