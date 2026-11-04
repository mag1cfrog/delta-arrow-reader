// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::functions::expr_fn;
use datafusion::functions::regex::expr_fn as regex_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::regex::regexpinstr::RegexpInstrFunc;
use datafusion::functions::string as native_string;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, cast, expr, lit, try_cast, when,
};
use datafusion_functions_nested::expr_fn::array_element;
use datafusion_spark::function::math::expr_fn as math_fn;
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::expr_fn as string_fn;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use datafusion_spark::function::string::length::SparkLengthFunc;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
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
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::function::scalar::datetime::date_format;

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
    Ok(ScalarUDF::from(SparkSubstring::new()).call(vec![value, position, length]))
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
