// Spark replacement adapter for the Delta reader experiment.
// See crates/delta-arrow-reader-spark-sql/tests/fixtures/regexp-replace-README.md.
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, GenericStringBuilder, OffsetSizeTrait};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_datafusion_err, exec_err, internal_err};
use datafusion_expr::function::Hint;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use regex::{Captures, Regex};

use crate::functions_utils::{StrMemo, make_scalar_function};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegexpReplace {
    signature: Signature,
    preparation_errors: bool,
}

impl Default for SparkRegexpReplace {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegexpReplace {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            preparation_errors: false,
        }
    }

    pub fn with_preparation_errors(mut self) -> Self {
        self.preparation_errors = true;
        self
    }

    pub fn prepares_literal_errors(&self) -> bool {
        self.preparation_errors
    }
}

impl ScalarUDFImpl for SparkRegexpReplace {
    fn name(&self) -> &str {
        "regexp_replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if !self.preparation_errors {
            return Ok(ExprSimplifyResult::Original(args));
        }
        // Native passes simplify and retain operands first. Never invoke an operand here.
        let Some(values) = args
            .iter()
            .map(|arg| match arg {
                Expr::Literal(value, _) => Some(value.clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        let types = values
            .iter()
            .map(|value| value.data_type())
            .collect::<Vec<_>>();
        if self
            .coerce_types(&types)
            .is_ok_and(|coerced| coerced == types)
        {
            let values = values
                .into_iter()
                .map(ColumnarValue::Scalar)
                .collect::<Vec<_>>();
            let _ = regexp_replace_values(&values)?;
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .first()
            .cloned()
            .ok_or_else(|| exec_datafusion_err!("regexp_replace requires a source"))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            args.arg_fields[0].data_type().clone(),
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [source, pattern, replacement, position] = arg_types else {
            return exec_err!("regexp_replace requires three strings and an INT position");
        };
        let mut types = [source, pattern, replacement]
            .into_iter()
            .map(|data_type| match data_type {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(data_type.clone()),
                DataType::Null => Ok(DataType::Utf8),
                other => exec_err!("regexp_replace requires STRING, got {other}"),
            })
            .collect::<Result<Vec<_>>>()?;
        if !matches!(position, DataType::Int32 | DataType::Null) {
            return exec_err!("regexp_replace requires an INT position, got {position}");
        }
        types.push(DataType::Int32);
        Ok(types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        regexp_replace_values(&args.args)
    }
}

fn regexp_replace_values(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    make_scalar_function(
        regexp_replace,
        vec![
            Hint::Pad,
            Hint::AcceptsSingular,
            Hint::AcceptsSingular,
            Hint::AcceptsSingular,
        ],
    )(args)
}

fn string_at(array: &ArrayRef, row: usize) -> Result<Option<&str>> {
    let row = if array.len() == 1 { 0 } else { row };
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(row),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row),
        DataType::Utf8View => array.as_string_view().value(row),
        other => return internal_err!("regexp_replace expected a string array, got {other}"),
    }))
}

fn regexp_replace(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::LargeUtf8 => regexp_replace_strings::<i64>(args),
        _ => regexp_replace_strings::<i32>(args),
    }
}

fn regexp_replace_strings<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [source, pattern, replacement, position] = args else {
        return internal_err!("regexp_replace expected four arguments");
    };
    let position = position.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
    let mut builder = GenericStringBuilder::<O>::new();
    let mut regex_memo = StrMemo::new();
    let mut output = String::new();
    for row in 0..source.len() {
        let position_row = if position.len() == 1 { 0 } else { row };
        let (Some(value), Some(pattern), Some(replacement)) = (
            string_at(source, row)?,
            string_at(pattern, row)?,
            string_at(replacement, row)?,
        ) else {
            builder.append_null();
            continue;
        };
        if position.is_null(position_row) {
            builder.append_null();
            continue;
        }
        let position = position.value(position_row);
        if position <= 0 {
            return exec_err!(
                "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] regexp_replace position must be positive, got {position}"
            );
        }
        // Compile only live rows, before checking whether the region is past the end.
        let regex = regex_memo.get_or_try_insert_ref(pattern, |pattern| {
            Regex::new(pattern).map_err(|error| {
                exec_datafusion_err!(
                    "[INVALID_PARAMETER_VALUE.PATTERN] regexp_replace pattern {pattern:?}: {error}"
                )
            })
        })?;
        let replacement_error = |cause| {
            exec_datafusion_err!(
                "[INVALID_REGEXP_REPLACE] source={value:?}, pattern={pattern:?}, replacement={replacement:?}, position={position}: {cause}"
            )
        };
        if pattern.is_empty() || pattern == r"\B" {
            let units = value.encode_utf16().collect::<Vec<_>>();
            let start = (position - 1) as usize;
            if start != 0 && start >= units.len() {
                builder.append_value(value);
                continue;
            }
            let mut matches = Vec::new();
            if !pattern.is_empty() {
                matches.resize(units.len() + 1, true);
                let mut previous_word = false;
                let mut offset = 0;
                let mut character_buffer = [0; 4];
                for character in value.chars() {
                    // Retain native word classification at scalar boundaries.
                    // Surrogate midpoints have two nonword halves; only the
                    // actual region start hides its preceding character.
                    let word = !regex.is_match(character.encode_utf8(&mut character_buffer));
                    matches[offset] = (offset != start && previous_word) == word;
                    offset += character.len_utf16();
                    previous_word = word;
                }
                matches[offset] = !previous_word;
                if !matches[start..].iter().any(|matched| *matched) {
                    builder.append_value(value);
                    continue;
                }
            }
            let captures = regex.captures("").ok_or_else(|| {
                exec_datafusion_err!("regexp_replace zero-width pattern has no match")
            })?;
            output.clear();
            append_replacement(&mut output, replacement, regex, &captures)
                .map_err(replacement_error)?;
            let replacement_units = output.encode_utf16().collect::<Vec<_>>();
            let mut result_units = units[..start].to_vec();
            for index in start..units.len() {
                if matches.is_empty() || matches[index] {
                    result_units.extend_from_slice(&replacement_units);
                }
                result_units.push(units[index]);
            }
            if matches.is_empty() || matches[units.len()] {
                result_units.extend_from_slice(&replacement_units);
            }
            // Encode after assembly so an empty replacement can preserve a pair,
            // including when the untouched prefix ends inside that pair.
            output.clear();
            output.extend(char::decode_utf16(result_units).map(|unit| unit.unwrap_or('?')));
            builder.append_value(&output);
            continue;
        }
        let start = utf16_start(value, (position - 1) as usize)?;
        let Some(start) = start else {
            builder.append_value(value);
            continue;
        };
        output.clear();
        output.push_str(&value[..start]);
        let region = &value[start..];
        let mut end = 0;
        let mut search = 0;
        while let Some(captures) = regex.captures_at(region, search) {
            let matched = captures
                .get(0)
                .ok_or_else(|| exec_datafusion_err!("regexp_replace match has no full capture"))?;
            output.push_str(&region[end..matched.start()]);
            append_replacement(&mut output, replacement, regex, &captures)
                .map_err(replacement_error)?;
            end = matched.end();
            // Java admits an empty match immediately after a nonempty match.
            // Regex::captures_iter skips that match, so drive the search directly.
            search = end;
            if matched.is_empty() {
                let Some(next) = region[end..].chars().next() else {
                    break;
                };
                // Native regex searches only Unicode scalar boundaries. Java's
                // additional matches inside surrogate pairs remain an external gap.
                search += next.len_utf8();
            }
        }
        output.push_str(&region[end..]);
        builder.append_value(&output);
    }
    let result = Arc::new(builder.finish()) as ArrayRef;
    if result.data_type() == source.data_type() {
        Ok(result)
    } else {
        cast(&result, source.data_type()).map_err(Into::into)
    }
}

// Java Matcher regions use UTF-16 units and anchoring bounds at the region start.
fn utf16_start(value: &str, position: usize) -> Result<Option<usize>> {
    if position == 0 {
        return Ok(Some(0));
    }
    let mut units = 0;
    for (byte, character) in value.char_indices() {
        if units == position {
            return Ok(Some(byte));
        }
        units += character.len_utf16();
        if units > position {
            return exec_err!(
                "regexp_replace cannot start a native regex region inside a UTF-16 surrogate pair"
            );
        }
    }
    // Spark leaves the source unchanged when a noninitial region starts at its end.
    Ok(None)
}

// Java Matcher.appendReplacement parses replacements only after finding a match.
fn append_replacement(
    output: &mut String,
    replacement: &str,
    regex: &Regex,
    captures: &Captures<'_>,
) -> Result<()> {
    let mut characters = replacement.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '\\' => output.push(characters.next().ok_or_else(|| {
                exec_datafusion_err!(
                    "java.lang.IllegalArgumentException: character to be escaped is missing"
                )
            })?),
            '$' => {
                let first = characters.next().ok_or_else(|| exec_datafusion_err!(
                    "java.lang.IllegalArgumentException: Illegal group reference: group index is missing"
                ))?;
                let group = if first == '{' {
                    let mut name = String::new();
                    while characters.peek().is_some_and(char::is_ascii_alphanumeric) {
                        if let Some(character) = characters.next() {
                            name.push(character);
                        }
                    }
                    if name.is_empty() {
                        return exec_err!(
                            "java.lang.IllegalArgumentException: named capturing group has 0 length name"
                        );
                    }
                    if characters.next() != Some('}') {
                        return exec_err!(
                            "java.lang.IllegalArgumentException: named capturing group is missing trailing '}}'"
                        );
                    }
                    if name.as_bytes()[0].is_ascii_digit() {
                        return exec_err!(
                            "java.lang.IllegalArgumentException: capturing group name {{{name}}} starts with digit character"
                        );
                    }
                    regex
                        .capture_names()
                        .position(|candidate| candidate == Some(name.as_str()))
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "java.lang.IllegalArgumentException: No group with name {{{name}}}"
                            )
                        })?
                } else if first.is_ascii_digit() {
                    let mut group = (first as u8 - b'0') as usize;
                    while let Some(digit) = characters.peek().filter(|digit| digit.is_ascii_digit())
                    {
                        let next = group
                            .checked_mul(10)
                            .and_then(|value| value.checked_add((*digit as u8 - b'0') as usize));
                        match next {
                            Some(next) if next < captures.len() => {
                                group = next;
                                characters.next();
                            }
                            _ => break,
                        }
                    }
                    if group >= captures.len() {
                        return exec_err!("java.lang.IndexOutOfBoundsException: No group {group}");
                    }
                    group
                } else {
                    return exec_err!(
                        "java.lang.IllegalArgumentException: Illegal group reference"
                    );
                };
                if let Some(value) = captures.get(group) {
                    output.push_str(value.as_str());
                }
            }
            literal => output.push(literal),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};

    #[test]
    fn preparation_policy_has_distinct_native_udf_identity() -> Result<()> {
        use datafusion_expr::ScalarUDF;
        use std::hash::{DefaultHasher, Hash, Hasher};

        let native = ScalarUDF::from(SparkRegexpReplace::new());
        let spark = ScalarUDF::from(SparkRegexpReplace::new().with_preparation_errors());
        let fingerprint = |function: &ScalarUDF| {
            let mut hasher = DefaultHasher::new();
            function.hash(&mut hasher);
            hasher.finish()
        };
        assert_ne!(native, spark);
        assert_ne!(fingerprint(&native), fingerprint(&spark));
        assert!(!native.inner().propagates_constant_folding_errors());
        let alias = spark.with_aliases(["replacement_alias"]);
        assert!(!alias.inner().propagates_constant_folding_errors());
        let info = SimplifyContext::builder()
            .with_schema(Arc::new(datafusion_common::DFSchema::empty()))
            .build();
        let args = vec![
            datafusion_expr::lit("a"),
            datafusion_expr::lit("(a)"),
            datafusion_expr::lit("$9"),
            datafusion_expr::lit(1_i32),
        ];
        assert!(matches!(
            native.simplify(args.clone(), &info)?,
            ExprSimplifyResult::Original(_)
        ));
        let error = match alias.simplify(args, &info) {
            Err(error) => error.to_string(),
            Ok(_) => return exec_err!("prepared alias did not check its own literal kernel"),
        };
        assert!(error.ends_with("java.lang.IndexOutOfBoundsException: No group 9"));
        Ok(())
    }

    #[test]
    fn sliced_replacements_skip_null_rows_and_keep_string_storage() -> Result<()> {
        let source = StringArray::from(vec![
            Some("outside"),
            Some("a"),
            None,
            Some("a"),
            Some("outside"),
        ]);
        let pattern = StringArray::from(vec!["outside", "(a)", "[", "(a)", "outside"]);
        let replacement = StringArray::from(vec!["outside", "$100", "$", r"\$1", "outside"]);
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let source = cast(&(Arc::new(source.clone()) as ArrayRef), &data_type)?.slice(1, 3);
            let pattern = cast(&(Arc::new(pattern.clone()) as ArrayRef), &data_type)?.slice(1, 3);
            let replacement =
                cast(&(Arc::new(replacement.clone()) as ArrayRef), &data_type)?.slice(1, 3);
            let position = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
            let result = regexp_replace(&[source, pattern, replacement, position])?;
            assert_eq!(result.data_type(), &data_type);
            let result = cast(&result, &DataType::Utf8)?;
            assert_eq!(
                result.as_string::<i32>(),
                &StringArray::from(vec![Some("a00"), None, Some("$1")])
            );
        }
        Ok(())
    }

    #[test]
    fn sliced_empty_patterns_assemble_utf16_before_encoding() -> Result<()> {
        let source = StringArray::from(vec![
            Some("outside"),
            Some("a😀b"),
            Some("a😀b"),
            Some("a😀b"),
            None,
            Some("é中𐐷z"),
            Some("a😀b"),
            Some("a😀b"),
            Some("a😀b"),
            Some(""),
            Some("a😀b"),
            Some("outside"),
        ]);
        let pattern = StringArray::from(vec![
            "outside", "", "", "", "[", "", ".", ".?", "", "", "", "outside",
        ]);
        let replacement = StringArray::from(vec![
            "outside", "X", "", "$0", "$9", "Ω🦊", "X", "X", "$9", "$9", "$9", "outside",
        ]);
        let position = Int32Array::from(vec![
            Some(1),
            Some(1),
            Some(3),
            Some(3),
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(5),
            Some(2),
            None,
            Some(1),
        ]);
        let expected = StringArray::from(vec![
            Some("XaX?X?XbX"),
            Some("a😀b"),
            Some("a😀b"),
            None,
            Some("Ω🦊éΩ🦊中Ω🦊?Ω🦊?Ω🦊zΩ🦊"),
            Some("XXX"),
            Some("XXXX"),
            Some("a😀b"),
            Some(""),
            None,
        ]);
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let source = cast(&(Arc::new(source.clone()) as ArrayRef), &data_type)?.slice(1, 10);
            let pattern = cast(&(Arc::new(pattern.clone()) as ArrayRef), &data_type)?.slice(1, 10);
            let replacement =
                cast(&(Arc::new(replacement.clone()) as ArrayRef), &data_type)?.slice(1, 10);
            let position = (Arc::new(position.clone()) as ArrayRef).slice(1, 10);
            let result = regexp_replace(&[source, pattern, replacement, position])?;
            assert_eq!(result.data_type(), &data_type);
            let result = cast(&result, &DataType::Utf8)?;
            assert_eq!(result.as_string::<i32>(), &expected);
        }
        Ok(())
    }

    #[test]
    fn sliced_nonboundaries_keep_utf16_regions_and_lazy_replacements() -> Result<()> {
        let source = StringArray::from(vec![
            Some("outside"),
            Some("a😀b"),
            Some("a😀b"),
            Some("a😀b"),
            Some("𐐷"),
            Some("a𐐷b"),
            Some("a😀b"),
            Some("a😀b"),
            Some("a😀b"),
            Some("a"),
            Some(""),
            None,
            Some("a😀b"),
            Some("a😀b"),
            Some("a😀b"),
            Some("a😀b"),
            Some("outside"),
        ]);
        let pattern = StringArray::from(vec![
            Some("outside"),
            Some(r"\B"),
            Some(r"\B"),
            Some(r"\B"),
            Some(r"\B"),
            Some(r"\B"),
            Some(r"\B"),
            Some(r"\B"),
            Some(r"\B"),
            Some(r"\B"),
            Some(r"\B"),
            Some(r"\B"),
            None,
            Some(r"\B"),
            Some(""),
            Some("."),
            Some("outside"),
        ]);
        let replacement = StringArray::from(vec![
            "outside", "X", "X", "X", "X", "X", "", "$0", "$1", "$1", "X", "$9", "$9", "$9", "X",
            "X", "outside",
        ]);
        let position = Int32Array::from(vec![
            Some(1),
            Some(1),
            Some(2),
            Some(3),
            Some(1),
            Some(1),
            Some(3),
            Some(3),
            Some(5),
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            None,
            Some(1),
            Some(1),
            Some(1),
        ]);
        let expected = StringArray::from(vec![
            Some("a?X?b"),
            Some("aX?X?b"),
            Some("a?X?b"),
            Some("?X?"),
            Some("aX?X?Xb"),
            Some("a😀b"),
            Some("a😀b"),
            Some("a😀b"),
            Some("a"),
            Some("X"),
            None,
            None,
            None,
            Some("XaX?X?XbX"),
            Some("XXX"),
        ]);
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let source = cast(&(Arc::new(source.clone()) as ArrayRef), &data_type)?.slice(1, 15);
            let pattern = cast(&(Arc::new(pattern.clone()) as ArrayRef), &data_type)?.slice(1, 15);
            let replacement =
                cast(&(Arc::new(replacement.clone()) as ArrayRef), &data_type)?.slice(1, 15);
            let position = (Arc::new(position.clone()) as ArrayRef).slice(1, 15);
            let result = regexp_replace(&[source, pattern, replacement, position])?;
            assert_eq!(result.data_type(), &data_type);
            let result = cast(&result, &DataType::Utf8)?;
            assert_eq!(result.as_string::<i32>(), &expected);
        }
        let error = regexp_replace(&[
            Arc::new(StringArray::from(vec!["a😀b"])),
            Arc::new(StringArray::from(vec![r"\B"])),
            Arc::new(StringArray::from(vec!["$1"])),
            Arc::new(Int32Array::from(vec![3])),
        ])
        .err()
        .ok_or_else(|| exec_datafusion_err!("live missing group must fail"))?;
        assert!(error.to_string().contains("No group 1"));
        Ok(())
    }
}
