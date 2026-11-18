/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Path evaluation and write styles adapted from Apache Spark revision
// 32f7299601108917fb01920a54e084595b7b3bf8:
// https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/json/JsonExpressionEvalUtils.scala

use std::borrow::Cow;
use std::fmt::Write;

use chumsky::prelude::*;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, JsonValue};
use sail_common_datafusion::formatter::format_float64;

use super::common::{GetError, JsonPath, invoke};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkGetJsonObject {
    signature: Signature,
}

impl Default for SparkGetJsonObject {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkGetJsonObject {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkGetJsonObject {
    fn name(&self) -> &str {
        "get_json_object"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() != 2 {
            return plan_err!("get_json_object expects 2 arguments");
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar_path = match args.args.get(1) {
            Some(ColumnarValue::Scalar(value)) => Some(match value {
                ScalarValue::Utf8(Some(path))
                | ScalarValue::Utf8View(Some(path))
                | ScalarValue::LargeUtf8(Some(path)) => parse_path(path),
                _ => None,
            }),
            _ => None,
        };
        invoke::<StringArray>(&args.args, |document, path| {
            let parsed;
            let steps = if let Some(path) = &scalar_path {
                path.as_deref().ok_or(GetError)?
            } else {
                let [JsonPath::Key(path)] = path else {
                    return Err(GetError);
                };
                parsed = parse_path(path).ok_or(GetError)?;
                &parsed
            };
            extract(document, steps)
        })
    }
}

#[derive(Clone, Debug)]
enum JsonPathStep {
    Key(String),
    Index(usize),
    Wildcard,
    FieldWildcard,
}

fn parse_path<'a>(path: &'a str) -> Option<Vec<JsonPathStep>> {
    let key = just::<_, &'a str, extra::Err<Rich<'a, char>>>('.')
        .ignore_then(
            none_of(['.', '['])
                .repeated()
                .at_least(1)
                .collect::<String>(),
        )
        .map(JsonPathStep::Key);
    let quoted = just("['")
        .ignore_then(none_of('\'').repeated().at_least(1).collect::<String>())
        .then_ignore(just("']"))
        .map(JsonPathStep::Key);
    let index = just('[')
        .ignore_then(
            any()
                .filter(|c: &char| c.is_ascii_digit())
                .repeated()
                .at_least(1)
                .collect::<String>(),
        )
        .then_ignore(just(']'))
        .try_map(|number, span| {
            number
                .parse::<i64>()
                .ok()
                .and_then(|index| usize::try_from(index).ok())
                .map(JsonPathStep::Index)
                .ok_or_else(|| Rich::custom(span, "invalid array index"))
        });
    let wildcard = just("[*]").to(JsonPathStep::Wildcard);
    let field_wildcard = choice((just(".*"), just("['*']"))).to(JsonPathStep::FieldWildcard);
    just('$')
        .ignore_then(
            choice((field_wildcard, key, quoted, wildcard, index))
                .repeated()
                .collect::<Vec<_>>(),
        )
        .then_ignore(end())
        .parse(path)
        .into_result()
        .ok()
}

#[derive(Clone, Copy, PartialEq)]
enum JsonWriteStyle {
    Raw,
    Quoted,
    Flatten,
}

enum JsonPathOutput<'a, 'j> {
    Value {
        value: &'a JsonValue<'j>,
        unquote_string: bool,
    },
    Array(Vec<JsonPathOutput<'a, 'j>>),
    BufferedArray {
        values: Vec<JsonPathOutput<'a, 'j>>,
        keep_outer_array: bool,
    },
}

#[derive(Default)]
struct JsonPathResult<'a, 'j> {
    matched: bool,
    output: Vec<JsonPathOutput<'a, 'j>>,
}

fn evaluate<'a, 'j>(
    value: &'a JsonValue<'j>,
    path: &[JsonPathStep],
    style: JsonWriteStyle,
) -> JsonPathResult<'a, 'j> {
    let Some((step, rest)) = path.split_first() else {
        if style == JsonWriteStyle::Flatten
            && let JsonValue::Array(items) = value
        {
            let mut result = JsonPathResult::default();
            for item in items.iter() {
                let selected = evaluate(item, &[], style);
                result.matched |= selected.matched;
                result.output.extend(selected.output);
            }
            return result;
        }
        return JsonPathResult {
            matched: true,
            output: vec![JsonPathOutput::Value {
                value,
                unquote_string: style == JsonWriteStyle::Raw,
            }],
        };
    };
    match (value, step) {
        (JsonValue::Object(fields), JsonPathStep::Key(key)) => {
            let mut result = JsonPathResult::default();
            for (_, value) in fields
                .iter()
                .filter(|(name, value)| name == key && !matches!(value, JsonValue::Null))
            {
                let selected = evaluate(value, rest, style);
                result.output.extend(selected.output);
                if selected.matched {
                    result.matched = true;
                    break;
                }
            }
            result
        }
        (JsonValue::Array(items), JsonPathStep::Index(index)) => {
            let style = if matches!(rest.first(), Some(JsonPathStep::Wildcard)) {
                JsonWriteStyle::Quoted
            } else {
                style
            };
            items
                .get(*index)
                .map(|item| evaluate(item, rest, style))
                .unwrap_or_default()
        }
        (JsonValue::Array(items), JsonPathStep::Wildcard)
            if matches!(rest.first(), Some(JsonPathStep::Wildcard)) =>
        {
            let mut result = JsonPathResult::default();
            let mut output = Vec::new();
            for item in items.iter() {
                let selected = evaluate(item, &rest[1..], JsonWriteStyle::Flatten);
                result.matched |= selected.matched;
                output.extend(selected.output);
            }
            result.output.push(JsonPathOutput::Array(output));
            result
        }
        (JsonValue::Array(items), JsonPathStep::Wildcard) => {
            let next_style = if style == JsonWriteStyle::Raw {
                JsonWriteStyle::Quoted
            } else {
                style
            };
            let mut matches = 0;
            let mut output = Vec::new();
            for item in items.iter() {
                let selected = evaluate(item, rest, next_style);
                matches += usize::from(selected.matched);
                output.extend(selected.output);
            }
            let matched = matches > 0;
            // Quoted wildcards emit an array even when no descendant matched. Their
            // parent retains that output while independently counting successful matches.
            let output = if style == JsonWriteStyle::Quoted {
                vec![JsonPathOutput::Array(output)]
            } else if matched {
                vec![JsonPathOutput::BufferedArray {
                    values: output,
                    keep_outer_array: matches > 1,
                }]
            } else {
                Vec::new()
            };
            JsonPathResult { matched, output }
        }
        _ => JsonPathResult::default(),
    }
}

// Spark escapes supplementary Unicode in its UTF8 generator, but writes it literally in buffers.
#[derive(Clone, Copy)]
enum JsonUnicodeEscaping {
    SurrogatePairs,
    LiteralCharacters,
}

#[derive(Clone, Copy)]
enum JsonOutputContext {
    Root,
    Array,
}

fn write_quoted_string(
    text: &str,
    output: &mut String,
    escaping: JsonUnicodeEscaping,
) -> std::result::Result<(), GetError> {
    output.push('"');
    for character in text.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\x08' => output.push_str("\\b"),
            '\x0c' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            c if c < ' ' => write!(output, "\\u{:04X}", c as u32).map_err(|_| GetError)?,
            c if c.len_utf16() == 2 && matches!(escaping, JsonUnicodeEscaping::SurrogatePairs) => {
                for unit in c.encode_utf16(&mut [0; 2]) {
                    write!(output, "\\u{unit:04X}").map_err(|_| GetError)?;
                }
            }
            c => output.push(c),
        }
    }
    output.push('"');
    Ok(())
}

fn write_json(
    value: &JsonValue<'_>,
    output: &mut String,
    escaping: JsonUnicodeEscaping,
) -> std::result::Result<(), GetError> {
    match value {
        JsonValue::Null => output.push_str("null"),
        JsonValue::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        JsonValue::Int(value) => write!(output, "{value}").map_err(|_| GetError)?,
        JsonValue::BigInt(value) => write!(output, "{value}").map_err(|_| GetError)?,
        JsonValue::Float(value) => {
            let text = format_float64(*value);
            if value.is_finite() {
                output.push_str(&text);
            } else {
                write_quoted_string(&text, output, escaping)?;
            }
        }
        JsonValue::Str(value) => write_quoted_string(value, output, escaping)?,
        JsonValue::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_json(value, output, escaping)?;
            }
            output.push(']');
        }
        JsonValue::Object(fields) => {
            output.push('{');
            for (index, (key, value)) in fields.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_quoted_string(key, output, escaping)?;
                output.push(':');
                write_json(value, output, escaping)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

fn write_output(
    values: &[JsonPathOutput<'_, '_>],
    output: &mut String,
    context: JsonOutputContext,
    escaping: JsonUnicodeEscaping,
) -> std::result::Result<(), GetError> {
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(match context {
                JsonOutputContext::Root => ' ',
                JsonOutputContext::Array => ',',
            });
        }
        match value {
            JsonPathOutput::Value {
                value: JsonValue::Str(value),
                unquote_string: true,
            } => output.push_str(value),
            JsonPathOutput::Value { value, .. } => write_json(value, output, escaping)?,
            JsonPathOutput::Array(values) => {
                output.push('[');
                write_output(values, output, JsonOutputContext::Array, escaping)?;
                output.push(']');
            }
            JsonPathOutput::BufferedArray {
                values,
                keep_outer_array,
            } => {
                if *keep_outer_array {
                    output.push('[');
                }
                // Spark buffers non-quoted wildcards in a character generator,
                // then copies the complete buffer or its unwrapped contents as one value.
                write_output(
                    values,
                    output,
                    JsonOutputContext::Array,
                    JsonUnicodeEscaping::LiteralCharacters,
                )?;
                if *keep_outer_array {
                    output.push(']');
                }
            }
        }
    }
    Ok(())
}

// Spark's JSON reader accepts single quotes and unescaped string controls.
fn normalize_strings(document: &str) -> std::result::Result<Cow<'_, str>, GetError> {
    if !document.contains('\'') && !document.bytes().any(|byte| byte < b' ') {
        return Ok(Cow::Borrowed(document));
    }
    let mut output = String::with_capacity(document.len());
    let mut chars = document.chars();
    while let Some(character) = chars.next() {
        if matches!(character, '\'' | '"') {
            output.push('"');
            loop {
                match chars.next().ok_or(GetError)? {
                    c if c == character => break,
                    '\\' => {
                        let escaped = chars.next().ok_or(GetError)?;
                        if escaped == '\'' {
                            output.push('\'');
                        } else {
                            output.push('\\');
                            output.push(escaped);
                        }
                    }
                    '"' => output.push_str("\\\""),
                    c if c < ' ' => write!(output, "\\u{:04x}", c as u32).map_err(|_| GetError)?,
                    c => output.push(c),
                }
            }
            output.push('"');
        } else {
            output.push(character);
        }
    }
    Ok(Cow::Owned(output))
}

fn extract(document: Option<&str>, path: &[JsonPathStep]) -> std::result::Result<String, GetError> {
    let document = normalize_strings(document.ok_or(GetError)?)?;
    // Jiter bounds nesting before the recursive path walk and single-buffer writer.
    let value = Jiter::new(document.as_bytes()).next_value()?;
    let selected = evaluate(&value, path, JsonWriteStyle::Raw);
    if !selected.matched {
        return Err(GetError);
    }
    let mut output = String::new();
    write_output(
        &selected.output,
        &mut output,
        JsonOutputContext::Root,
        JsonUnicodeEscaping::SurrogatePairs,
    )?;
    Ok(output)
}
