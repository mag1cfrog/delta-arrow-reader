// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ListBuilder, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use xee_xpath::query::SequenceQuery;
use xee_xpath::{DocumentHandle, Documents, Item, Queries, Query};

use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Xpath {
    signature: Signature,
}

impl Default for Xpath {
    fn default() -> Self {
        Self::new()
    }
}

impl Xpath {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Xpath {
    fn name(&self) -> &str {
        "xpath"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `XPathExtract.nullable = true`, unconditional; `XPathList` inherits it and
    /// declares no `nullable` of its own.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/xml/xpath.scala#L40>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!(
                "`xpath` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        arg_types
            .iter()
            .map(|data_type| match data_type {
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null => {
                    Ok(DataType::Utf8)
                }
                other => {
                    plan_err!("The `xpath` function can only accept strings, but got {other:?}.")
                }
            })
            .collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(xpath_inner, vec![])(&args)
    }
}

fn xpath_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [xmls, paths] = take_function_args("xpath", args)?;
    let xmls = xmls.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Internal("xpath expected a string array for xml".to_string())
    })?;
    let paths = paths
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("xpath expected a string array for path".to_string())
        })?;

    let mut builder = ListBuilder::new(StringBuilder::new());
    for_each_xpath_row(
        xmls,
        paths,
        |path| {
            Queries::default().sequence(path).map_err(|error| {
                DataFusionError::Execution(format!("Invalid XPath '{path}': {error}"))
            })
        },
        evaluate_xpath_list,
        |values| match values {
            Some(values) => {
                for value in values {
                    builder.values().append_option(value);
                }
                builder.append(true);
            }
            None => builder.append(false),
        },
    )?;
    Ok(Arc::new(builder.finish()))
}

struct BatchXpathPath<Q> {
    last_row: usize,
    query: Option<Q>,
}

pub(super) fn for_each_xpath_row<Q, T>(
    xmls: &StringArray,
    paths: &StringArray,
    compile: impl Fn(&str) -> Result<Q>,
    evaluate: impl Fn(&mut Documents, DocumentHandle, &str, &Q) -> Result<T>,
    mut append: impl FnMut(Option<T>),
) -> Result<()> {
    let paired_inputs = || {
        xmls.iter()
            .zip(paths.iter())
            .map(|(xml, path)| match (xml, path) {
                (Some(xml), Some(path)) if !xml.is_empty() && !path.is_empty() => Some((xml, path)),
                _ => None,
            })
    };
    // Retain each compiled query only until its final usable row.
    let mut batch_paths = HashMap::new();
    for (row, input) in paired_inputs().enumerate() {
        if let Some((_, path)) = input {
            batch_paths.insert(
                path,
                BatchXpathPath {
                    last_row: row,
                    query: None,
                },
            );
        }
    }

    for (row, input) in paired_inputs().enumerate() {
        let Some((xml, path)) = input else {
            append(None);
            continue;
        };
        // An invalid document takes precedence over an invalid path on the same row.
        let mut documents = Documents::new();
        let document = documents.add_string_without_uri(xml).map_err(|error| {
            DataFusionError::Execution(format!("Invalid XML document: {error}"))
        })?;
        let batch_path = batch_paths.get_mut(path).ok_or_else(|| {
            DataFusionError::Internal("XPath path missing from batch index".into())
        })?;
        let query = match &mut batch_path.query {
            Some(query) => query,
            slot @ None => slot.insert(compile(path)?),
        };
        append(Some(evaluate(&mut documents, document, path, query)?));
        if batch_path.last_row == row {
            batch_path.query = None;
        }
    }
    Ok(())
}

fn evaluate_xpath_list(
    documents: &mut Documents,
    document: DocumentHandle,
    path: &str,
    query: &SequenceQuery,
) -> Result<Vec<Option<String>>> {
    let value = query.execute(documents, document).map_err(|error| {
        DataFusionError::Execution(format!("Error loading expression '{path}': {error}"))
    })?;
    value
        .iter()
        .map(|item| match item {
            Item::Node(node) => Ok(node_value(documents.xot(), node)),
            Item::Atomic(_) | Item::Function(_) => Err(DataFusionError::Execution(format!(
                "Error loading expression '{path}': Cannot convert XPath result to a NodeList"
            ))),
        })
        .collect()
}

fn node_value(xot: &xot::Xot, node: xot::Node) -> Option<String> {
    match xot.value(node) {
        xot::Value::Document | xot::Value::Element(_) => None,
        xot::Value::Attribute(attribute) => Some(attribute.value().to_string()),
        xot::Value::Text(text) => Some(text.get().to_string()),
        xot::Value::Comment(comment) => Some(comment.get().to_string()),
        xot::Value::Namespace(namespace) => {
            Some(xot.namespace_str(namespace.namespace()).to_string())
        }
        xot::Value::ProcessingInstruction(instruction) => {
            Some(instruction.data().unwrap_or("").to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    #[test]
    fn xpath_compiles_each_valid_path_once_in_a_batch() -> Result<()> {
        let names: Vec<_> = (0..300).map(|index| format!("path_{index}")).collect();
        let paths = StringArray::from_iter(
            names
                .iter()
                .map(String::as_str)
                .cycle()
                .take(600)
                .map(Some)
                .chain([Some("unused"), Some(""), None]),
        );
        let xmls = StringArray::from_iter((0..600).map(|_| Some("<r/>")).chain([
            None,
            Some("bad XML"),
            Some("bad XML"),
        ]));
        let counts = RefCell::new(HashMap::new());
        let mut output = Vec::new();
        for_each_xpath_row(
            &xmls,
            &paths,
            |path| {
                *counts.borrow_mut().entry(path.to_owned()).or_insert(0) += 1;
                Ok(path.to_owned())
            },
            |_, _, _, path| Ok(path.clone()),
            |value| output.push(value),
        )?;

        let counts = counts.into_inner();
        assert_eq!(counts.len(), names.len());
        assert!(counts.values().all(|count| *count == 1));
        assert_eq!(&output[..300], &output[300..600]);
        assert_eq!(&output[600..], &[None, None, None]);
        Ok(())
    }
}
