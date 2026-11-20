// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use xee_xpath::query::Convert;
use xee_xpath::{Documents, Item, Queries, Query};

use super::xpath::for_each_xpath_row;
use crate::functions_utils::make_scalar_function;

/// The kind of typed XPath function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum XpathTypedKind {
    Boolean,
    Double,
    Float,
    Int,
    Long,
    Number,
    Short,
    String,
}

impl XpathTypedKind {
    fn name(self) -> &'static str {
        match self {
            Self::Boolean => "xpath_boolean",
            Self::Double => "xpath_double",
            Self::Float => "xpath_float",
            Self::Int => "xpath_int",
            Self::Long => "xpath_long",
            Self::Number => "xpath_number",
            Self::Short => "xpath_short",
            Self::String => "xpath_string",
        }
    }
    fn return_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Double | Self::Number => DataType::Float64,
            Self::Float => DataType::Float32,
            Self::Int => DataType::Int32,
            Self::Long => DataType::Int64,
            Self::Short => DataType::Int16,
            Self::String => DataType::Utf8,
        }
    }
}

/// A scalar UDF that evaluates a typed XPath expression against an XML document.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct XpathTyped {
    kind: XpathTypedKind,
    signature: Signature,
}

impl XpathTyped {
    pub fn new(kind: XpathTypedKind) -> Self {
        Self {
            kind,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for XpathTyped {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `XPathExtract.nullable = true`, unconditional; every typed variant inherits it
    /// and declares no `nullable` of its own.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/xml/xpath.scala#L40>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            self.kind.return_type(),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let name = self.kind.name();
        if arg_types.len() != 2 {
            return plan_err!(
                "`{name}` function requires 2 arguments, got {}",
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
                    plan_err!("The `{name}` function can only accept strings, but got {other:?}.")
                }
            })
            .collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let kind = self.kind;
        make_scalar_function(move |a| xpath_typed_inner(kind, a), vec![])(&args)
    }
}

fn xpath_typed_inner(kind: XpathTypedKind, args: &[ArrayRef]) -> Result<ArrayRef> {
    let name = kind.name();
    let [xmls, paths] = take_function_args(name, args)?;
    let xmls = xmls.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Internal(format!("{name} expected a string array for xml"))
    })?;
    let paths = paths
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{name} expected a string array for path"))
        })?;
    match kind {
        XpathTypedKind::Boolean => build_boolean_array(xmls, paths),
        XpathTypedKind::Double | XpathTypedKind::Number => build_float64_array(xmls, paths),
        XpathTypedKind::Float => build_float32_array(xmls, paths),
        XpathTypedKind::Int => build_int32_array(xmls, paths),
        XpathTypedKind::Long => build_int64_array(xmls, paths),
        XpathTypedKind::Short => build_int16_array(xmls, paths),
        XpathTypedKind::String => build_string_array(xmls, paths),
    }
}

fn for_each_typed_xpath_row<T>(
    xmls: &StringArray,
    paths: &StringArray,
    xpath_function: &str,
    convert_item: impl Convert<T>,
    append: impl FnMut(Option<T>),
) -> Result<()> {
    for_each_xpath_row(
        xmls,
        paths,
        |path| {
            let wrapped = format!("{xpath_function}({path})");
            Queries::default()
                .one(&wrapped, &convert_item)
                .map_err(|e| DataFusionError::Execution(format!("Invalid XPath '{path}': {e}")))
        },
        |documents, document, path, query| {
            query.execute(documents, document).map_err(|e| {
                DataFusionError::Execution(format!("Error evaluating XPath '{path}': {e}"))
            })
        },
        append,
    )
}

fn build_boolean_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::new();
    for_each_typed_xpath_row(
        xmls,
        paths,
        "boolean",
        |_: &mut Documents, item: &Item| Ok(item.try_into_value::<bool>()?),
        |value| builder.append_option(value),
    )?;
    Ok(Arc::new(builder.finish()))
}

fn build_float64_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Float64Builder::new();
    for_each_numeric_xpath_row(xmls, paths, |value| builder.append_option(value))?;
    Ok(Arc::new(builder.finish()))
}

fn build_float32_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Float32Builder::new();
    for_each_numeric_xpath_row(xmls, paths, |value| {
        builder.append_option(value.map(|n| n as f32))
    })?;
    Ok(Arc::new(builder.finish()))
}

fn build_int32_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Int32Builder::new();
    for_each_numeric_xpath_row(xmls, paths, |value| {
        builder.append_option(value.map(|n| n as i32))
    })?;
    Ok(Arc::new(builder.finish()))
}

fn build_int64_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Int64Builder::new();
    for_each_numeric_xpath_row(xmls, paths, |value| {
        builder.append_option(value.map(|n| n as i64))
    })?;
    Ok(Arc::new(builder.finish()))
}

fn build_int16_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Int16Builder::new();
    for_each_numeric_xpath_row(xmls, paths, |value| {
        builder.append_option(value.map(|n| n as i16))
    })?;
    Ok(Arc::new(builder.finish()))
}

fn for_each_numeric_xpath_row(
    xmls: &StringArray,
    paths: &StringArray,
    append: impl FnMut(Option<f64>),
) -> Result<()> {
    for_each_typed_xpath_row(
        xmls,
        paths,
        "number",
        |_: &mut Documents, item: &Item| Ok(item.try_into_value::<f64>()?),
        append,
    )
}

fn build_string_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = StringBuilder::new();
    for_each_typed_xpath_row(
        xmls,
        paths,
        "string",
        |_: &mut Documents, item: &Item| Ok(item.try_into_value::<String>()?),
        |value| builder.append_option(value),
    )?;
    Ok(Arc::new(builder.finish()))
}
