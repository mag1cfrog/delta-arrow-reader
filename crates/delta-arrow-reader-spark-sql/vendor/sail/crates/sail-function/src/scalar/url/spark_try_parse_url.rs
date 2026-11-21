// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

use crate::functions_utils::make_scalar_function;
use crate::scalar::url::parse_url::{
    ParseUrl, parse_url_return_type, parse_url_with_literal_part, spark_handled_parse_url,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryParseUrl {
    signature: Signature,
}

impl Default for SparkTryParseUrl {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryParseUrl {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryParseUrl {
    fn name(&self) -> &str {
        "spark_try_parse_url"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TryParseUrl` is `RuntimeReplaceable` over `ParseUrl`, whose
    /// `nullable = true` is unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/urlExpressions.scala#L221>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = parse_url_return_type(self.name(), &arg_types)?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let parse_url: ParseUrl = ParseUrl::new();
        parse_url.coerce_types(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        parse_url_with_literal_part(&args, null_on_parse_error)
            .unwrap_or_else(|| make_scalar_function(spark_try_parse_url, vec![])(&args))
    }
}

fn spark_try_parse_url(args: &[ArrayRef]) -> Result<ArrayRef> {
    spark_handled_parse_url(args, null_on_parse_error)
}

fn null_on_parse_error(result: Result<Option<String>>) -> Result<Option<String>> {
    Ok(result.unwrap_or_default())
}
