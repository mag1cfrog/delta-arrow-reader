// Modified for the DAR Spark SQL runtime. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err, plan_err};
use datafusion_expr::binary::try_type_union_resolution;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use itertools::Itertools;

#[user_doc(
    doc_section(label = "Conditional Functions"),
    description = "Returns the first of its arguments that is not _null_. Returns _null_ if all arguments are _null_. This function is often used to substitute a default value for _null_ values.",
    syntax_example = "coalesce(expression1[, ..., expression_n])",
    sql_example = r#"```sql
> select coalesce(null, null, 'datafusion');
+----------------------------------------+
| coalesce(NULL,NULL,Utf8("datafusion")) |
+----------------------------------------+
| datafusion                             |
+----------------------------------------+
```"#,
    argument(
        name = "expression1, expression_n",
        description = "Expression to use if previous expressions are _null_. Can be a constant, column, or function, and any combination of arithmetic operators. Pass as many expression arguments as necessary."
    )
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CoalesceFunc {
    pub(super) signature: Signature,
}

impl Default for CoalesceFunc {
    fn default() -> Self {
        CoalesceFunc::new()
    }
}

impl CoalesceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CoalesceFunc {
    fn name(&self) -> &str {
        "coalesce"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // If any the arguments in coalesce is non-null, the result is non-null
        let nullable = args.arg_fields.iter().all(|f| f.is_nullable());
        let return_type = args
            .arg_fields
            .iter()
            .map(|f| f.data_type())
            .find_or_first(|d| !d.is_null())
            .unwrap()
            .clone();
        Ok(Field::new(self.name(), return_type, nullable).into())
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args.is_empty() {
            return plan_err!("coalesce must have at least one argument");
        }
        if args.len() == 1 {
            return Ok(ExprSimplifyResult::Simplified(
                args.into_iter().next().unwrap(),
            ));
        }

        let first = args
            .iter()
            .position(|arg| !matches!(arg, Expr::Literal(value, _) if value.is_null()))
            .unwrap_or(args.len() - 1);
        let mut end = args.len();
        for (index, arg) in args.iter().enumerate().skip(first) {
            if !info.nullable(arg)? {
                end = index + 1;
                break;
            }
        }
        if first == 0 && end == args.len() {
            return Ok(ExprSimplifyResult::Original(args));
        }
        // Discarded arguments still determine the SQL result type.
        let original = ScalarUDF::from(self.clone()).call(args.clone());
        let result_type = info.get_data_type(&original)?;
        let args = args
            .into_iter()
            .skip(first)
            .take(end - first)
            .map(|arg| arg.cast_to(&result_type, info.schema().as_ref()))
            .collect::<Result<Vec<_>>>()?;
        if args.len() == 1 {
            Ok(ExprSimplifyResult::Simplified(
                args.into_iter().next().unwrap(),
            ))
        } else {
            Ok(ExprSimplifyResult::Original(args))
        }
    }

    /// coalesce evaluates to the first value which is not NULL
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("coalesce requires conditional physical evaluation")
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        let eager = vec![&args[0]];
        let lazy = args[1..].iter().collect();
        Some((eager, lazy))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn returns_first_non_null_argument(&self) -> bool {
        true
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return exec_err!("coalesce must have at least one argument");
        }

        try_type_union_resolution(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
