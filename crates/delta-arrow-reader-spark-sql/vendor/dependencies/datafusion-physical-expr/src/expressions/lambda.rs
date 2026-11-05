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

//! Physical lambda expression: [`LambdaExpr`]

use std::hash::Hash;
use std::sync::Arc;

use crate::{
    ScalarFunctionExpr,
    expressions::{BinaryExpr, Column, LambdaVariable, Literal},
    higher_order_function::HigherOrderFunctionExpr,
    physical_expr::PhysicalExpr,
    utils::compact_lambda_captures,
};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{
    plan_err,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_common::{HashSet, Result, internal_err};
use datafusion_expr::ColumnarValue;

/// Represents a lambda with the given parameters names and body
#[derive(Debug, Eq, Clone)]
pub struct LambdaExpr {
    params: Vec<String>,
    body: Arc<dyn PhysicalExpr>,
    /// Body over compact captures followed by every declared parameter slot.
    projected_body: Arc<dyn PhysicalExpr>,
    /// Source input indices in compact capture order, excluding this lambda's parameters.
    /// Empty until the enclosing higher-order expression binds the input width.
    capture_indices: Vec<usize>,
    /// None is unbound; Some(0) is a known empty enclosing input schema.
    input_schema_size: Option<usize>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808 [https://github.com/apache/datafusion/issues/13196]
impl PartialEq for LambdaExpr {
    fn eq(&self, other: &Self) -> bool {
        self.params.eq(&other.params)
            && self.body.eq(&other.body)
            && self.input_schema_size.eq(&other.input_schema_size)
    }
}

impl Hash for LambdaExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.params.hash(state);
        self.body.hash(state);
        self.input_schema_size.hash(state);
    }
}

impl LambdaExpr {
    /// Creates a lambda whose enclosing input width is not yet bound.
    ///
    /// The enclosing higher-order expression binds that width before evaluation,
    /// then derives captures followed by all declared parameter slots. An empty
    /// capture list on this unbound expression is provisional, not capture-free.
    pub fn try_new(params: Vec<String>, body: Arc<dyn PhysicalExpr>) -> Result<Self> {
        if !all_unique(&params) {
            return plan_err!(
                "lambda params must be unique, got ({})",
                params.join(", ")
            );
        }

        check_async_udf(&body)?;

        Self::new(params, body, None)
    }

    fn new(
        params: Vec<String>,
        body: Arc<dyn PhysicalExpr>,
        input_schema_size: Option<usize>,
    ) -> Result<Self> {
        let Some(input_size) = input_schema_size else {
            return Ok(Self {
                params,
                projected_body: Arc::clone(&body),
                body,
                capture_indices: vec![],
                input_schema_size,
            });
        };
        let mut used_column_indices = HashSet::new();

        body.apply(|node| {
            if let Some(lambda) = node.downcast_ref::<LambdaExpr>() {
                used_column_indices.extend(
                    lambda
                        .capture_indices()
                        .iter()
                        .copied()
                        .filter(|index| *index < input_size),
                );
                return Ok(TreeNodeRecursion::Jump);
            }
            if let Some(col) = node.downcast_ref::<Column>() {
                if col.index() < input_size {
                    used_column_indices.insert(col.index());
                }
            } else if let Some(var) = node.downcast_ref::<LambdaVariable>() {
                if var.index() < input_size {
                    used_column_indices.insert(var.index());
                }
            }

            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut capture_indices = used_column_indices.into_iter().collect::<Vec<_>>();

        capture_indices.sort();

        let projected_body = compact_lambda_captures(
            Arc::clone(&body),
            input_size,
            &capture_indices,
        )?;

        Ok(Self {
            params,
            body,
            projected_body,
            capture_indices,
            input_schema_size,
        })
    }

    /// Get the lambda's params names
    pub fn params(&self) -> &[String] {
        &self.params
    }

    /// Get the lambda's body
    pub fn body(&self) -> &Arc<dyn PhysicalExpr> {
        &self.body
    }

    /// Source input indices in ascending compact column order, excluding own parameter slots.
    /// This list is provisional while `input_schema_size()` is None.
    pub(crate) fn capture_indices(&self) -> &[usize] {
        &self.capture_indices
    }

    /// Body over compact captures followed by all declared parameters.
    pub(crate) fn projected_body(&self) -> &Arc<dyn PhysicalExpr> {
        &self.projected_body
    }

    /// Complete enclosing width, or None until bound; Some(0) is a bound empty input.
    pub(crate) fn input_schema_size(&self) -> Option<usize> {
        self.input_schema_size
    }

    pub(crate) fn with_input_schema_size(&self, size: usize) -> Result<Self> {
        Self::new(self.params.clone(), Arc::clone(&self.body), Some(size))
    }

    pub(crate) fn with_new_body_and_input_schema_size(
        &self,
        body: Arc<dyn PhysicalExpr>,
        size: usize,
    ) -> Result<Self> {
        check_async_udf(&body)?;
        Self::new(self.params.clone(), body, Some(size))
    }

    /// Reuses normalization only for an enclosing lambda's sorted capture compaction.
    /// Arithmetic and higher-order bodies retain their projected body; other
    /// reconstruction kinds derive it again.
    pub(crate) fn with_compacted_outer_captures(
        &self,
        body: Arc<dyn PhysicalExpr>,
        input_schema_size: usize,
        capture_indices: Vec<usize>,
    ) -> Result<Self> {
        check_async_udf(&body)?;
        // Other reconstruction kinds may change more than input indices.
        let can_reuse_projected_body = !self.body.exists(|node| {
            Ok(!(node.is::<Column>()
                || node.is::<LambdaVariable>()
                || node.is::<Literal>()
                || node.is::<BinaryExpr>()
                || node.is::<HigherOrderFunctionExpr>()
                || node.is::<LambdaExpr>()))
        })?;
        if !can_reuse_projected_body {
            return Self::new(self.params.clone(), body, Some(input_schema_size));
        }
        if capture_indices.len() != self.capture_indices.len()
            || capture_indices.windows(2).any(|pair| pair[0] >= pair[1])
            || capture_indices.last().is_some_and(|index| *index >= input_schema_size)
        {
            return internal_err!("lambda capture compaction changed capture order or bounds");
        }
        Ok(Self {
            params: self.params.clone(),
            body,
            projected_body: Arc::clone(&self.projected_body),
            capture_indices,
            input_schema_size: Some(input_schema_size),
        })
    }

}

impl std::fmt::Display for LambdaExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}) -> {}", self.params.join(", "), self.body)
    }
}

impl PhysicalExpr for LambdaExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        internal_err!("LambdaExpr::evaluate() should not be called")
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.body]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [body] = children.as_slice() else {
            return internal_err!(
                "LambdaExpr expects exactly 1 child, got {}",
                children.len()
            );
        };

        check_async_udf(body)?;

        Ok(Arc::new(Self::new(
            self.params.clone(),
            Arc::clone(body),
            self.input_schema_size,
        )?))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}) -> {}", self.params.join(", "), self.body)
    }
}

/// Create a lambda expression
pub fn lambda(
    params: impl IntoIterator<Item = impl Into<String>>,
    body: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(LambdaExpr::try_new(
        params.into_iter().map(Into::into).collect(),
        body,
    )?))
}

fn all_unique(params: &[String]) -> bool {
    match params.len() {
        0 | 1 => true,
        2 => params[0] != params[1],
        _ => {
            let mut set = HashSet::with_capacity(params.len());

            params.iter().all(|p| set.insert(p.as_str()))
        }
    }
}

fn check_async_udf(body: &Arc<dyn PhysicalExpr>) -> Result<()> {
    if body.exists(|expr| {
        Ok(expr
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|udf| udf.fun().as_async().is_some()))
    })? {
        return plan_err!(
            "Async functions in lambdas aren't supported, see https://github.com/apache/datafusion/issues/22091"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::expressions::{NoOp, lambda::lambda};
    use arrow::{array::RecordBatch, datatypes::Schema};
    use std::sync::Arc;

    #[test]
    fn test_lambda_evaluate() {
        let lambda = lambda(["a"], Arc::new(NoOp::new())).unwrap();
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(lambda.evaluate(&batch).is_err());
    }

    #[test]
    fn test_lambda_duplicate_name() {
        assert!(lambda(["a", "a"], Arc::new(NoOp::new())).is_err());
    }
}
