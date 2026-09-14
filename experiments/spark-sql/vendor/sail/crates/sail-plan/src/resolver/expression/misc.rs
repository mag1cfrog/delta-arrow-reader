// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::expr::FieldMetadata;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, when};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions::expr_fn as datafusion_fn;
use datafusion_functions_nested::expr_fn::{array_element, array_length, map_extract};
use sail_common::spec::{self, DEFAULT_COLUMN_VALUE_PLACEHOLDER_ID};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::table_input::TableInput;

use crate::error::{PlanError, PlanResult};
use crate::formatter::SparkPlanFormatter;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_alias(
        &self,
        expr: spec::Expr,
        name: Vec<spec::Identifier>,
        metadata: Option<Vec<(String, String)>>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let name = name.into_iter().map(|x| x.into()).collect::<Vec<String>>();
        let named_expr = self.resolve_named_expression(expr, schema, state).await?;
        let NamedExpr {
            name: inner_name,
            expr,
            metadata: inner_metadata,
        } = named_expr;
        if name.is_empty() {
            return Ok(
                NamedExpr::new(inner_name, expr).with_metadata(metadata.unwrap_or(inner_metadata))
            );
        }
        let metadata = metadata.unwrap_or(inner_metadata);
        let expr = if let [n] = name.as_slice() {
            if !metadata.is_empty() {
                let metadata_map: HashMap<String, String> = metadata.into_iter().collect();
                let field_metadata = Some(FieldMetadata::from(metadata_map));
                expr.alias_with_metadata(n, field_metadata)
            } else {
                expr.alias(n)
            }
        } else {
            expr
        };
        Ok(NamedExpr::new(name, expr))
    }

    pub(super) async fn resolve_expression_placeholder(
        &self,
        placeholder: String,
    ) -> PlanResult<NamedExpr> {
        let name = placeholder.clone();
        let expr = expr::Expr::Placeholder(expr::Placeholder::new_with_field(placeholder, None));
        Ok(NamedExpr::new(vec![name], expr))
    }

    pub(super) fn resolve_expression_default_column_value(&self) -> PlanResult<NamedExpr> {
        let expr = expr::Expr::Placeholder(expr::Placeholder::new_with_field(
            DEFAULT_COLUMN_VALUE_PLACEHOLDER_ID.to_string(),
            None,
        ));
        Ok(NamedExpr::new(vec!["DEFAULT".to_string()], expr))
    }

    pub(super) async fn resolve_expression_identifier_clause(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let resolved = self.resolve_expression(expr, schema, state).await?;
        let name = self.evaluate_identifier_expr(resolved)?;
        let object_name = sail_sql_analyzer::expression::from_ast_object_name(
            sail_sql_analyzer::parser::parse_object_name(&name)?,
        )?;
        self.resolve_expression_attribute(object_name, false, schema, state)
    }

    /// Evaluates a resolved DataFusion expression as an identifier string.
    ///
    pub(in super::super) fn evaluate_identifier_expr(
        &self,
        expr: expr::Expr,
    ) -> PlanResult<String> {
        let evaluator = LiteralEvaluator::new();
        let scalar = evaluator.evaluate(&expr).map_err(|e| {
            PlanError::invalid(format!("IDENTIFIER expression must be a constant: {e}"))
        })?;
        match scalar {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => Ok(s),
            _ => Err(PlanError::invalid(
                "IDENTIFIER expression must evaluate to a string",
            )),
        }
    }

    pub(super) async fn resolve_expression_table(
        &self,
        expr: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let query = match expr {
            spec::Expr::ScalarSubquery { subquery } => *subquery,
            spec::Expr::UnresolvedAttribute {
                name,
                plan_id: None,
                is_metadata_column: false,
            } => spec::QueryPlan::new(spec::QueryNode::Read {
                read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                    name,
                    temporal: None,
                    sample: None,
                    options: vec![],
                })),
                is_streaming: false,
            }),
            _ => {
                return Err(PlanError::invalid(
                    "expected a query or a table reference for table input",
                ));
            }
        };
        let plan = self.resolve_query_plan(query, state).await?;
        Ok(NamedExpr::new(
            vec!["table".to_string()],
            ScalarUDF::from(TableInput::new(Arc::new(plan))).call(vec![]),
        ))
    }

    pub(super) async fn resolve_expression_extract_value(
        &self,
        child: spec::Expr,
        extraction: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let NamedExpr { name, expr, .. } =
            self.resolve_named_expression(child, schema, state).await?;
        let data_type = expr.get_type(schema)?;

        // For Maps, we support non-literal expressions as keys
        if matches!(data_type, DataType::Map(_, _)) {
            let NamedExpr {
                name: extraction_name,
                expr: extraction_expr,
                ..
            } = self
                .resolve_named_expression(extraction, schema, state)
                .await?;

            let result_name = format!("{}[{}]", name.one()?, extraction_name.one()?);
            // Use map_extract which supports dynamic keys, then extract first element
            let result_expr = array_element(map_extract(expr, extraction_expr), lit(1));
            return Ok(NamedExpr::new(vec![result_name], result_expr));
        }

        // For other types (List, Struct), extraction must be a literal.
        // An UnresolvedAttribute from dot notation (e.g. `a.b`) is treated as a
        // literal field name so that the spec can keep the attribute unresolved.
        let extraction = match extraction {
            spec::Expr::Literal(lit) => lit,
            spec::Expr::UnresolvedAttribute { name, .. } => {
                let name: Vec<String> = name.into();
                spec::Literal::Utf8 {
                    value: Some(name.one()?),
                }
            }
            _ => return Err(PlanError::invalid("extraction must be a literal")),
        };
        let extraction = self.resolve_literal(extraction, state)?;

        let extraction_name =
            SparkPlanFormatter.literal_to_string(&extraction, &self.config.session_timezone)?;
        let name = match data_type {
            DataType::Struct(_) => {
                format!("{}.{}", name.one()?, extraction_name)
            }
            _ => {
                format!("{}[{}]", name.one()?, extraction_name)
            }
        };
        let expr = match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => {
                let ScalarValue::Int64(index) = extraction.cast_to(&DataType::Int64)? else {
                    return Err(PlanError::AnalysisError(format!(
                        "invalid extraction value for array: {extraction}"
                    )));
                };
                let index_expr = lit(ScalarValue::Int64(index));
                let element = array_element(
                    expr.clone(),
                    lit(ScalarValue::Int64(index.map(|x| x.saturating_add(1)))),
                );
                if self.config.ansi_mode {
                    let length = cast(array_length(expr.clone()), DataType::Int64);
                    let message = datafusion_fn::concat(vec![
                        lit("[INVALID_ARRAY_INDEX] The index "),
                        cast(index_expr, DataType::Utf8),
                        lit(" is out of bounds. The array has "),
                        cast(length.clone(), DataType::Utf8),
                        lit(
                            " elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead.",
                        ),
                    ]);
                    let error = cast(
                        ScalarUDF::from(RaiseError::new()).call(vec![message]),
                        field.data_type().clone(),
                    );
                    let out_of_bounds = match index {
                        Some(index) if index < 0 => expr.is_not_null(),
                        Some(index) => expr.is_not_null().and(length.lt_eq(lit(index))),
                        None => lit(false),
                    };
                    when(out_of_bounds, error).otherwise(element)?
                } else {
                    element
                }
            }
            DataType::Struct(fields) => {
                let ScalarValue::Utf8(Some(name)) = extraction else {
                    return Err(PlanError::AnalysisError(format!(
                        "invalid extraction value for struct: {extraction}"
                    )));
                };
                let Ok(name) = fields
                    .iter()
                    .filter(|x| x.name().eq_ignore_ascii_case(&name))
                    .map(|x| x.name().to_string())
                    .collect::<Vec<_>>()
                    .one()
                else {
                    return Err(PlanError::AnalysisError(format!(
                        "missing or ambiguous field: {name}"
                    )));
                };
                expr.field(name)
            }
            _ => {
                return Err(PlanError::AnalysisError(format!(
                    "cannot extract value from data type: {data_type}"
                )));
            }
        };
        Ok(NamedExpr::new(vec![name], expr))
    }
}
