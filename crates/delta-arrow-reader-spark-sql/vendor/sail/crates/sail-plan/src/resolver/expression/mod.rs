// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::fmt::Debug;

use async_recursion::async_recursion;
use datafusion_common::DFSchemaRef;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::expr;
use datafusion_functions::core::getfield::GetFieldFunc;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::explode::Explode;
use sail_function::scalar::multi_expr::MultiExpr;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{drop_inherited_column_metadata, set_result_metadata};
use crate::resolver::PlanResolver;
use crate::resolver::interval_qualifier::{IntervalOperand, IntervalQualifier};
use crate::resolver::state::PlanResolverState;

mod attribute;
mod cast;
mod day_time_interval;
mod function;
mod grouping;
mod lambda;
mod literal;
mod misc;
mod predicate;
mod sort;
mod subquery;
mod wildcard;
mod window;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct NamedExpr {
    /// The name of the expression to be used in projection.
    /// The name can be empty if the expression is not supposed to exist in the resolved
    /// projection (a wildcard expression, a sort expression, etc.).
    /// A list of names may be present for multi-expression (a temporary expression
    /// to be expanded into multiple ones in the projection).
    pub name: Vec<String>,
    /// Display inside another expression, when it differs from the projection name.
    pub expression_name: Option<String>,
    pub expr: expr::Expr,
    pub metadata: Vec<(String, String)>,
    /// The Spark qualifier of an interval value, where the resolver knows it (#1152).
    pub interval_qualifier: Option<IntervalQualifier>,
}

impl NamedExpr {
    pub fn new(name: Vec<String>, expr: expr::Expr) -> Self {
        let metadata = match &expr {
            expr::Expr::Alias(alias) => alias
                .metadata
                .as_ref()
                .map(|x| {
                    x.inner()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect()
                })
                .unwrap_or(vec![]),
            expr::Expr::Literal(_literal, field_metadata) => field_metadata
                .as_ref()
                .map(|x| {
                    x.inner()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect()
                })
                .unwrap_or(vec![]),
            _ => vec![],
        };
        Self {
            name,
            expression_name: None,
            expr,
            metadata,
            interval_qualifier: None,
        }
    }

    pub fn try_from_alias_expr(expr: expr::Expr) -> PlanResult<Self> {
        match expr {
            expr::Expr::Alias(alias) => Ok(Self {
                name: vec![alias.name],
                expression_name: None,
                expr: *alias.expr,
                metadata: alias
                    .metadata
                    .map(|x| x.to_hashmap().into_iter().collect())
                    .unwrap_or(vec![]),
                interval_qualifier: None,
            }),
            _ => Err(PlanError::invalid(
                "alias expected to create named expression",
            )),
        }
    }

    pub fn into_alias(self, field_id: String) -> expr::Expr {
        let expression = if self.metadata.is_empty() {
            self.expr
        } else {
            set_result_metadata(self.expr, self.metadata)
        };
        expression.alias(field_id)
    }

    pub fn with_metadata(mut self, metadata: Vec<(String, String)>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_expression_name(mut self, name: String) -> Self {
        self.expression_name = Some(name);
        self
    }

    pub fn with_interval_qualifier(mut self, qualifier: Option<IntervalQualifier>) -> Self {
        self.interval_qualifier = qualifier;
        self
    }

    pub fn expression_name(&self) -> PlanResult<String> {
        match &self.expression_name {
            Some(name) => Ok(name.clone()),
            None => Ok(self.name.clone().one()?),
        }
    }
}

/// The qualifier an interval literal writes, as `INTERVAL '3' DAY`.
fn literal_interval_qualifier(literal: &spec::Literal) -> Option<IntervalQualifier> {
    match literal {
        spec::Literal::DurationMicrosecond {
            start_field,
            end_field,
            ..
        }
        | spec::Literal::IntervalYearMonth {
            start_field,
            end_field,
            ..
        } => IntervalQualifier::new(*start_field, *end_field),
        _ => None,
    }
}

/// The qualifier recorded for the field a resolved scalar subquery returns.
fn scalar_subquery_interval_qualifier(
    expr: &expr::Expr,
    state: &PlanResolverState,
) -> Option<IntervalQualifier> {
    let mut field = None;
    expr.apply(|expr| {
        Ok(match expr {
            expr::Expr::ScalarSubquery(subquery) => {
                field = subquery
                    .subquery
                    .schema()
                    .fields()
                    .first()
                    .map(|field| field.name().clone());
                TreeNodeRecursion::Stop
            }
            _ => TreeNodeRecursion::Continue,
        })
    })
    .ok()?;
    state.interval_qualifier(&field?)
}

/// The qualifier recorded for the field a resolved column reads.
fn column_interval_qualifier(
    expr: &expr::Expr,
    state: &PlanResolverState,
) -> Option<IntervalQualifier> {
    let mut expr = expr;
    while let expr::Expr::Alias(alias) = expr {
        expr = alias.expr.as_ref();
    }
    match expr {
        expr::Expr::Column(column) | expr::Expr::OuterReferenceColumn(_, column) => {
            state.interval_qualifier(&column.name)
        }
        _ => None,
    }
}

fn is_extraction_only(expression: &spec::Expr) -> bool {
    match expression {
        spec::Expr::UnresolvedAttribute { .. }
        | spec::Expr::IdentifierClause { .. }
        | spec::Expr::Literal(_) => true,
        spec::Expr::UnresolvedExtractValue { child, extraction } => {
            is_extraction_only(child) && is_extraction_only(extraction)
        }
        _ => false,
    }
}

fn needs_projection_expansion(expression: &expr::Expr) -> bool {
    matches!(expression, expr::Expr::ScalarFunction(function)
        if function.func.inner().is::<Explode>() || function.func.inner().is::<MultiExpr>())
}

fn inherits_alias_metadata(expression: &expr::Expr) -> bool {
    match expression {
        expr::Expr::Alias(_)
        | expr::Expr::Column(_)
        | expr::Expr::OuterReferenceColumn(..)
        | expr::Expr::Literal(..) => true,
        expr::Expr::ScalarFunction(function) => function.func.inner().is::<GetFieldFunc>(),
        _ => false,
    }
}

impl PlanResolver<'_> {
    #[async_recursion]
    /// Resolves a Sail spec expression into a named expression.
    ///
    /// Dispatches to type-specific resolvers based on the expression variant (e.g., Cast, Literal,
    /// Function). Returns a `NamedExpr` containing the resolved expression and display name(s)
    /// for projection/aliasing.
    pub(super) async fn resolve_named_expression(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        use spec::Expr;

        match expr {
            Expr::UnresolvedAttribute {
                plan_id: Some(_), ..
            }
            | Expr::UnresolvedStar {
                plan_id: Some(_), ..
            } => Err(PlanError::unsupported("DataFrame plan IDs")),
            Expr::Literal(literal) => {
                let qualifier = literal_interval_qualifier(&literal);
                self.resolve_expression_literal(literal, state)
                    .map(|named| named.with_interval_qualifier(qualifier))
            }
            Expr::UnresolvedAttribute {
                name,
                plan_id: None,
                is_metadata_column,
            } => self
                .resolve_expression_attribute(name, is_metadata_column, schema, state)
                .map(|named| {
                    let qualifier = column_interval_qualifier(&named.expr, state);
                    named.with_interval_qualifier(qualifier)
                }),
            // Outside the call that reads it as a unit, a unit keyword is a column name, as Spark
            // reads a bare word there.
            Expr::DatetimeUnitKeyword(unit) => self.resolve_expression_attribute(
                spec::ObjectName::bare(unit),
                false,
                schema,
                state,
            ),
            Expr::UnresolvedFunction(function) => {
                self.resolve_expression_function(function, schema, state)
                    .await
            }
            Expr::UnresolvedStar {
                target,
                plan_id: None,
                wildcard_options,
            } => {
                self.resolve_expression_wildcard(target, wildcard_options, schema, state)
                    .await
            }
            Expr::Alias {
                expr,
                name,
                metadata,
            } => {
                self.resolve_expression_alias(*expr, name, metadata, schema, state)
                    .await
            }
            Expr::Cast {
                expr,
                cast_to_type,
                rename: _,
                is_try,
            } => self
                .resolve_expression_cast(*expr, cast_to_type, is_try, schema, state)
                .await
                .map(|(named, _)| named),
            Expr::UnresolvedRegex { .. } | Expr::UpdateFields { .. } => {
                Err(PlanError::unsupported("DataFrame column expressions"))
            }
            Expr::SortOrder(sort) => {
                self.resolve_expression_sort_order(sort, schema, state)
                    .await
            }
            Expr::LambdaFunction {
                function,
                arguments,
            } => {
                self.resolve_expression_lambda_function(*function, arguments, None, schema, state)
                    .await
            }
            Expr::Window {
                window_function,
                window,
            } => {
                self.resolve_expression_window(*window_function, window, schema, state)
                    .await
            }
            Expr::UnresolvedExtractValue { child, extraction } => {
                self.resolve_expression_extract_value(*child, *extraction, schema, state)
                    .await
            }
            Expr::UnresolvedNamedLambdaVariable(variable) => {
                self.resolve_expression_named_lambda_variable(variable, state)
                    .await
            }
            Expr::CommonInlineUserDefinedFunction { .. } => {
                Err(PlanError::unsupported("inline user-defined functions"))
            }
            Expr::CallFunction {
                function_name,
                arguments,
            } => {
                self.resolve_expression_call_function(function_name, arguments, schema, state)
                    .await
            }
            Expr::DefaultColumnValue => self.resolve_expression_default_column_value(),
            Expr::Placeholder(placeholder) => {
                self.resolve_expression_placeholder(placeholder).await
            }
            Expr::Rollup(rollup) => self.resolve_expression_rollup(rollup, schema, state).await,
            Expr::Cube(cube) => self.resolve_expression_cube(cube, schema, state).await,
            Expr::GroupingSets(grouping_sets) => {
                self.resolve_expression_grouping_sets(grouping_sets, schema, state)
                    .await
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                // Detect multi-column IN subquery: (a, b) IN (SELECT x, y FROM ...)
                // The SQL parser produces a Tuple which the analyzer converts to
                // UnresolvedFunction("struct", [a, b]).
                if let Expr::UnresolvedFunction(ref f) = *expr
                    && f.function_name.parts() == [spec::Identifier::from("struct")]
                    && f.arguments.len() > 1
                {
                    let arguments = match *expr {
                        Expr::UnresolvedFunction(f) => f.arguments,
                        _ => unreachable!(),
                    };
                    return self
                        .resolve_multi_column_in_subquery(
                            arguments, *subquery, negated, schema, state,
                        )
                        .await;
                }
                self.resolve_expression_in_subquery(*expr, *subquery, negated, schema, state)
                    .await
            }
            Expr::ScalarSubquery { subquery } => self
                .resolve_expression_scalar_subquery(*subquery, schema, state)
                .await
                .map(|named| {
                    let qualifier = scalar_subquery_interval_qualifier(&named.expr, state);
                    named.with_interval_qualifier(qualifier)
                }),
            Expr::Exists { subquery, negated } => {
                self.resolve_expression_exists(*subquery, negated, schema, state)
                    .await
            }
            Expr::Subquery { .. } => {
                Err(PlanError::unsupported("Spark Connect subquery references"))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                self.resolve_expression_in_list(*expr, list, negated, schema, state)
                    .await
            }
            Expr::IsFalse(expr) => self.resolve_expression_is_false(*expr, schema, state).await,
            Expr::IsNotFalse(expr) => {
                self.resolve_expression_is_not_false(*expr, schema, state)
                    .await
            }
            Expr::IsTrue(expr) => self.resolve_expression_is_true(*expr, schema, state).await,
            Expr::IsNotTrue(expr) => {
                self.resolve_expression_is_not_true(*expr, schema, state)
                    .await
            }
            Expr::IsNull(expr) => self.resolve_expression_is_null(*expr, schema, state).await,
            Expr::IsNotNull(expr) => {
                self.resolve_expression_is_not_null(*expr, schema, state)
                    .await
            }
            Expr::IsUnknown(expr) => {
                self.resolve_expression_is_unknown(*expr, schema, state)
                    .await
            }
            Expr::IsNotUnknown(expr) => {
                self.resolve_expression_is_not_unknown(*expr, schema, state)
                    .await
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                self.resolve_expression_between(*expr, negated, *low, *high, schema, state)
                    .await
            }
            Expr::IsDistinctFrom { left, right } => {
                self.resolve_expression_is_distinct_from(*left, *right, schema, state)
                    .await
            }
            Expr::IsNotDistinctFrom { left, right } => {
                self.resolve_expression_is_not_distinct_from(*left, *right, schema, state)
                    .await
            }
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            } => {
                self.resolve_expression_similar_to(
                    *expr,
                    *pattern,
                    negated,
                    escape_char,
                    case_insensitive,
                    schema,
                    state,
                )
                .await
            }
            Expr::Table { expr } => self.resolve_expression_table(*expr, state).await,
            Expr::UnresolvedDate { value } => self.resolve_expression_date(value, state),
            Expr::UnresolvedTimestamp {
                value,
                timestamp_type,
            } => self.resolve_expression_timestamp(value, timestamp_type, state),
            Expr::UnresolvedTime { value } => self.resolve_expression_time(value, state),
            Expr::IdentifierClause { expr } => {
                self.resolve_expression_identifier_clause(*expr, schema, state)
                    .await
            }
            // NamedArgument should only appear inside UDF argument lists, not standalone
            Expr::NamedArgument { .. } => Err(PlanError::invalid(
                "named argument expression can only be used in UDF arguments",
            )),
        }
    }

    /// A direct column CAST keeps its column name when used as an output.
    pub(super) async fn resolve_named_output_expressions(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().include_cast_in_expression_name = false;
        let mut results: Vec<NamedExpr> = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let named_expr = self
                .resolve_named_expression(expression, schema, state)
                .await?;
            results.push(named_expr);
        }
        Ok(results)
    }

    pub(super) async fn resolve_projection_expressions(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().include_cast_in_expression_name = false;
        let mut results = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let extraction_only = is_extraction_only(&expression);
            let named_source = matches!(
                &expression,
                spec::Expr::Literal(_)
                    | spec::Expr::UnresolvedAttribute { .. }
                    | spec::Expr::Alias { .. }
                    | spec::Expr::UnresolvedStar { .. }
                    | spec::Expr::IdentifierClause { .. }
                    | spec::Expr::UnresolvedDate { .. }
                    | spec::Expr::UnresolvedTime { .. }
                    | spec::Expr::UnresolvedTimestamp { .. }
            );
            let (mut named, direct_named_cast) = match expression {
                spec::Expr::Cast {
                    expr,
                    cast_to_type,
                    is_try,
                    ..
                } => {
                    self.resolve_expression_cast(*expr, cast_to_type, is_try, schema, state)
                        .await?
                }
                expression => (
                    self.resolve_named_expression(expression, schema, state)
                        .await?,
                    false,
                ),
            };
            if !named_source && !needs_projection_expansion(&named.expr) {
                if !direct_named_cast && !extraction_only {
                    // Output metadata must not participate in grouping expression identity.
                    named.metadata.retain(|(key, _)| {
                        matches!(
                            key.as_str(),
                            "ARROW:extension:name" | "ARROW:extension:metadata"
                        )
                    });
                    named
                        .metadata
                        .push(("__autoGeneratedAlias".into(), "true".into()));
                } else if !inherits_alias_metadata(&named.expr) {
                    named.expr = drop_inherited_column_metadata(named.expr);
                }
            }
            results.push(named);
        }
        Ok(results)
    }

    pub(super) async fn resolve_expression(
        &self,
        expressions: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Expr> {
        let NamedExpr { expr, .. } = self
            .resolve_named_expression(expressions, schema, state)
            .await?;
        Ok(expr)
    }

    pub(super) async fn resolve_expressions(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<expr::Expr>> {
        let mut results: Vec<expr::Expr> = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let expr = self.resolve_expression(expression, schema, state).await?;
            results.push(expr);
        }
        Ok(results)
    }

    /// Nested CASTs remain visible in the enclosing expression's display name.
    pub(super) async fn resolve_expression_arguments(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Vec<expr::Expr>)> {
        let (names, exprs, _) = self
            .resolve_expression_arguments_with_qualifiers(expressions, schema, state)
            .await?;
        Ok((names, exprs))
    }

    /// [`Self::resolve_expression_arguments`], with each argument's interval qualifier.
    pub(super) async fn resolve_expression_arguments_with_qualifiers(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Vec<expr::Expr>, Vec<IntervalOperand>)> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().include_cast_in_expression_name = true;
        let mut names: Vec<String> = Vec::with_capacity(expressions.len());
        let mut exprs: Vec<expr::Expr> = Vec::with_capacity(expressions.len());
        let mut operands: Vec<IntervalOperand> = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let named = self
                .resolve_named_expression(expression, schema, state)
                .await?;
            names.push(named.expression_name()?);
            operands.push(IntervalOperand::of(
                &named.expr,
                named.interval_qualifier,
                schema,
            ));
            exprs.push(named.expr);
        }
        Ok((names, exprs, operands))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr::expr::{Alias, Expr};
    use datafusion_expr::{BinaryExpr, Operator};
    use sail_common::geoarrow::extension::GeoArrowWkbType;
    use sail_common::spec;

    use crate::config::PlanConfig;
    use crate::error::PlanResult;
    use crate::resolver::PlanResolver;
    use crate::resolver::expression::NamedExpr;
    use crate::resolver::state::PlanResolverState;

    #[tokio::test]
    async fn supplied_alias_metadata_survives_projection_and_aggregate_planning() -> PlanResult<()>
    {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use arrow::util::display::array_value_to_string;

        use crate::expression_errors::{PrepareSparkExpressions, SparkSqlExpressionSemantics};
        use crate::physical_plan::SparkQueryPlanner;
        use datafusion::dataframe::DataFrame;
        use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;

        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
                .build(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true)
                .with_metadata(HashMap::from([("source".into(), "provider".into())])),
        ]));
        ctx.register_batch(
            "metadata_inputs",
            RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![Some(2), None, Some(3)]))],
            )?,
        )?;
        for ansi_mode in [true, false] {
            let resolver = PlanResolver::new(
                &ctx,
                Arc::new(PlanConfig {
                    ansi_mode,
                    ..PlanConfig::default()
                }),
            );
            for (sql, unaliased_name, expected_values) in [
                (
                    "SELECT k AS value FROM metadata_inputs",
                    "k",
                    vec!["", "2", "3"],
                ),
                (
                    "SELECT CAST(k AS BIGINT) AS value FROM metadata_inputs",
                    "k",
                    vec!["", "2", "3"],
                ),
                (
                    "SELECT CASE WHEN true THEN k ELSE 2 END AS value FROM metadata_inputs",
                    "CASE WHEN true THEN k ELSE 2 END",
                    vec!["", "2", "3"],
                ),
                (
                    "SELECT SUM(k) AS value FROM metadata_inputs",
                    "sum(k)",
                    vec!["5"],
                ),
                (
                    "SELECT NULL + (SELECT 1) AS value",
                    "(NULL + subquery)",
                    vec![""],
                ),
                (
                    "SELECT ROUND((SELECT 1),NULL) AS value",
                    "round(subquery, NULL)",
                    vec![""],
                ),
            ] {
                for supplied in [
                    vec![],
                    vec![("unit".to_owned(), "reading".to_owned())],
                    vec![(String::new(), "empty-key".to_owned())],
                    vec![
                        ("unit".to_owned(), "reading".to_owned()),
                        (String::new(), "empty-key".to_owned()),
                        ("ARROW:extension:name".to_owned(), "test.reading".to_owned()),
                        ("ARROW:extension:metadata".to_owned(), String::new()),
                    ],
                ] {
                    for rename in [true, false] {
                        let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
                        let mut query = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                        let spec::QueryNode::Project { expressions, .. } = &mut query.node else {
                            return Err(crate::error::PlanError::internal(
                                "expected SELECT projection",
                            ));
                        };
                        let spec::Expr::Alias { name, metadata, .. } = &mut expressions[0] else {
                            return Err(crate::error::PlanError::internal(
                                "expected supplied alias",
                            ));
                        };
                        if !rename {
                            name.clear();
                        }
                        *metadata = Some(supplied.clone());
                        let expected = supplied.iter().cloned().collect::<HashMap<_, _>>();
                        let named = resolver.resolve_named_plan(query).await?;
                        let expected_name = if rename { "value" } else { unaliased_name };
                        assert_eq!(named.fields, [expected_name]);
                        assert_eq!(named.plan.schema().field(0).metadata(), &expected);
                        let mut state = ctx.state();
                        let extensions = &mut state.config_mut().options_mut().extensions;
                        extensions.insert(SparkSqlExpressionSemantics::default());
                        extensions.insert(PreserveAggregateStatisticsSchema::default());
                        let physical = DataFrame::new(state, named.plan)
                            .create_physical_plan()
                            .await?;
                        assert_eq!(physical.schema().field(0).metadata(), &expected);
                        let schema = physical.schema();
                        let batches =
                            datafusion::physical_plan::collect(physical, ctx.task_ctx()).await?;
                        let mut values = Vec::new();
                        for batch in batches {
                            assert_eq!(batch.schema(), schema);
                            for (field, column) in schema.fields().iter().zip(batch.columns()) {
                                if !field.is_nullable() {
                                    assert_eq!(column.logical_null_count(), 0);
                                }
                                column.to_data().validate_full()?;
                            }
                            for row in 0..batch.num_rows() {
                                values.push(array_value_to_string(batch.column(0).as_ref(), row)?);
                            }
                        }
                        values.sort();
                        assert_eq!(values, expected_values);
                    }
                }
            }
        }
        Ok(())
    }

    fn create_session() -> PlanResult<SessionContext> {
        let state = SessionStateBuilder::new().build();
        Ok(SessionContext::new_with_state(state))
    }

    #[tokio::test]
    async fn test_resolve_expression_with_name() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(
            &ctx,
            Arc::new(PlanConfig {
                ansi_mode: false,
                ..Default::default()
            }),
        );

        async fn resolve(resolver: &PlanResolver<'_>, expr: spec::Expr) -> PlanResult<NamedExpr> {
            resolver
                .resolve_named_expression(
                    expr,
                    &Arc::new(DFSchema::empty()),
                    &mut PlanResolverState::new(),
                )
                .await
        }

        assert_eq!(
            resolve(
                &resolver,
                spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                    function_name: spec::ObjectName::bare("not"),
                    arguments: vec![spec::Expr::Literal(spec::Literal::Boolean {
                        value: Some(true)
                    })],
                    named_arguments: vec![],
                    is_distinct: false,
                    is_user_defined_function: false,
                    is_internal: None,
                    ignore_nulls: None,
                    filter: None,
                    order_by: None,
                })
            )
            .await?,
            NamedExpr {
                name: vec!["(NOT true)".to_string()],
                expression_name: None,
                expr: Expr::Not(Box::new(Expr::Literal(
                    ScalarValue::Boolean(Some(true)),
                    None
                ))),
                metadata: Default::default(),
                interval_qualifier: None,
            }
        );

        assert_eq!(
            resolve(
                &resolver,
                spec::Expr::Alias {
                    // This name "b" is overridden by the outer name "c".
                    expr: Box::new(spec::Expr::Alias {
                        // The resolver assigns a name (a human-readable string) for the function,
                        // and is then overridden by the explicitly specified outer name.
                        expr: Box::new(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                            function_name: spec::ObjectName::bare("+"),
                            arguments: vec![
                                spec::Expr::Alias {
                                    // The resolver assigns a name "1" for the literal,
                                    // and is then overridden by the explicitly specified name.
                                    expr: Box::new(spec::Expr::Literal(spec::Literal::Int32 {
                                        value: Some(1)
                                    })),
                                    name: vec!["a".to_string().into()],
                                    metadata: None,
                                },
                                // The resolver assigns a name "2" for the literal.
                                spec::Expr::Literal(spec::Literal::Int32 { value: Some(2) }),
                            ],
                            named_arguments: vec![],
                            is_distinct: false,
                            is_user_defined_function: false,
                            is_internal: None,
                            ignore_nulls: None,
                            filter: None,
                            order_by: None
                        })),
                        name: vec!["b".to_string().into()],
                        metadata: None,
                    }),
                    name: vec!["c".to_string().into()],
                    metadata: None,
                }
            )
            .await?,
            NamedExpr {
                name: vec!["c".to_string()],
                expression_name: None,
                expr: Expr::Alias(Alias {
                    expr: Box::new(Expr::Alias(Alias {
                        expr: Box::new(Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(Expr::Alias(Alias {
                                expr: Box::new(Expr::Literal(ScalarValue::Int32(Some(1)), None)),
                                name: "a".to_string(),
                                relation: None,
                                metadata: None,
                            })),
                            op: Operator::Plus,
                            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(2)), None)),
                        })),
                        relation: None,
                        name: "b".to_string(),
                        metadata: None,
                    })),
                    relation: None,
                    name: "c".to_string(),
                    metadata: None,
                }),
                metadata: Default::default(),
                interval_qualifier: None,
            },
        );

        Ok(())
    }

    fn assert_metadata_value(
        metadata_map: &HashMap<String, String>,
        key: &str,
        expected_value: &str,
    ) {
        assert_eq!(
            metadata_map.get(key),
            Some(&expected_value.to_string()),
            "Expected {} in metadata, got: {:?}",
            key,
            metadata_map
        );
    }

    #[tokio::test]
    async fn test_st_geomfromwkb_returns_geometry_metadata() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));

        let result = resolver
            .resolve_named_expression(
                spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                    function_name: spec::ObjectName::bare("st_geomfromwkb"),
                    arguments: vec![spec::Expr::Literal(spec::Literal::Binary {
                        value: Some(vec![
                            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 240, 255, 63, 0, 0, 0, 0, 0, 0, 64,
                        ]),
                    })],
                    named_arguments: vec![],
                    is_distinct: false,
                    is_user_defined_function: false,
                    is_internal: None,
                    ignore_nulls: None,
                    filter: None,
                    order_by: None,
                }),
                &Arc::new(DFSchema::empty()),
                &mut PlanResolverState::new(),
            )
            .await?;

        let metadata: Vec<(String, String)> = result.metadata.iter().as_slice().to_vec();
        let metadata_map: HashMap<_, _> = metadata.clone().into_iter().collect();

        assert_metadata_value(
            &metadata_map,
            arrow_schema::extension::EXTENSION_TYPE_NAME_KEY,
            GeoArrowWkbType::NAME,
        );
        assert_metadata_value(
            &metadata_map,
            arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY,
            r#"{"crs":"SRID:0"}"#,
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_st_geogfromwkb_returns_geography_metadata() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));

        let result = resolver
            .resolve_named_expression(
                spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                    function_name: spec::ObjectName::bare("st_geogfromwkb"),
                    arguments: vec![spec::Expr::Literal(spec::Literal::Binary {
                        value: Some(vec![
                            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 240, 255, 63, 0, 0, 0, 0, 0, 0, 64,
                        ]),
                    })],
                    named_arguments: vec![],
                    is_distinct: false,
                    is_user_defined_function: false,
                    is_internal: None,
                    ignore_nulls: None,
                    filter: None,
                    order_by: None,
                }),
                &Arc::new(DFSchema::empty()),
                &mut PlanResolverState::new(),
            )
            .await?;

        let metadata: Vec<(String, String)> = result.metadata.iter().as_slice().to_vec();
        let metadata_map: HashMap<_, _> = metadata.clone().into_iter().collect();

        assert_metadata_value(
            &metadata_map,
            arrow_schema::extension::EXTENSION_TYPE_NAME_KEY,
            GeoArrowWkbType::NAME,
        );
        assert_metadata_value(
            &metadata_map,
            arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY,
            r#"{"crs":"OGC:CRS84","edges":"spherical"}"#,
        );

        Ok(())
    }
}
