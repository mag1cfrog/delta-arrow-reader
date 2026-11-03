// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{Column, DFSchemaRef, TableReference};
use datafusion_expr::expr::{LambdaVariable, ScalarFunction};
use datafusion_expr::{ScalarUDF, col, expr, lit};
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_function::scalar::array_struct_field::ArrayStructField;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::lambda::LAMBDA_VARIABLE_EXPRESSION_NAME;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) fn resolve_expression_attribute(
        &self,
        name: spec::ObjectName,
        is_metadata_column: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        if is_metadata_column {
            return Err(PlanError::todo("resolve metadata column"));
        }
        // Lambda parameters shadow columns inside a lambda function body. SQL lambda
        // bodies reference parameters as plain attributes, so the lambda scope stack
        // is consulted first.
        if let [first, rest @ ..] = name.parts()
            && let Some((declared, field)) = state
                .resolve_lambda_parameter(first.as_ref())
                .map(|(param, field)| (param.to_string(), field.cloned()))
        {
            let projection_name = rest
                .last()
                .map(|x| x.as_ref())
                .unwrap_or(declared.as_str())
                .to_string();
            let mut expr = expr::Expr::LambdaVariable(LambdaVariable::new(declared, field));
            for part in rest {
                expr = expr::Expr::ScalarFunction(ScalarFunction::new_udf(
                    get_field(),
                    vec![expr, lit(part.as_ref().to_string())],
                ));
            }
            let expression_name = std::iter::once(LAMBDA_VARIABLE_EXPRESSION_NAME)
                .chain(rest.iter().map(|part| part.as_ref()))
                .collect::<Vec<_>>()
                .join(".");
            return Ok(
                NamedExpr::new(vec![projection_name], expr).with_expression_name(expression_name)
            );
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_grouping_for_having())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_having())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some(named) = self.resolve_field_or_nested_field(&name, schema, state)? {
            return Ok(named);
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_grouping())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) = self.resolve_hidden_field(&name, schema, state)? {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        // Keep suggestion-bearing scopes on their existing diagnostic path.
        let no_suggestions = schema.fields().is_empty()
            && state.get_projections_for_grouping().is_empty()
            && state.get_projections_for_having().is_empty()
            && state.get_grouping_for_having().is_empty()
            && !state.in_lambda_scope();
        let condition = if no_suggestions {
            " [UNRESOLVED_COLUMN.WITHOUT_SUGGESTION]"
        } else {
            ""
        };
        let Some(outer_schema) = state.get_outer_query_schema().cloned() else {
            return Err(PlanError::AnalysisError(format!(
                // Spark tests expect the error message to start with: "attribute {name:?} is missing"
                "attribute {name:?} is missing from the schema: cannot resolve attribute{condition}"
            )));
        };
        let condition = if no_suggestions && outer_schema.fields().is_empty() {
            condition
        } else {
            ""
        };
        match self.resolve_outer_field(&name, &outer_schema, state)? {
            Some((name, expr)) => Ok(NamedExpr::new(vec![name], expr)),
            None => Err(PlanError::AnalysisError(format!(
                // Spark tests expect the error message to start with: "attribute {name:?} is missing"
                "attribute {name:?} is missing from the schema: cannot resolve attribute or outer attribute{condition}"
            ))),
        }
    }

    fn resolve_field_or_nested_field(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<NamedExpr>> {
        let candidates = Self::generate_qualified_nested_field_candidates(name.parts());
        let mut candidates = schema
            .iter()
            .flat_map(|(qualifier, field)| {
                let Ok(info) = state.get_field_info(field.name()) else {
                    return vec![];
                };
                candidates
                    .iter()
                    .filter_map(|(q, name, inner)| {
                        if (!info.is_hidden() || q.is_some())
                            && qualifier_matches(q.as_ref(), qualifier)
                            && info.name().eq_ignore_ascii_case(name.as_ref())
                        {
                            let expr = Self::resolve_potentially_nested_field(
                                col((qualifier, field)),
                                field.data_type(),
                                inner,
                            )?;
                            let projection_name = inner.last().unwrap_or(name).as_ref().to_string();
                            let named = NamedExpr::new(vec![projection_name], expr);
                            Some(if inner.is_empty() {
                                named
                            } else {
                                named.with_expression_name(
                                    std::iter::once(*name)
                                        .chain(inner.iter())
                                        .map(|part| part.as_ref())
                                        .collect::<Vec<_>>()
                                        .join("."),
                                )
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_aggregate_field(
        &self,
        name: &spec::ObjectName,
        expressions: &[NamedExpr],
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let [name] = name.parts() else {
            return Ok(None);
        };
        let mut candidates = expressions
            .iter()
            .filter_map(|expr| {
                let NamedExpr {
                    name: agg, expr, ..
                } = expr;
                match agg.as_slice() {
                    [agg] if agg.eq_ignore_ascii_case(name.as_ref()) => {
                        Some((name.as_ref().to_string(), expr.clone()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous aggregate expression: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_hidden_field(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let [name] = name.parts() else {
            return Ok(None);
        };
        let mut candidates = schema
            .iter()
            .filter_map(|(qualifier, field)| {
                if qualifier.is_some() {
                    return None;
                }
                let Ok(info) = state.get_field_info(field.name()) else {
                    return None;
                };
                if !info.is_hidden() {
                    return None;
                }
                if info.name().eq_ignore_ascii_case(name.as_ref()) {
                    Some((
                        name.as_ref().to_string(),
                        expr::Expr::Column(Column::new_unqualified(field.name())),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_outer_field(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let candidates = Self::generate_qualified_field_candidates(name.parts());
        let mut candidates = schema
            .iter()
            .flat_map(|(qualifier, field)| {
                let Ok(info) = state.get_field_info(field.name()) else {
                    return vec![];
                };
                candidates
                    .iter()
                    .filter(|(q, name)| {
                        (!info.is_hidden() || q.is_some())
                            && qualifier_matches(q.as_ref(), qualifier)
                            && info.name().eq_ignore_ascii_case(name.as_ref())
                    })
                    .map(|(_, name)| {
                        (
                            name.as_ref().to_string(),
                            expr::Expr::OuterReferenceColumn(
                                field.clone(),
                                Column::new(qualifier.cloned(), field.name()),
                            ),
                        )
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous outer attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_potentially_nested_field<T: AsRef<str>>(
        expr: expr::Expr,
        data_type: &DataType,
        inner: &[T],
    ) -> Option<expr::Expr> {
        match inner {
            [] => Some(expr),
            [name, remaining @ ..] => match data_type {
                DataType::Struct(fields) => fields
                    .iter()
                    .find(|x| x.name().eq_ignore_ascii_case(name.as_ref()))
                    .and_then(|field| {
                        let args = vec![expr, lit(field.name().to_string())];
                        let expr =
                            expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                        Self::resolve_potentially_nested_field(expr, field.data_type(), remaining)
                    }),
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) => {
                    let DataType::Struct(fields) = field.data_type() else {
                        return None;
                    };
                    fields
                        .iter()
                        .find(|x| x.name().eq_ignore_ascii_case(name.as_ref()))
                        .and_then(|child| {
                            let expr = ScalarUDF::from(ArrayStructField::new())
                                .call(vec![expr, lit(child.name().to_string())]);
                            let item = Arc::new(Field::new_list_field(
                                child.data_type().clone(),
                                field.is_nullable() || child.is_nullable(),
                            ));
                            let data_type = match data_type {
                                DataType::List(_) => DataType::List(item),
                                DataType::LargeList(_) => DataType::LargeList(item),
                                DataType::FixedSizeList(_, size) => {
                                    DataType::FixedSizeList(item, *size)
                                }
                                _ => unreachable!("list data type matched above"),
                            };
                            Self::resolve_potentially_nested_field(expr, &data_type, remaining)
                        })
                }
                _ => None,
            },
        }
    }

    fn generate_qualified_field_candidates<T: AsRef<str>>(
        name: &[T],
    ) -> Vec<(Option<TableReference>, &T)> {
        match name {
            [n1] => vec![(None, n1)],
            [n1, n2] => vec![(Some(TableReference::bare(n1.as_ref())), n2)],
            [n1, n2, n3] => vec![(Some(TableReference::partial(n1.as_ref(), n2.as_ref())), n3)],
            [n1, n2, n3, n4] => vec![(
                Some(TableReference::full(n1.as_ref(), n2.as_ref(), n3.as_ref())),
                n4,
            )],
            _ => vec![],
        }
    }

    fn generate_qualified_nested_field_candidates<T: AsRef<str>>(
        name: &[T],
    ) -> Vec<(Option<TableReference>, &T, &[T])> {
        let mut out = vec![];
        if let [n1, x @ ..] = name {
            out.push((None, n1, x));
        }
        if let [n1, n2, x @ ..] = name {
            out.push((Some(TableReference::bare(n1.as_ref())), n2, x));
        }
        if let [n1, n2, n3, x @ ..] = name {
            out.push((
                Some(TableReference::partial(n1.as_ref(), n2.as_ref())),
                n3,
                x,
            ));
        }
        if let [n1, n2, n3, n4, x @ ..] = name {
            out.push((
                Some(TableReference::full(n1.as_ref(), n2.as_ref(), n3.as_ref())),
                n4,
                x,
            ));
        }
        out
    }
}

/// Returns whether the qualifier matches the target qualifier.
/// Identifiers are case-insensitive.
/// Note that the match is not symmetric, so please ensure the arguments are in the correct order.
pub(super) fn qualifier_matches(
    qualifier: Option<&TableReference>,
    target: Option<&TableReference>,
) -> bool {
    let table_matches = |table: &str| {
        target
            .map(|x| x.table())
            .is_some_and(|x| x.eq_ignore_ascii_case(table))
    };
    let schema_matches = |schema: &str| {
        target
            .and_then(|x| x.schema())
            .is_some_and(|x| x.eq_ignore_ascii_case(schema))
    };
    let catalog_matches = |catalog: &str| {
        target
            .and_then(|x| x.catalog())
            .is_some_and(|x| x.eq_ignore_ascii_case(catalog))
    };
    match qualifier {
        Some(TableReference::Bare { table }) => table_matches(table),
        Some(TableReference::Partial { schema, table }) => {
            schema_matches(schema) && table_matches(table)
        }
        Some(TableReference::Full {
            catalog,
            schema,
            table,
        }) => catalog_matches(catalog) && schema_matches(schema) && table_matches(table),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use datafusion_common::DFSchema;

    use super::*;
    use crate::config::PlanConfig;

    #[test]
    fn qualified_columns_win_without_hiding_ambiguity() -> PlanResult<()> {
        let session = SessionContext::new();
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));
        for reversed in [false, true] {
            let mut state = PlanResolverState::new();
            let id = Arc::new(Field::new(
                state.register_field_name("id"),
                DataType::Int32,
                true,
            ));
            let nested = Arc::new(Field::new(
                state.register_field_name("p"),
                DataType::Struct(vec![Arc::new(Field::new("id", DataType::Int32, true))].into()),
                true,
            ));
            let qualifier = TableReference::full("catalog", "schema", "p");
            let mut fields = vec![
                (Some(qualifier.clone()), id.clone()),
                (Some(qualifier.clone()), nested.clone()),
            ];
            if reversed {
                fields.reverse();
            }
            let schema = Arc::new(DFSchema::new_with_metadata(
                fields.clone(),
                Default::default(),
            )?);
            for parts in [
                vec!["p", "id"],
                vec!["schema", "p", "id"],
                vec!["catalog", "schema", "p", "id"],
            ] {
                assert_eq!(
                    resolver.resolve_field_or_nested_field(&parts.into(), &schema, &mut state)?,
                    Some(NamedExpr::new(
                        vec!["id".into()],
                        col((Some(&qualifier), &id)),
                    )),
                );
            }
            assert_eq!(
                resolver.resolve_field_or_nested_field(
                    &["p", "p", "id"].into(),
                    &schema,
                    &mut state,
                )?,
                Some(
                    NamedExpr::new(
                        vec!["id".into()],
                        get_field().call(vec![col((Some(&qualifier), &nested)), lit("id")]),
                    )
                    .with_expression_name("p.id".into()),
                ),
            );
            let struct_only = Arc::new(DFSchema::new_with_metadata(
                vec![(Some(qualifier.clone()), nested.clone())],
                Default::default(),
            )?);
            assert_eq!(
                resolver.resolve_field_or_nested_field(
                    &["p", "id"].into(),
                    &struct_only,
                    &mut state,
                )?,
                Some(
                    NamedExpr::new(
                        vec!["id".into()],
                        get_field().call(vec![col((Some(&qualifier), &nested)), lit("id")]),
                    )
                    .with_expression_name("p.id".into()),
                ),
            );
            fields.push((
                Some(qualifier),
                Arc::new(Field::new(
                    state.register_field_name("id"),
                    DataType::Int32,
                    true,
                )),
            ));
            let duplicate_schema =
                Arc::new(DFSchema::new_with_metadata(fields, Default::default())?);
            assert!(matches!(
                resolver.resolve_field_or_nested_field(
                    &["p", "id"].into(),
                    &duplicate_schema,
                    &mut state,
                ),
                Err(PlanError::AnalysisError(message)) if message.starts_with("ambiguous attribute")
            ));
        }
        Ok(())
    }

    #[test]
    fn namespace_tiers_bind_before_nested_access_and_duplicate_checks() -> PlanResult<()> {
        let session = SessionContext::new();
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));
        for qualifier in [
            TableReference::partial("p", "p"),
            TableReference::full("spark_catalog", "p", "p"),
            TableReference::full("p", "p", "p"),
        ] {
            for reversed in [false, true] {
                let mut state = PlanResolverState::new();
                let id = Arc::new(Field::new(
                    state.register_field_name("id"),
                    DataType::Int32,
                    true,
                ));
                let nested = Arc::new(Field::new(
                    state.register_field_name("p"),
                    DataType::Struct(
                        vec![Arc::new(Field::new("id", DataType::Int32, true))].into(),
                    ),
                    true,
                ));
                let mut fields = vec![
                    (Some(qualifier.clone()), id.clone()),
                    (Some(qualifier.clone()), nested),
                ];
                if reversed {
                    fields.reverse();
                }
                let schema = Arc::new(DFSchema::new_with_metadata(
                    fields.clone(),
                    Default::default(),
                )?);
                let mut names = vec![spec::ObjectName::from(["p", "p", "id"])];
                if let Some(catalog) = qualifier.catalog() {
                    names.push([catalog, "p", "p", "id"].into());
                }
                for name in &names {
                    assert_eq!(
                        resolver.resolve_field_or_nested_field(name, &schema, &mut state)?,
                        Some(NamedExpr::new(
                            vec!["id".into()],
                            col((Some(&qualifier), &id)),
                        )),
                    );
                }
                fields.push((
                    Some(qualifier.clone()),
                    Arc::new(Field::new(
                        state.register_field_name("id"),
                        DataType::Int32,
                        true,
                    )),
                ));
                let duplicates = Arc::new(DFSchema::new_with_metadata(fields, Default::default())?);
                for name in &names {
                    assert!(matches!(
                        resolver.resolve_field_or_nested_field(name, &duplicates, &mut state),
                        Err(PlanError::AnalysisError(message))
                            if message == format!("ambiguous attribute: {name:?}")
                    ));
                }
            }
        }
        let mut state = PlanResolverState::new();
        let scalar = Arc::new(Field::new(
            state.register_field_name("p"),
            DataType::Int32,
            true,
        ));
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![(Some(TableReference::bare("p")), scalar)],
            Default::default(),
        )?);
        let name = spec::ObjectName::from(["p", "p", "id"]);
        assert!(matches!(
            resolver.resolve_expression_attribute(name.clone(), false, &schema, &mut state),
            Err(PlanError::AnalysisError(message))
                if message.contains("[INVALID_EXTRACT_BASE_FIELD_TYPE]")
                    && message.contains("cannot resolve attribute")
        ));
        Ok(())
    }

    #[test]
    fn selected_local_base_errors_do_not_reach_outer_scope() -> PlanResult<()> {
        let session = SessionContext::new();
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));
        for (local_type, condition) in [
            (None, None),
            (
                Some(DataType::Struct(
                    vec![Arc::new(Field::new("other", DataType::Int32, true))].into(),
                )),
                Some("[FIELD_NOT_FOUND]"),
            ),
            (Some(DataType::Int32), Some("[INVALID_EXTRACT_BASE_FIELD_TYPE]")),
        ] {
            let mut state = PlanResolverState::new();
            let outer_field = Arc::new(Field::new(
                state.register_field_name("id"),
                DataType::Int32,
                true,
            ));
            let qualifier = TableReference::bare("p");
            let outer = Arc::new(DFSchema::new_with_metadata(
                vec![(Some(qualifier.clone()), outer_field.clone())],
                Default::default(),
            )?);
            let fields = local_type
                .map(|data_type| {
                    vec![(
                        Some(TableReference::bare("q")),
                        Arc::new(Field::new(state.register_field_name("p"), data_type, true)),
                    )]
                })
                .unwrap_or_default();
            let local = Arc::new(DFSchema::new_with_metadata(fields, Default::default())?);
            let mut scope = state.enter_query_scope(outer);
            let resolved = resolver.resolve_expression_attribute(
                ["p", "id"].into(),
                false,
                &local,
                scope.state(),
            );
            if let Some(condition) = condition {
                assert!(matches!(resolved,
                    Err(PlanError::AnalysisError(message))
                        if message.contains(condition) && message.contains("id")));
            } else {
                assert_eq!(
                    resolved?,
                    NamedExpr::new(
                        vec!["id".into()],
                        expr::Expr::OuterReferenceColumn(
                            outer_field.clone(),
                            Column::new(Some(qualifier), outer_field.name()),
                        ),
                    ),
                );
            }
        }
        Ok(())
    }
}
