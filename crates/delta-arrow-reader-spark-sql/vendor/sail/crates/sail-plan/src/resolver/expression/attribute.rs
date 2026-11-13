// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchemaRef, ScalarValue, TableReference};
use datafusion_expr::expr::{LambdaVariable, ScalarFunction};
use datafusion_expr::{ExprSchemable, ScalarUDF, col, expr, lit};
use datafusion_functions::core::get_field;
use datafusion_functions::core::getfield::GetFieldFunc;
use sail_common::spec;
use sail_function::scalar::array_struct_field::ArrayStructField;
use sail_function::scalar::get_field_with_parent_nulls::GetFieldWithParentNulls;

use crate::error::{PlanError, PlanResult};
use crate::function::common::clear_inherited_field_metadata;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::lambda::LAMBDA_VARIABLE_EXPRESSION_NAME;
use crate::resolver::schema::quote_column_name;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(in super::super) fn resolve_expression_attribute(
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
            let mut expr = expr::Expr::LambdaVariable(LambdaVariable::new(declared, field.clone()));
            let mut expression_name = LAMBDA_VARIABLE_EXPRESSION_NAME.to_string();
            if let Some(field) = field {
                // Spark resolves dotted lambda tails with strict NULL-base validation.
                expr = Self::resolve_nested_field_and_append_display(
                    expr,
                    field.data_type(),
                    rest,
                    schema,
                    false,
                    &mut expression_name,
                )?;
            } else {
                for part in rest {
                    expr = expr::Expr::ScalarFunction(ScalarFunction::new_udf(
                        get_field(),
                        vec![expr, lit(part.as_ref().to_string())],
                    ));
                    expression_name.push('.');
                    expression_name.push_str(part.as_ref());
                }
            }
            expr = Self::guard_nested_parent_nulls(expr, schema)?;
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
        if let Some(output) = state.sort_select_schema(schema)
            && let Some(named) = self.resolve_field_or_nested_field(&name, &output, state, false)?
        {
            return Ok(named);
        }
        if let Some(named) = self.resolve_field_or_nested_field(&name, schema, state, false)? {
            return Ok(named);
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_grouping())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) = self.resolve_hidden_field(&name, schema, state)? {
            return Ok(NamedExpr::new(vec![name], expr).with_column_origin());
        }
        if let Some(outer_schema) = state.get_outer_query_schema().cloned()
            && let Some(named) =
                self.resolve_field_or_nested_field(&name, &outer_schema, state, true)?
        {
            return Ok(named);
        }
        Err(Self::unknown_column_error(
            &name,
            Self::available_attribute_names(schema, state),
        ))
    }

    fn available_attribute_names(schema: &DFSchemaRef, state: &PlanResolverState) -> Vec<String> {
        let selected = state.sort_select_schema(schema);
        let mut columns = Vec::new();
        for schema in std::iter::once(schema)
            .chain(selected.as_ref())
            .chain(state.get_outer_query_schema())
        {
            for (qualifier, field) in schema.iter() {
                let Ok(info) = state.get_field_info(field.name()) else {
                    continue;
                };
                let mut parts = Vec::new();
                if let Some(qualifier) = qualifier {
                    parts.extend(qualifier.catalog());
                    parts.extend(qualifier.schema());
                    parts.push(qualifier.table());
                }
                parts.push(info.name());
                columns.push(quote_column_name(parts));
            }
        }
        for named in state
            .get_projections_for_grouping()
            .iter()
            .chain(state.get_projections_for_having())
            .chain(state.get_grouping_for_having())
        {
            columns.extend(named.name.iter().map(|name| quote_column_name([name])));
        }
        columns.extend(
            state
                .lambda_parameter_names()
                .map(|name| quote_column_name([name])),
        );
        columns
    }

    fn resolve_field_or_nested_field(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &PlanResolverState,
        is_outer: bool,
    ) -> PlanResult<Option<NamedExpr>> {
        let mut candidates = vec![];
        let mut identities = HashSet::new();
        // Spark tries catalog/schema/table, schema/table, table, then unqualified columns.
        for (q, column_name, inner) in
            Self::generate_qualified_nested_field_candidates(name.parts())
        {
            for (qualifier, field) in schema.iter() {
                let Ok(info) = state.get_field_info(field.name()) else {
                    continue;
                };
                if (!info.is_hidden() || q.is_some())
                    && qualifier_matches(q.as_ref(), qualifier)
                    && info.name().eq_ignore_ascii_case(column_name.as_ref())
                {
                    if identities.insert(state.column_identity(&Column::from((qualifier, field)))?)
                    {
                        candidates.push((qualifier, field, column_name, inner));
                    }
                }
            }
            if !candidates.is_empty() {
                break;
            }
        }
        if candidates.len() > 1 {
            let scope = if is_outer { "outer " } else { "" };
            return Err(PlanError::AnalysisError(format!(
                "ambiguous {scope}attribute: {name:?}"
            )));
        }
        let Some((qualifier, field, name, inner)) = candidates.pop() else {
            return Ok(None);
        };
        let base = if is_outer {
            let mut outer_field = field.as_ref().clone();
            clear_inherited_field_metadata(&mut outer_field);
            expr::Expr::OuterReferenceColumn(
                Arc::new(outer_field),
                Column::new(qualifier.cloned(), field.name()),
            )
        } else {
            col((qualifier, field))
        };
        // A bound local base shadows outer columns even when its nested tail is invalid.
        let mut expression_name = name.as_ref().to_string();
        let expr = Self::resolve_nested_field_and_append_display(
            base,
            field.data_type(),
            inner,
            schema,
            true,
            &mut expression_name,
        )?;
        let expr = if !inner.is_empty() {
            Self::guard_nested_parent_nulls(expr, schema)?
        } else {
            expr
        };
        let projection_name = inner.last().unwrap_or(name).as_ref().to_string();
        let named = NamedExpr::new(vec![projection_name], expr);
        Ok(Some(if inner.is_empty() {
            named.with_column_origin()
        } else {
            named.with_expression_name(expression_name)
        }))
    }

    pub(super) fn guard_nested_parent_nulls(
        expr: expr::Expr,
        schema: &DFSchemaRef,
    ) -> PlanResult<expr::Expr> {
        Ok(expr
            .transform_up(|mut expr| -> datafusion_common::Result<_> {
                if let expr::Expr::ScalarFunction(function) = &mut expr
                    && function.func.inner().is::<GetFieldFunc>()
                    && let Some(base) = function.args.first()
                    && base.nullable(schema)?
                {
                    function.func = Arc::new(ScalarUDF::from(GetFieldWithParentNulls::new()));
                    return Ok(Transformed::yes(expr));
                }
                Ok(Transformed::no(expr))
            })?
            .data)
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

    fn resolve_struct_field<'a>(fields: &'a Fields, name: &str) -> PlanResult<&'a FieldRef> {
        let mut matching = fields
            .iter()
            .filter(|field| field.name().eq_ignore_ascii_case(name));
        let Some(field) = matching.next() else {
            return Err(PlanError::analysis(format!(
                "cannot resolve attribute: [FIELD_NOT_FOUND] field {name:?} is absent from STRUCT fields {:?}",
                fields.iter().map(|field| field.name()).collect::<Vec<_>>()
            )));
        };
        if matching.next().is_some() {
            return Err(PlanError::analysis(format!(
                "cannot resolve attribute: [AMBIGUOUS_REFERENCE_TO_FIELDS] field {name:?} matches multiple STRUCT children"
            )));
        }
        Ok(field)
    }

    /// Resolve a dotted tail and append its display to the caller's base name.
    /// Map steps use brackets; field steps keep dots and input spelling.
    /// Callers own projection names and must discard partial display on error.
    pub(super) fn resolve_nested_field_and_append_display<T: AsRef<str>>(
        expr: expr::Expr,
        data_type: &DataType,
        inner: &[T],
        schema: &DFSchemaRef,
        allow_null_base: bool,
        display_name: &mut String,
    ) -> PlanResult<expr::Expr> {
        match inner {
            [] => Ok(expr),
            [name, remaining @ ..] => {
                let lookup_type = match data_type {
                    DataType::Dictionary(_, value_type)
                        if matches!(value_type.as_ref(), DataType::Struct(_)) =>
                    {
                        value_type.as_ref()
                    }
                    _ => data_type,
                };
                let map_key = matches!(lookup_type, DataType::Map(_, _));
                display_name.push(if map_key { '[' } else { '.' });
                display_name.push_str(name.as_ref());
                if map_key {
                    display_name.push(']');
                }
                match lookup_type {
                    // Spark folds NULL extraction before evaluating even a projected base.
                    DataType::Null if allow_null_base => {
                        for part in remaining {
                            display_name.push('.');
                            display_name.push_str(part.as_ref());
                        }
                        Ok(lit(ScalarValue::Null))
                    }
                    DataType::Struct(fields) => {
                        let field = Self::resolve_struct_field(fields, name.as_ref())?;
                        let args = vec![expr, lit(field.name().to_string())];
                        let expr =
                            expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                        let native_type;
                        let result_type = if matches!(data_type, DataType::Dictionary(_, _)) {
                            native_type = expr.get_type(schema)?;
                            &native_type
                        } else {
                            field.data_type()
                        };
                        Self::resolve_nested_field_and_append_display(
                            expr,
                            result_type,
                            remaining,
                            schema,
                            allow_null_base,
                            display_name,
                        )
                    }
                    DataType::Map(_, _) => {
                        let expr = get_field().call(vec![expr, lit(name.as_ref().to_string())]);
                        let data_type = expr.get_type(schema)?;
                        Self::resolve_nested_field_and_append_display(
                            expr,
                            &data_type,
                            remaining,
                            schema,
                            allow_null_base,
                            display_name,
                        )
                    }
                    DataType::List(field)
                    | DataType::LargeList(field)
                    | DataType::FixedSizeList(field, _) => {
                        let DataType::Struct(fields) = field.data_type() else {
                            return Err(PlanError::analysis(format!(
                                "cannot resolve attribute: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] field {:?} requires a STRUCT array element, got {}",
                                name.as_ref(),
                                field.data_type()
                            )));
                        };
                        let child = Self::resolve_struct_field(fields, name.as_ref())?;
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
                        Self::resolve_nested_field_and_append_display(
                            expr,
                            &data_type,
                            remaining,
                            schema,
                            allow_null_base,
                            display_name,
                        )
                    }
                    _ => Err(PlanError::analysis(format!(
                        "cannot resolve attribute: [INVALID_EXTRACT_BASE_FIELD_TYPE] field {:?} cannot be extracted from {data_type}",
                        name.as_ref()
                    ))),
                }
            }
        }
    }

    pub(super) fn generate_qualified_nested_field_candidates<T: AsRef<str>>(
        name: &[T],
    ) -> Vec<(Option<TableReference>, &T, &[T])> {
        let mut out = vec![];
        if let [n1, n2, n3, n4, x @ ..] = name {
            out.push((
                Some(TableReference::full(n1.as_ref(), n2.as_ref(), n3.as_ref())),
                n4,
                x,
            ));
        }
        if let [n1, n2, n3, x @ ..] = name {
            out.push((
                Some(TableReference::partial(n1.as_ref(), n2.as_ref())),
                n3,
                x,
            ));
        }
        if let [n1, n2, x @ ..] = name {
            out.push((Some(TableReference::bare(n1.as_ref())), n2, x));
        }
        if let [n1, x @ ..] = name {
            out.push((None, n1, x));
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
    fn nested_display_appends_preserve_typed_lookup_policies() -> PlanResult<()> {
        let leaf = Arc::new(
            Field::new("Leaf", DataType::Int32, false)
                .with_metadata([("source".into(), "child".into())].into()),
        );
        let record = DataType::Struct(vec![leaf.clone()].into());
        let map = |value_type| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", value_type, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        let list_item = Arc::new(Field::new_list_field(record.clone(), true));
        let result_item = Arc::new(leaf.as_ref().clone().with_name("item").with_nullable(true));
        for (data_type, tail, display, expected_type) in [
            (record.clone(), vec!["leaf"], "base.leaf", DataType::Int32),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(record.clone())),
                vec!["leaf"],
                "base.leaf",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int32)),
            ),
            (
                DataType::List(list_item.clone()),
                vec!["leaf"],
                "base.leaf",
                DataType::List(result_item.clone()),
            ),
            (
                DataType::LargeList(list_item.clone()),
                vec!["leaf"],
                "base.leaf",
                DataType::LargeList(result_item.clone()),
            ),
            (
                DataType::FixedSizeList(list_item, 2),
                vec!["leaf"],
                "base.leaf",
                DataType::FixedSizeList(result_item, 2),
            ),
            (
                map(record),
                vec!["outer", "leaf"],
                "base[outer].leaf",
                DataType::Int32,
            ),
            (
                DataType::Struct(vec![Field::new("Map", map(DataType::Int32), true)].into()),
                vec!["map", "a.b"],
                "base.map[a.b]",
                DataType::Int32,
            ),
            (
                map(map(DataType::Int32)),
                vec!["outer", "key"],
                "base[outer][key]",
                DataType::Int32,
            ),
        ] {
            let field = Arc::new(Field::new("base", data_type, true));
            let schema = Arc::new(DFSchema::new_with_metadata(
                vec![(None, field.clone())],
                Default::default(),
            )?);
            let mut actual_display = "base".to_string();
            let resolved = PlanResolver::resolve_nested_field_and_append_display(
                col((None::<&TableReference>, &field)),
                field.data_type(),
                &tail,
                &schema,
                false,
                &mut actual_display,
            )?;
            assert_eq!(actual_display, display);
            assert_eq!(resolved.get_type(&schema)?, expected_type);
        }
        let schema = Arc::new(DFSchema::empty());
        let mut display = "base".to_string();
        assert_eq!(
            PlanResolver::resolve_nested_field_and_append_display(
                lit(ScalarValue::Null),
                &DataType::Null,
                &["key", "unvalidated"],
                &schema,
                true,
                &mut display,
            )?,
            lit(ScalarValue::Null),
        );
        assert_eq!(display, "base.key.unvalidated");
        for data_type in [DataType::Null, DataType::Int32] {
            let result = PlanResolver::resolve_nested_field_and_append_display(
                lit(ScalarValue::try_from(&data_type)?),
                &data_type,
                &["key"],
                &schema,
                false,
                &mut String::new(),
            );
            assert!(matches!(result, Err(PlanError::AnalysisError(message))
            if message == format!(
                "cannot resolve attribute: [INVALID_EXTRACT_BASE_FIELD_TYPE] field \"key\" cannot be extracted from {data_type}"
            )));
        }
        Ok(())
    }

    #[test]
    fn nested_display_appends_handle_a_bounded_deep_struct_path() -> PlanResult<()> {
        let data_type = (0..64).fold(DataType::Int32, |inner, _| {
            DataType::Struct(vec![Field::new("next", inner, false)].into())
        });
        let field = Arc::new(Field::new("base", data_type, false));
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![(None, field.clone())],
            Default::default(),
        )?);
        let mut display = "base".to_string();
        let resolved = PlanResolver::resolve_nested_field_and_append_display(
            col((None::<&TableReference>, &field)),
            field.data_type(),
            &["next"; 64],
            &schema,
            false,
            &mut display,
        )?;
        assert_eq!(display, format!("base{}", ".next".repeat(64)));
        assert_eq!(resolved.get_type(&schema)?, DataType::Int32);
        Ok(())
    }

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
                    resolver.resolve_field_or_nested_field(
                        &parts.into(),
                        &schema,
                        &state,
                        false
                    )?,
                    Some(
                        NamedExpr::new(vec!["id".into()], col((Some(&qualifier), &id)),)
                            .with_column_origin()
                    ),
                );
            }
            assert_eq!(
                resolver.resolve_field_or_nested_field(
                    &["p", "p", "id"].into(),
                    &schema,
                    &state,
                    false,
                )?,
                Some(
                    NamedExpr::new(
                        vec!["id".into()],
                        ScalarUDF::from(GetFieldWithParentNulls::new())
                            .call(vec![col((Some(&qualifier), &nested)), lit("id")]),
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
                    &state,
                    false,
                )?,
                Some(
                    NamedExpr::new(
                        vec!["id".into()],
                        ScalarUDF::from(GetFieldWithParentNulls::new())
                            .call(vec![col((Some(&qualifier), &nested)), lit("id")]),
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
                    &state,
                    false,
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
                        resolver.resolve_field_or_nested_field(name, &schema, &state, false)?,
                        Some(
                            NamedExpr::new(vec!["id".into()], col((Some(&qualifier), &id)),)
                                .with_column_origin()
                        ),
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
                        resolver.resolve_field_or_nested_field(name, &duplicates, &state, false),
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
            (
                Some(DataType::Int32),
                Some("[INVALID_EXTRACT_BASE_FIELD_TYPE]"),
            ),
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
                    )
                    .with_column_origin(),
                );
            }
        }
        Ok(())
    }

    #[test]
    fn typed_lambda_fields_use_the_same_child_validation() -> PlanResult<()> {
        let session = SessionContext::new();
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));
        let schema = Arc::new(DFSchema::empty());
        for (data_type, condition) in [
            (
                DataType::Struct(vec![Arc::new(Field::new("Item", DataType::Int32, true))].into()),
                None,
            ),
            (
                DataType::Struct(vec![Arc::new(Field::new("other", DataType::Int32, true))].into()),
                Some("[FIELD_NOT_FOUND]"),
            ),
            (
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("item", DataType::Int32, true)),
                        Arc::new(Field::new("ITEM", DataType::Int32, true)),
                    ]
                    .into(),
                ),
                Some("[AMBIGUOUS_REFERENCE_TO_FIELDS]"),
            ),
            (DataType::Int32, Some("[INVALID_EXTRACT_BASE_FIELD_TYPE]")),
        ] {
            let mut state = PlanResolverState::new();
            let field = Arc::new(Field::new("x", data_type, true));
            let mut scope = state.enter_lambda_scope(vec![("x".into(), Some(field.clone()))]);
            let resolved = resolver.resolve_expression_attribute(
                ["x", "item"].into(),
                false,
                &schema,
                scope.state(),
            );
            if let Some(condition) = condition {
                assert!(matches!(resolved,
                    Err(PlanError::AnalysisError(message))
                        if message.contains(condition) && message.contains("field \"item\"")));
            } else {
                assert_eq!(
                    resolved?.expr,
                    ScalarUDF::from(GetFieldWithParentNulls::new()).call(vec![
                        expr::Expr::LambdaVariable(LambdaVariable::new("x".into(), Some(field))),
                        lit("Item"),
                    ]),
                );
            }
        }
        let mut state = PlanResolverState::new();
        let mut scope = state.enter_lambda_scope(vec![("x".into(), None)]);
        assert_eq!(
            resolver
                .resolve_expression_attribute(["x", "item"].into(), false, &schema, scope.state(),)?
                .expr,
            ScalarUDF::from(GetFieldWithParentNulls::new()).call(vec![
                expr::Expr::LambdaVariable(LambdaVariable::new("x".into(), None)),
                lit("item"),
            ]),
        );
        Ok(())
    }
}
