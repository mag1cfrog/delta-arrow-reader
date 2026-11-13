// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use arrow::datatypes::DataType;
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
use datafusion_common::{DFSchemaRef, TableReference};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ExprSchemable, ScalarUDF, col, expr, lit};
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::multi_expr::MultiExpr;
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::attribute::qualifier_matches;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_wildcard(
        &self,
        target: Option<spec::ObjectName>,
        mut wildcard_options: spec::WildcardOptions,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let exclusions = wildcard_options.except_columns.take();
        let wildcard = match target {
            Some(target) if wildcard_options == Default::default() => {
                self.resolve_wildcard_or_nested_field_wildcard(&target, schema, state)
            }
            _ => {
                let qualifier = target
                    .map(|x| self.resolve_table_reference(&x))
                    .transpose()?;
                let options = self.resolve_wildcard_options(wildcard_options).await?;
                Ok(NamedExpr::new(
                    vec!["*".to_string()],
                    #[expect(deprecated)]
                    expr::Expr::Wildcard {
                        qualifier,
                        options: Box::new(options),
                    },
                ))
            }
        }?;
        match exclusions {
            None => Ok(wildcard),
            Some(exclusions) => {
                let columns = self.expand_function_argument(wildcard, schema, state)?;
                let columns =
                    Self::resolve_wildcard_exclusions(columns, &exclusions, schema, state)?;
                let (names, expressions) = columns
                    .into_iter()
                    .map(|named| Ok((named.name.one()?, named.expr)))
                    .collect::<PlanResult<Vec<_>>>()?
                    .into_iter()
                    .unzip();
                Ok(NamedExpr::new(
                    names,
                    ScalarUDF::from(MultiExpr::new()).call(expressions),
                ))
            }
        }
    }

    fn resolve_wildcard_exclusions(
        columns: Vec<NamedExpr>,
        exclusions: &[spec::ObjectName],
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        if exclusions.is_empty() {
            return Err(PlanError::invalid(
                "except columns must have at least one column",
            ));
        }
        let mut paths = vec![Vec::new(); columns.len()];
        for exclusion in exclusions {
            let mut resolved = false;
            for (qualifier, column_name, inner) in
                Self::generate_qualified_nested_field_candidates(exclusion.parts())
            {
                let matches = columns
                    .iter()
                    .enumerate()
                    .filter(|(_, named)| {
                        named
                            .name
                            .first()
                            .is_some_and(|name| name.eq_ignore_ascii_case(column_name.as_ref()))
                            && match &named.expr {
                                expr::Expr::Column(column)
                                | expr::Expr::OuterReferenceColumn(_, column) => qualifier_matches(
                                    qualifier.as_ref(),
                                    column
                                        .relation
                                        .as_ref()
                                        .or_else(|| state.using_key_qualifier(column.name())),
                                ),
                                _ => qualifier.is_none(),
                            }
                    })
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                if matches.is_empty() {
                    continue;
                }
                let [index] = matches.as_slice() else {
                    return Err(PlanError::analysis(format!(
                        "ambiguous wildcard exclusion: {exclusion:?}"
                    )));
                };
                paths[*index].push(inner);
                resolved = true;
                break;
            }
            if !resolved {
                return Err(PlanError::analysis(format!(
                    "cannot resolve wildcard exclusion: {exclusion:?}"
                )));
            }
        }
        columns
            .into_iter()
            .zip(paths)
            .filter_map(|(mut column, paths)| {
                if paths.iter().any(|path| path.is_empty()) {
                    return None;
                }
                if paths.is_empty() {
                    return Some(Ok(column));
                }
                Some((|| {
                    let data_type = column.expr.get_type(schema)?;
                    column.expr =
                        Self::exclude_struct_fields(column.expr, &data_type, &paths, schema)?;
                    Ok(column)
                })())
            })
            .collect()
    }

    fn exclude_struct_fields(
        expression: expr::Expr,
        data_type: &DataType,
        paths: &[&[spec::Identifier]],
        schema: &DFSchemaRef,
    ) -> PlanResult<expr::Expr> {
        let DataType::Struct(fields) = data_type else {
            return Err(PlanError::analysis("wildcard exclusion requires a STRUCT"));
        };
        for path in paths {
            let matches = fields
                .iter()
                .filter(|field| field.name().eq_ignore_ascii_case(path[0].as_ref()))
                .count();
            if matches != 1 {
                return Err(PlanError::analysis(format!(
                    "missing or ambiguous wildcard exclusion field: {:?}",
                    path[0]
                )));
            }
        }
        let mut names = Vec::new();
        let mut arguments = Vec::new();
        for field in fields {
            let remaining = paths
                .iter()
                .filter(|path| field.name().eq_ignore_ascii_case(path[0].as_ref()))
                .map(|path| &path[1..])
                .collect::<Vec<_>>();
            if remaining.iter().any(|path| path.is_empty()) {
                continue;
            }
            let value = get_field().call(vec![expression.clone(), lit(field.name().clone())]);
            let value = if remaining.is_empty() {
                Self::guard_nested_parent_nulls(value, schema)?
            } else {
                Self::exclude_struct_fields(value, field.data_type(), &remaining, schema)?
            };
            names.push(field.name().clone());
            arguments.push(value);
        }
        // Spark rebuilds affected STRUCTs even when their original parent was NULL.
        Ok(ScalarUDF::from(StructFunction::new(names)).call(arguments))
    }

    fn resolve_wildcard_or_nested_field_wildcard(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        for (q, remaining) in Self::generate_qualified_wildcard_candidates(name.parts()) {
            if remaining.is_empty() {
                let matched = schema
                    .iter()
                    .find(|(qualifier, _)| qualifier_matches(q.as_ref(), *qualifier))
                    .or_else(|| {
                        state.get_outer_query_schema().and_then(|outer| {
                            outer
                                .iter()
                                .find(|(qualifier, _)| qualifier_matches(q.as_ref(), *qualifier))
                        })
                    });
                if let Some((qualifier, _)) = matched {
                    return Ok(NamedExpr::new(
                        vec!["*".to_string()],
                        #[expect(deprecated)]
                        expr::Expr::Wildcard {
                            qualifier: qualifier.cloned(),
                            options: Default::default(),
                        },
                    ));
                }
            }
        }

        let candidates = Self::generate_qualified_wildcard_candidates(name.parts())
            .into_iter()
            .flat_map(|(q, name)| match name {
                [] => vec![],
                [column, inner @ ..] => schema
                    .iter()
                    .filter_map(|(qualifier, field)| {
                        let Ok(info) = state.get_field_info(field.name()) else {
                            return None;
                        };
                        if (!info.is_hidden() || q.is_some())
                            && qualifier_matches(q.as_ref(), qualifier)
                            && info.name().eq_ignore_ascii_case(column.as_ref())
                        {
                            Self::resolve_nested_field_wildcard(
                                col((q.as_ref(), field)),
                                field.data_type(),
                                inner,
                            )
                        } else {
                            None
                        }
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let mut wildcard = candidates
            .one()
            .map_err(|_| PlanError::AnalysisError(format!("cannot resolve wildcard: {name:?}")))?;
        if let expr::Expr::ScalarFunction(function) = &mut wildcard.expr {
            for argument in &mut function.args {
                *argument = Self::guard_nested_parent_nulls(argument.clone(), schema)?;
            }
        }
        Ok(wildcard)
    }

    fn resolve_nested_field_wildcard<T: AsRef<str>>(
        expr: expr::Expr,
        data_type: &DataType,
        inner: &[T],
    ) -> Option<NamedExpr> {
        let DataType::Struct(fields) = data_type else {
            return None;
        };
        match inner {
            [] => {
                let (names, exprs) = fields
                    .iter()
                    .map(|field| {
                        let name = field.name().to_string();
                        let args = vec![expr.clone(), lit(name.clone())];
                        (
                            name,
                            expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args)),
                        )
                    })
                    .unzip();
                Some(NamedExpr::new(
                    names,
                    ScalarUDF::from(MultiExpr::new()).call(exprs),
                ))
            }
            [name, remaining @ ..] => fields
                .iter()
                .find(|x| x.name().eq_ignore_ascii_case(name.as_ref()))
                .and_then(|field| {
                    let args = vec![expr, lit(field.name().to_string())];
                    let expr =
                        expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                    Self::resolve_nested_field_wildcard(expr, field.data_type(), remaining)
                }),
        }
    }

    async fn resolve_wildcard_options(
        &self,
        wildcard_options: spec::WildcardOptions,
    ) -> PlanResult<expr::WildcardOptions> {
        fn make_ident(value: impl Into<String>) -> Ident {
            Ident::new(value.into())
        }

        fn make_object_name(value: impl Into<String>) -> ObjectName {
            ObjectName(vec![ObjectNamePart::Identifier(make_ident(value))])
        }

        let ilike = wildcard_options
            .ilike_pattern
            .map(|x| expr::IlikeSelectItem { pattern: x });
        let exclude = wildcard_options
            .exclude_columns
            .map(|x| {
                let exclude = if x.len() > 1 {
                    expr::ExcludeSelectItem::Multiple(x.into_iter().map(make_object_name).collect())
                } else if let Some(x) = x.into_iter().next() {
                    expr::ExcludeSelectItem::Single(make_object_name(x))
                } else {
                    return Err(PlanError::invalid(
                        "exclude columns must have at least one column",
                    ));
                };
                Ok(exclude)
            })
            .transpose()?;
        // ponytail: DF's sql feature changes these AST types. Exclude REPLACE/RENAME
        // from this probe until their metadata is adapted and tested.
        if wildcard_options.replace_columns.is_some() || wildcard_options.rename_columns.is_some() {
            return Err(PlanError::unsupported("wildcard REPLACE/RENAME"));
        }
        let replace = None;
        let rename = None;
        Ok(expr::WildcardOptions {
            ilike,
            exclude,
            except: None,
            replace,
            rename,
        })
    }

    fn generate_qualified_wildcard_candidates<T: AsRef<str>>(
        name: &[T],
    ) -> Vec<(Option<TableReference>, &[T])> {
        let mut out = vec![(None, name)];
        if let [n1, x @ ..] = name {
            out.push((Some(TableReference::bare(n1.as_ref())), x));
        }
        if let [n1, n2, x @ ..] = name {
            out.push((Some(TableReference::partial(n1.as_ref(), n2.as_ref())), x));
        }
        if let [n1, n2, n3, x @ ..] = name {
            out.push((
                Some(TableReference::full(n1.as_ref(), n2.as_ref(), n3.as_ref())),
                x,
            ));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use datafusion_common::DFSchema;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};

    use super::*;

    #[test]
    fn wildcard_exclusion_expression_growth_is_quadratic() -> PlanResult<()> {
        let expression_nodes = |depth| -> PlanResult<usize> {
            let mut data_type = DataType::Struct(
                vec![
                    Field::new("keep", DataType::Int32, true),
                    Field::new("drop", DataType::Int32, true),
                ]
                .into(),
            );
            for _ in 1..depth {
                data_type = DataType::Struct(vec![Field::new("next", data_type, true)].into());
            }
            let schema = Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
                "payload",
                data_type.clone(),
                true,
            )]))?);
            let mut path = vec![spec::Identifier::from("next"); depth - 1];
            path.push("drop".into());
            let expression = PlanResolver::exclude_struct_fields(
                col("payload"),
                &data_type,
                &[path.as_slice()],
                &schema,
            )?;
            let mut nodes = 0;
            expression.apply(|_| {
                nodes += 1;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(nodes)
        };
        let mut previous = None;
        for depth in [4, 8, 12, 24] {
            let nodes = expression_nodes(depth)?;
            println!("EXCEPT_EXPRESSION_NODES depth={depth} nodes={nodes}");
            if let Some((previous_depth, previous_nodes)) = previous {
                // Raw ancestor prefixes give quadratic growth; check shallow depths before advancing.
                assert!(
                    nodes * previous_depth * previous_depth <= previous_nodes * depth * depth,
                    "depth {previous_depth}->{depth} grew {previous_nodes} nodes to {nodes}"
                );
            }
            previous = Some((depth, nodes));
        }
        Ok(())
    }
}
