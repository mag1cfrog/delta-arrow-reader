//! Plan read-only queries and metadata over the host's registered providers.

use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::compute::kernels::regexp::regexp_is_match_scalar;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue, internal_err, plan_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, Values};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use sail_plan::formatter::SparkPlanFormatter;
use sail_plan::resolver::PlanResolver;
use sail_sql_analyzer::statement::ReadOnlyStatement;

use crate::{SparkSqlOptions, SparkSqlQuery};

pub(crate) async fn plan_statement(
    session: &SessionContext,
    statement: ReadOnlyStatement,
    options: &SparkSqlOptions,
) -> Result<SparkSqlQuery> {
    let resolver = if matches!(&statement, ReadOnlyStatement::Query(_)) {
        PlanResolver::new(session, Arc::new(options.clone()))
    } else {
        PlanResolver::new_for_metadata(session, Arc::new(options.clone()))
    };
    let resolve = async |query| {
        let named = resolver
            .resolve_named_plan(query)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok::<_, DataFusionError>(SparkSqlQuery {
            logical_plan: named.plan,
            column_names: named.fields,
        })
    };
    match statement {
        ReadOnlyStatement::Query(query) => resolve(query).await,
        ReadOnlyStatement::Describe(query) => {
            let query = resolve(query).await?;
            let rows = query
                .column_names
                .iter()
                .zip(query.logical_plan.schema().fields())
                .map(|(name, field)| {
                    Ok(vec![
                        ScalarValue::Utf8(Some(name.clone())),
                        ScalarValue::Utf8(Some(
                            SparkPlanFormatter.data_type_to_simple_string(field.data_type())?,
                        )),
                        ScalarValue::Utf8(field.metadata().get("comment").cloned()),
                    ])
                })
                .collect::<Result<Vec<_>>>()?;
            query_from_rows(
                &[
                    ("col_name", DataType::Utf8),
                    ("data_type", DataType::Utf8),
                    ("comment", DataType::Utf8),
                ],
                rows,
            )
        }
        ReadOnlyStatement::ShowColumns(query) => {
            let query = resolve(query).await?;
            query_from_rows(
                &[("col_name", DataType::Utf8)],
                query
                    .column_names
                    .into_iter()
                    .map(|name| vec![ScalarValue::Utf8(Some(name))])
                    .collect(),
            )
        }
        ReadOnlyStatement::Explain(query) => {
            let query = resolve(query).await?;
            let physical = query.create_physical_plan(session).await?;
            let text = format!(
                "Logical Plan:\n{}\n\nPhysical Plan:\n{}",
                query.logical_plan.display_indent_schema(),
                displayable(physical.as_ref()).indent(true)
            );
            query_from_rows(
                &[("plan", DataType::Utf8)],
                vec![vec![ScalarValue::Utf8(Some(text))]],
            )
        }
        ReadOnlyStatement::ShowTables { schema, pattern } => {
            let settings = session.state().config_options().catalog.clone();
            let (catalog_name, schema_name) = match schema.as_deref() {
                None => (settings.default_catalog, settings.default_schema),
                Some([schema]) => (settings.default_catalog, schema.clone()),
                Some([catalog, schema]) => (catalog.clone(), schema.clone()),
                _ => return plan_err!("SHOW TABLES requires a schema or catalog.schema name"),
            };
            let Some(catalog) = session.catalog(&catalog_name) else {
                return plan_err!("catalog does not exist: {catalog_name}");
            };
            if catalog.schema(&schema_name).is_none() {
                return plan_err!("schema does not exist: {schema_name}");
            }
            let mut entries = vec![];
            for namespace in catalog.schema_names() {
                let Some(schema) = catalog.schema(&namespace) else {
                    continue;
                };
                for name in schema.table_names() {
                    let Some(table) = schema.table(&name).await? else {
                        continue;
                    };
                    let temporary = table.table_type() == TableType::Temporary;
                    if namespace == schema_name || temporary {
                        entries.push((
                            if temporary {
                                String::new()
                            } else {
                                namespace.clone()
                            },
                            name,
                            temporary,
                        ));
                    }
                }
            }
            entries.sort();
            let rows = entries
                .into_iter()
                .map(|(namespace, name, temporary)| {
                    vec![
                        ScalarValue::Utf8(Some(namespace)),
                        ScalarValue::Utf8(Some(name)),
                        ScalarValue::Boolean(Some(temporary)),
                    ]
                })
                .collect();
            plan_listing(
                &[
                    ("namespace", DataType::Utf8),
                    ("tableName", DataType::Utf8),
                    ("isTemporary", DataType::Boolean),
                ],
                rows,
                1,
                pattern,
            )
        }
        ReadOnlyStatement::ShowSchemas { pattern } => {
            let name = session
                .state()
                .config_options()
                .catalog
                .default_catalog
                .clone();
            let Some(catalog) = session.catalog(&name) else {
                return plan_err!("catalog does not exist: {name}");
            };
            let mut names = catalog.schema_names();
            names.sort();
            let rows = names
                .into_iter()
                .map(|name| vec![ScalarValue::Utf8(Some(name))])
                .collect();
            plan_listing(&[("namespace", DataType::Utf8)], rows, 0, pattern)
        }
    }
}

fn query_from_rows(
    columns: &[(&str, DataType)],
    rows: Vec<Vec<ScalarValue>>,
) -> Result<SparkSqlQuery> {
    let fields = columns
        .iter()
        .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(DFSchema::try_from(Schema::new(fields))?);
    Ok(SparkSqlQuery {
        logical_plan: if rows.is_empty() {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            })
        } else {
            LogicalPlan::Values(Values {
                schema,
                values: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|value| Expr::Literal(value, None))
                            .collect()
                    })
                    .collect(),
            })
        },
        column_names: columns
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect(),
    })
}

fn plan_listing(
    columns: &[(&str, DataType)],
    mut rows: Vec<Vec<ScalarValue>>,
    name_column: usize,
    pattern: Option<String>,
) -> Result<SparkSqlQuery> {
    if let Some(pattern) = pattern {
        let names = rows
            .iter()
            .map(|row| match &row[name_column] {
                ScalarValue::Utf8(Some(value)) => Ok(value.as_str()),
                _ => internal_err!("metadata pattern column must contain strings"),
            })
            .collect::<Result<Vec<_>>>()?;
        let names = StringArray::from(names);
        let mut keep = vec![false; rows.len()];
        // Spark ignores invalid alternatives while matching each remaining fragment in full.
        for fragment in pattern.trim().split('|') {
            let fragment = format!("^(?:{})$", fragment.replace('*', ".*"));
            if let Ok(matches) = regexp_is_match_scalar(&names, &fragment, Some("i")) {
                for (index, keep) in keep.iter_mut().enumerate() {
                    *keep |= matches.value(index);
                }
            }
        }
        rows = rows
            .into_iter()
            .zip(keep)
            .filter_map(|(row, keep)| keep.then_some(row))
            .collect();
    }
    query_from_rows(columns, rows)
}
