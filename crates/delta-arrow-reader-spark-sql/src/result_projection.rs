//! Give the top projection its public result names when it can keep the same fields.

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;

pub(super) fn rename_result_columns(
    plan: Arc<dyn ExecutionPlan>,
    names: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let original_schema = plan.schema();
    if original_schema.fields().len() == names.len()
        && let Some(projection) = plan.downcast_ref::<ProjectionExec>()
        && !projection.preserves_row_error_prefix()
        && let Ok(renamed) = ProjectionExec::try_new(
            projection
                .expr()
                .iter()
                .zip(names)
                .map(|(expression, name)| (Arc::clone(&expression.expr), name.clone())),
            Arc::clone(projection.input()),
        )
    {
        let schema = renamed.schema();
        if schema.metadata() == original_schema.metadata()
            && original_schema
                .fields()
                .iter()
                .zip(schema.fields())
                .all(|(original, renamed)| {
                    original.as_ref().clone().with_name(renamed.name()) == **renamed
                })
        {
            return Ok(Arc::new(renamed));
        }
    }
    rename_physical_plan(plan, names)
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::dataframe::DataFrame;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;

    use super::*;
    use crate::{SparkSqlOptions, configure_spark_sql, plan_query};

    #[tokio::test]
    async fn result_aliases_preserve_rows_fields_and_failures() -> Result<()> {
        let session = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features())
                .with_config(SessionConfig::new().with_target_partitions(1))
                .build(),
        );
        let schema = Arc::new(Schema::new_with_metadata(
            [
                Field::new("id", DataType::Int64, false)
                    .with_metadata([("tag".into(), "source".into())].into()),
                Field::new("s", DataType::Utf8, true),
                Field::new("p", DataType::Int32, true),
                Field::new("d", DataType::Float64, true),
            ],
            [("table".into(), "result control".into())].into(),
        ));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![-1, 0, 1, 2])),
            Arc::new(StringArray::from(vec![
                Some("itemé"),
                Some(""),
                Some("🙂"),
                None,
            ])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), Some(255), None])),
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-1.0),
                Some(2.0),
                None,
            ])),
        ];
        let batch = RecordBatch::try_new(schema, columns)?;
        session.register_batch("result_inputs", batch.clone())?;
        session.register_batch("empty_result_inputs", batch.slice(0, 0))?;
        let selects = [
            "id AS v, replace(s, 'item', 'row') AS v, repeat(s, p) AS copies, right(s, p) AS tail, char(p) AS ch",
            "id, row_number() OVER (ORDER BY id) AS position, stddev_samp(d) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS dispersion",
            "CAST(s AS INT) AS converted, id",
        ];
        for ansi_mode in [false, true] {
            let options = SparkSqlOptions {
                ansi_mode,
                ..Default::default()
            };
            for table in ["result_inputs", "empty_result_inputs"] {
                for select in selects {
                    let query =
                        plan_query(&session, &format!("SELECT {select} FROM {table}"), &options)
                            .await?;
                    let mut state = session.state();
                    let extensions = &mut state.config_mut().options_mut().extensions;
                    extensions.insert(SparkSqlExpressionSemantics::default());
                    extensions.insert(PreserveAggregateStatisticsSchema::default());
                    let input = DataFrame::new(state, query.logical_plan)
                        .create_physical_plan()
                        .await?;
                    let expected = rename_physical_plan(input.clone(), &query.column_names)?;
                    let actual = rename_result_columns(input.clone(), &query.column_names)?;
                    assert_eq!(actual.schema(), expected.schema());
                    if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
                        if projection.preserves_row_error_prefix() {
                            assert!(Arc::ptr_eq(actual.children()[0], &input));
                        } else {
                            assert!(Arc::ptr_eq(actual.children()[0], projection.input()));
                        }
                    }
                    let actual = collect(actual, session.task_ctx()).await;
                    let expected = collect(expected, session.task_ctx()).await;
                    match (actual, expected) {
                        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
                        (Err(actual), Err(expected)) => {
                            assert_eq!(actual.to_string(), expected.to_string())
                        }
                        (actual, expected) => {
                            return datafusion::common::exec_err!(
                                "{select} from {table}, ANSI={ansi_mode}: {actual:?} differs from {expected:?}"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
