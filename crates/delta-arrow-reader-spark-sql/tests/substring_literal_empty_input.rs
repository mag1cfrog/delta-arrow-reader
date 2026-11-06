//! Literal substring preparation keeps LIMIT 0 and false-filter boundaries distinct.
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::new_empty_array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn literal_substring_limit_zero_keeps_empty_rows_and_required_false_filter_errors()
-> TestResult {
    for batch_size in [1, 32] {
        for ansi_mode in [true, false] {
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(
                            SessionConfig::new()
                                .with_batch_size(batch_size)
                                .with_target_partitions(2),
                        ),
                )
                .build(),
            );
            // Preserve the original 897 empty provider as a real table with its own Fields.
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                    "role".into(),
                    "floating-integer-order".into(),
                )])),
                Field::new("f", DataType::Float32, false).with_metadata(HashMap::from([
                    ("source".into(), "897-float".into()),
                    ("unit".into(), "binary-floating".into()),
                ])),
                Field::new("d", DataType::Float64, false).with_metadata(HashMap::from([
                    ("source".into(), "897-double".into()),
                    ("unit".into(), "binary-floating".into()),
                ])),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                schema
                    .fields()
                    .iter()
                    .map(|field| new_empty_array(field.data_type()))
                    .collect(),
            )?;
            session.register_table(
                "floating_integer_empty",
                Arc::new(MemTable::try_new(schema, vec![vec![batch], vec![]])?),
            )?;
            for (suffix, requires_error) in [
                ("FROM (SELECT 1 AS k LIMIT 0) t", false),
                ("LIMIT 0", false),
                ("WHERE false", true),
                ("FROM (SELECT 1 AS k) t WHERE false", true),
                ("FROM floating_integer_empty", true),
            ] {
                let sql =
                    format!("SELECT substring('abcdef',CAST('NaN' AS FLOAT),2) AS r {suffix}");
                let query = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        session_timezone: "UTC".into(),
                        ..Default::default()
                    },
                )
                .await?;
                assert_eq!(query.column_names, vec!["r"]);
                let fields = query.logical_plan.schema().fields();
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].data_type(), &DataType::Utf8);
                assert!(fields[0].is_nullable());
                assert!(fields[0].metadata().is_empty());
                let physical = query.create_physical_plan(&session).await;
                if ansi_mode && requires_error {
                    let error = physical.unwrap_err().to_string();
                    assert_eq!(
                        error.matches("[CAST_OVERFLOW]").count(),
                        1,
                        "{sql}: {error}"
                    );
                    assert!(
                        error.ends_with(
                            "Arrow error: Cast error: Can't cast value NaN to type Int32"
                        ),
                        "{sql}: {error}"
                    );
                } else {
                    let physical = physical?;
                    assert_eq!(physical.schema().field(0).name(), "r");
                    assert_eq!(physical.schema().field(0).data_type(), &DataType::Utf8);
                    assert!(physical.schema().field(0).metadata().is_empty());
                    if !requires_error || ansi_mode {
                        assert!(physical.schema().field(0).is_nullable());
                    }
                    let batches = collect(physical, session.task_ctx()).await?;
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        0,
                        "{sql}"
                    );
                }
            }
        }
    }
    Ok(())
}
