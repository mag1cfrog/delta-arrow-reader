//! Native async position validation and execution controls, without Spark UDF oracle credit.
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::json;

#[path = "../vendor/sail/crates/sail-plan/src/test_utils/regexp_literal_producer.rs"]
mod literal_producer;
use literal_producer::{Kind, producer};

#[expect(
    dead_code,
    reason = "This target uses the complete failed-query observer."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation};

fn session() -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(32)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    )
}

#[tokio::test]
async fn regexp_replace_rejects_async_positions_at_resolve() -> TestResult {
    for ansi in [true, false] {
        for position in [
            "literal_async_fold(1)",
            "CAST(literal_async_fold(1) AS BIGINT)",
            "literal_async_fold('1')",
            "literal_counted_producer(literal_async_fold(1))",
        ] {
            let session = session();
            let (asynchronous, counts) = producer(Kind::AsyncFold);
            let (immutable, immutable_counts) = producer(Kind::ImmutableFold);
            assert!(asynchronous.as_async().is_some());
            session.register_udf(asynchronous);
            session.register_udf(immutable);
            let sql = format!("SELECT regexp_replace('a','(a)','x',{position}) AS r");
            let actual = capture_observation(&session, &json!({"sql":sql,"ansi":ansi})).await?;
            assert_eq!(
                actual,
                json!({
                    "error":"resolve: External error: invalid argument: [DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] regexp_replace position must be a foldable INT expression",
                    "error_stage":"resolve",
                    "analyzed_schema":null,"analyzed_arrow_schema":null,
                    "physical_schema":null,"physical_arrow_schema":null,
                }),
                "{sql} ANSI={ansi}"
            );
            // These shared counters observe inner entries, not native wrapper entries.
            assert_eq!(counts.snapshot(), json!({"calls":0,"events":[]}));
            assert_eq!(immutable_counts.snapshot(), json!({"calls":0,"events":[]}));
        }
    }
    Ok(())
}

#[tokio::test]
async fn regexp_replace_preserves_async_string_arguments_and_native_dispatch() -> TestResult {
    for ansi_mode in [true, false] {
        for (native, sql, argument, expected) in [
            (
                false,
                "SELECT regexp_replace(literal_async_fold('a'),'(a)','x') AS r",
                "a",
                "x",
            ),
            (
                false,
                "SELECT regexp_replace('a',literal_async_fold('(a)'),'x') AS r",
                "(a)",
                "x",
            ),
            (
                false,
                "SELECT regexp_replace('a','(a)',literal_async_fold('x')) AS r",
                "x",
                "x",
            ),
            (true, "SELECT literal_async_fold('a') AS r", "a", "a"),
        ] {
            let session = session();
            let (asynchronous, counts) = producer(Kind::AsyncFold);
            assert!(asynchronous.as_async().is_some());
            session.register_udf(asynchronous);
            let physical = if native {
                let frame = session.sql(sql).await?;
                assert_eq!(
                    frame.schema().inner().as_ref(),
                    &Schema::new(vec![Field::new("r", DataType::Utf8, false)])
                );
                assert_eq!(counts.snapshot(), json!({"calls":0,"events":[]}));
                counts.phase.store(1, Ordering::SeqCst);
                frame.create_physical_plan().await?
            } else {
                let query = plan_query(
                    &session,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?;
                assert_eq!(query.column_names, ["r"]);
                assert_eq!(
                    query.logical_plan.schema().inner().as_ref(),
                    &Schema::new(vec![Field::new("#0", DataType::Utf8, false)])
                );
                assert_eq!(counts.snapshot(), json!({"calls":0,"events":[]}));
                counts.phase.store(1, Ordering::SeqCst);
                query.create_physical_plan(&session).await?
            };
            let schema = Arc::new(Schema::new(vec![Field::new("r", DataType::Utf8, false)]));
            assert_eq!(physical.schema(), schema);
            assert_eq!(counts.snapshot(), json!({"calls":0,"events":[]}));
            counts.phase.store(2, Ordering::SeqCst);
            let batches = datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
            assert_eq!(batches.len(), 1, "{sql} ANSI={ansi_mode}");
            assert_eq!(batches[0].schema(), schema);
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(
                array_value_to_string(batches[0].column(0).as_ref(), 0)?,
                expected
            );
            let field = Field::new("lit", DataType::Utf8, false);
            assert_eq!(
                counts.snapshot(),
                json!({"calls":1,"events":[{
                    "phase":2,"number_rows":1,
                    "arguments":format!("[Scalar(Utf8({argument:?}))]"),
                    "arg_fields":format!("{:?}", [Arc::new(field.clone())]),
                    "return_field":format!("{field:?}"),
                }]}),
                "{sql} ANSI={ansi_mode}"
            );
        }
    }
    Ok(())
}
