//! Spark integral CAST errors keep their condition, cause and evaluation phase.

use std::sync::Arc;

use arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    )
}

fn assert_invalid_integral_cast(error: &DataFusionError) {
    assert!(
        error.to_string().contains("[CAST_INVALID_INPUT]"),
        "{error}"
    );
    assert!(
        matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
            if matches!(cause.as_ref(), ArrowError::CastError(message)
                if message.contains("Cannot cast string 'bad'"))),
        "lost the original Arrow cast cause: {error:?}"
    );
}

#[tokio::test]
async fn integral_cast_results_and_conditions_match_spark() -> TestResult {
    let records = include_str!("fixtures/integral-cast-conditions-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 193);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = spark_results::capture_spark_query_result(&session, case).await;
            let label = format!("{} ANSI={} batch={batch_size}", case["id"], case["ansi"]);
            match actual {
                Ok(result) if result == case["expected"] => {}
                Err(error) if case["expected"]["condition"] == "CAST_INVALID_INPUT" => {
                    let error = error
                        .downcast_ref::<DataFusionError>()
                        .ok_or("missing DataFusion error")?;
                    if !error.to_string().contains("[CAST_INVALID_INPUT]") {
                        differences.push(format!("{label}: {error}"));
                    }
                    assert!(
                        matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
                            if matches!(cause.as_ref(), ArrowError::CastError(_))),
                        "{label}: lost the Arrow cast cause: {error:?}"
                    );
                }
                actual => differences.push(format!(
                    "{label}: expected {}, got {actual:?}",
                    case["expected"]
                )),
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn required_integral_casts_keep_error_phase_and_arrow_cause() -> TestResult {
    for batch_size in [1, 32] {
        let session = session(batch_size);
        let values = [
            Some("outside"),
            Some("7"),
            None,
            Some("bad"),
            Some("outside"),
        ];
        let arrays: [ArrayRef; 3] = [
            Arc::new(StringArray::from(values.to_vec())),
            Arc::new(LargeStringArray::from(values.to_vec())),
            Arc::new(StringViewArray::from(values.to_vec())),
        ];
        for (layout, array) in arrays.into_iter().enumerate() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "s",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema.clone(), vec![array.slice(1, 3)])?;
            let batches = (0..batch.num_rows())
                .step_by(batch_size)
                .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
                .collect();
            let table = format!("inputs_{layout}");
            session.register_table(&table, Arc::new(MemTable::try_new(schema, vec![batches])?))?;
            for kind in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
                let query = plan_query(
                    &session,
                    &format!("SELECT CAST(s AS {kind}) AS r FROM {table}"),
                    &SparkSqlOptions::default(),
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                let error = collect(physical, session.task_ctx())
                    .await
                    .expect_err("a provider CAST must fail during execution");
                assert_invalid_integral_cast(&error);
            }
        }
        for kind in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
            for sql in [
                format!("SELECT CAST('bad' AS {kind}) AS r"),
                format!("SELECT CAST('bad' AS {kind}) AS r WHERE false"),
                format!("SELECT CAST(s AS {kind}) AS r FROM VALUES ('bad') t(s)"),
            ] {
                let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
                let error = query
                    .create_physical_plan(&session)
                    .await
                    .expect_err("constant/local CAST must fail before execution");
                assert_invalid_integral_cast(&error);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn inner_failures_and_native_cast_errors_keep_their_identity() -> TestResult {
    for configured in [false, true] {
        let session = if configured {
            session(1)
        } else {
            SessionContext::new()
        };
        session.register_batch(
            "inputs",
            RecordBatch::try_from_iter([(
                "s",
                Arc::new(StringArray::from(vec!["bad"])) as ArrayRef,
            )])?,
        )?;
        for kind in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
            for sql in [
                format!("SELECT CAST('bad' AS {kind}) AS r"),
                format!("SELECT CAST(s AS {kind}) AS r FROM inputs"),
            ] {
                let error = session
                    .sql(&sql)
                    .await?
                    .collect()
                    .await
                    .expect_err("native CAST must fail");
                assert!(!error.to_string().contains("CAST_INVALID_INPUT"), "{error}");
                assert!(
                    matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
                    if matches!(cause.as_ref(), ArrowError::CastError(_))),
                    "{error:?}"
                );
            }
        }
        if configured {
            for sql in [
                "SELECT CAST(CAST('bad' AS DOUBLE) AS INT) AS r",
                "SELECT CAST(CAST(s AS DOUBLE) AS INT) AS r FROM inputs",
            ] {
                let actual: datafusion::common::Result<_> = async {
                    let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
                    let physical = query.create_physical_plan(&session).await?;
                    collect(physical, session.task_ctx()).await
                }
                .await;
                let error = actual.expect_err("the inner floating CAST must fail");
                let root = error.find_root().to_string();
                assert!(
                    root.contains("[CAST_INVALID_INPUT] Cannot cast string \"bad\" to Float64"),
                    "{error}"
                );
                assert_eq!(
                    error.to_string().matches("CAST_INVALID_INPUT").count(),
                    1,
                    "{error}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn existing_integer_cast_callers_preserve_the_arrow_cause() -> TestResult {
    let session = session(1);
    session.register_batch(
        "inputs",
        RecordBatch::try_from_iter([("s", Arc::new(StringArray::from(vec!["bad"])) as ArrayRef)])?,
    )?;
    let mut differences = Vec::new();
    for (caller, sql) in [
        ("comparison", "SELECT s=7 AS r FROM inputs"),
        (
            "conditional",
            "SELECT COALESCE(CAST(NULL AS INT),s) AS r FROM inputs",
        ),
        (
            "set",
            "SELECT s AS r FROM inputs UNION ALL SELECT CAST(1 AS BIGINT) AS r",
        ),
        (
            "date_day",
            "SELECT DATE_ADD(DATE '2020-01-01',s) AS r FROM inputs",
        ),
    ] {
        let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
        let physical = query.create_physical_plan(&session).await?;
        let error = collect(physical, session.task_ctx())
            .await
            .expect_err("a required provider integer conversion must fail during execution");
        println!(
            "{}",
            json!({"caller": caller, "sql": sql, "phase": "execution",
            "error": error.to_string(), "root": format!("{:?}", error.find_root())})
        );
        if !error.to_string().contains("[CAST_INVALID_INPUT]")
            || !matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
                if matches!(cause.as_ref(), ArrowError::CastError(message)
                    if message.contains("Cannot cast string 'bad'")))
        {
            differences.push(format!("{caller}: {error}"));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
