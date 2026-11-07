//! Literal NULL length discards unused SUBSTRING work before local preparation.

use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::util::display::array_value_to_string;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(
    dead_code,
    reason = "This test reuses the original sliced IEEE providers."
)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code, reason = "This test reuses the shared input result type.")]
#[path = "support/spark_results.rs"]
mod spark_results;
#[allow(
    dead_code,
    reason = "This test reuses the original provider registration."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::TestResult;

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

async fn null_result(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
    expected_type: &DataType,
    expected_rows: usize,
) -> TestResult<Value> {
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        },
    )
    .await?;
    let index = query.column_names.len() - 1;
    let name = query.column_names[index].clone();
    let declared = Arc::clone(&query.logical_plan.schema().fields()[index]);
    assert_eq!(declared.data_type(), expected_type, "{sql}");
    assert!(declared.is_nullable(), "{sql}");
    assert!(declared.metadata().is_empty(), "{sql}");
    let analyzed_schema = string_binary::capture_schema(
        query.logical_plan.schema().fields(),
        Some(&query.column_names),
    )?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    let field = schema.field(index);
    assert_eq!(field.name(), &name, "{sql}");
    assert_eq!(field.data_type(), declared.data_type(), "{sql}");
    assert_eq!(field.is_nullable(), declared.is_nullable(), "{sql}");
    assert_eq!(field.metadata(), declared.metadata(), "{sql}");
    let batches = collect(physical, session.task_ctx()).await?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        expected_rows,
        "{sql}"
    );
    let mut rows = Vec::new();
    for batch in batches {
        assert_eq!(batch.schema().as_ref(), schema.as_ref(), "{sql}");
        assert_eq!(batch.column(index).null_count(), batch.num_rows(), "{sql}");
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for column in batch.columns() {
                values.push(if column.is_null(row) {
                    Value::Null
                } else {
                    Value::String(array_value_to_string(column.as_ref(), row)?)
                });
            }
            rows.push(values);
        }
    }
    Ok(json!({
        "analyzed_schema":analyzed_schema,
        "physical_schema":string_binary::capture_schema(schema.fields(), None)?,
        "rows":rows,
    }))
}

#[tokio::test]
async fn substring_literal_null_length_discards_unused_errors_and_keeps_parent_priority()
-> TestResult {
    for batch_size in [1, 32] {
        let session = session(batch_size);
        for ansi_mode in [true, false] {
            for (sql, expected_type) in [
                (
                    "SELECT substring('abcdef',CAST('NaN' AS FLOAT),CAST(NULL AS INT)) AS r",
                    DataType::Utf8,
                ),
                (
                    "SELECT substring('abcdef',CAST('Infinity' AS DOUBLE),CAST(NULL AS INT)) AS r",
                    DataType::Utf8,
                ),
                (
                    "SELECT substring(X'616263646566',CAST('NaN' AS FLOAT),CAST(NULL AS INT)) AS r",
                    DataType::Binary,
                ),
                (
                    "SELECT substring(X'616263646566',CAST('Infinity' AS DOUBLE),CAST(NULL AS INT)) AS r",
                    DataType::Binary,
                ),
                (
                    "SELECT substring(CAST(1/0 AS STRING),CAST('NaN' AS FLOAT),CAST(NULL AS INT)) AS r",
                    DataType::Utf8,
                ),
                (
                    "SELECT substring('abcdef',CAST(1/0 AS INT),CAST(NULL AS INT)) AS r",
                    DataType::Utf8,
                ),
            ] {
                for length_type in ["INT", "STRING"] {
                    let sql =
                        sql.replace("CAST(NULL AS INT)", &format!("CAST(NULL AS {length_type})"));
                    null_result(&session, &sql, ansi_mode, &expected_type, 1).await?;
                }
            }
            for sql in [
                "SELECT concat(substring('abcdef',CAST('NaN' AS FLOAT),CAST(NULL AS INT)),CAST(1/0 AS STRING)) AS r",
                "SELECT concat(substring('abcdef',CAST('NaN' AS FLOAT),CAST(NULL AS STRING)),CAST(1/0 AS STRING)) AS r",
            ] {
                if ansi_mode {
                    let query = plan_query(
                        &session,
                        sql,
                        &SparkSqlOptions {
                            ansi_mode,
                            ..Default::default()
                        },
                    )
                    .await?;
                    let error = query.create_physical_plan(&session).await.unwrap_err();
                    assert!(
                        matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
                        if matches!(cause.as_ref(), ArrowError::DivideByZero)),
                        "{error:?}"
                    );
                } else {
                    null_result(&session, sql, ansi_mode, &DataType::Utf8, 1).await?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn substring_coerced_null_length_matches_all_recorded_spark_results() -> TestResult {
    let fixture: Value = serde_json::from_str(include_str!(
        "fixtures/substring-coerced-null-length-spark.json"
    ))?;
    let inputs = &fixture["input_manifest"];
    let providers = inputs["providers"]
        .as_array()
        .ok_or("missing original providers")?
        .iter()
        .map(|provider| {
            json!({
                "name":provider["name"],
                "schema":{"type":"struct","fields":provider["fields"]},
                "rows":provider["rows"],
                "native_slice":provider["native_slice"],
            })
        })
        .collect::<Vec<_>>();
    let strings = providers
        .iter()
        .filter(|provider| {
            provider["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("substring_"))
        })
        .cloned()
        .collect::<Vec<_>>();
    let floats = providers
        .iter()
        .filter(|provider| {
            provider["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("floating_"))
        })
        .cloned()
        .collect::<Vec<_>>();
    let strings = json!({"registered_inputs":strings});
    let floats = json!({"inputs":floats,"native":inputs["native"]});
    let references = fixture["observations"]
        .as_array()
        .ok_or("missing Spark observations")?;
    assert_eq!(references.len(), 8);
    let mut seen = std::collections::HashSet::new();
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            string_binary::register_inputs(&session, &strings, representation, batch_size)?;
            spark_inputs::register_recorded_inputs_with_slices(&session, &floats, batch_size)?;
            for reference in references {
                let id = reference["id"].as_str().ok_or("missing reference ID")?;
                let ansi = reference["ansi"].as_bool().ok_or("missing ANSI mode")?;
                let sql = reference["sql"].as_str().ok_or("missing reference SQL")?;
                let input = inputs["cases"]
                    .as_array()
                    .ok_or("missing original cases")?
                    .iter()
                    .find(|case| case["id"] == id && case["ansi"] == ansi)
                    .ok_or("missing original input")?;
                assert_eq!(input["sql"], sql);
                assert!(seen.insert((id, ansi, representation, batch_size)));
                let expected = &reference["actual"];
                assert_eq!(expected["status"], "ok");
                let source_type = match id {
                    "n1055_string_null_length_literal_string" => DataType::Utf8,
                    "n1055_string_null_length_literal_binary" => DataType::Binary,
                    "n1055_string_null_length_column_string"
                    | "n1055_string_null_length_column_binary" => {
                        let source = if id.ends_with("_string") { "s" } else { "b" };
                        session
                            .table_provider("substring_text")
                            .await?
                            .schema()
                            .field_with_name(source)?
                            .data_type()
                            .clone()
                    }
                    _ => return Err(format!("unexpected reference {id}").into()),
                };
                let rows = expected["rows"].as_array().ok_or("missing Spark rows")?;
                let actual = null_result(&session, sql, ansi, &source_type, rows.len()).await?;
                for key in ["analyzed_schema", "physical_schema", "rows"] {
                    assert_eq!(
                        actual[key], expected[key],
                        "{id}, ANSI={ansi}, representation={representation}, batch={batch_size}, {key}"
                    );
                }
            }
        }
    }
    assert_eq!(seen.len(), 48);
    Ok(())
}

#[tokio::test]
async fn substring_coerced_null_length_keeps_argument_type_validation() -> TestResult {
    // Native type-admission controls, without new Spark observation credit.
    let session = session(32);
    for ansi_mode in [true, false] {
        for (sql, parameter) in [
            (
                "SELECT substring('abcdef',array(1),CAST(NULL AS STRING)) AS r",
                "substring position",
            ),
            (
                "SELECT substring('abcdef',1,CAST(NULL AS BINARY)) AS r",
                "substring length",
            ),
        ] {
            let error = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
            assert!(
                error.to_string().contains(&format!(
                    "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] {parameter} requires INT"
                )),
                "{sql}: {error:?}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn substring_literal_null_length_preserves_real_column_cast_checks() -> TestResult {
    let strings: Value = serde_json::from_str(
        include_str!("fixtures/substring-arguments-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing string inputs")?,
    )?;
    let floats: Value = serde_json::from_str(
        include_str!("fixtures/floating-integer-values/spark.jsonl")
            .lines()
            .next()
            .ok_or("missing IEEE inputs")?,
    )?;
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            string_binary::register_inputs(&session, &strings, representation, batch_size)?;
            spark_inputs::register_recorded_inputs_with_slices(&session, &floats, batch_size)?;
            for ansi_mode in [true, false] {
                for source in ["s", "b"] {
                    let provider = session.table_provider("substring_text").await?;
                    let source_type = provider
                        .schema()
                        .field_with_name(source)?
                        .data_type()
                        .clone();
                    let sql = format!(
                        "SELECT id,substring({source},CAST('NaN' AS FLOAT),CAST(NULL AS INT)) AS r FROM substring_text ORDER BY id"
                    );
                    null_result(&session, &sql, ansi_mode, &source_type, 12).await?;
                }
                for sql in [
                    "SELECT id,substring(s,CAST('NaN' AS FLOAT),n) AS r FROM substring_text WHERE id=12 ORDER BY id",
                    "SELECT id,substring(b,CAST('NaN' AS FLOAT),n) AS r FROM substring_text WHERE id=12 ORDER BY id",
                    "SELECT id,substring(s,CAST('NaN' AS FLOAT),n) AS r FROM substring_text WHERE id=5 ORDER BY id",
                    "SELECT id,substring(s,CAST('NaN' AS FLOAT),n) AS r FROM substring_text WHERE id=1 ORDER BY id",
                    "SELECT id,substring('abcdef',CAST('NaN' AS FLOAT),f) AS r FROM floating_integer_all_null_sliced ORDER BY id",
                    "SELECT id,substring(X'616263646566',CAST('NaN' AS FLOAT),f) AS r FROM floating_integer_all_null_sliced ORDER BY id",
                ] {
                    let query = plan_query(
                        &session,
                        sql,
                        &SparkSqlOptions {
                            ansi_mode,
                            ..Default::default()
                        },
                    )
                    .await?;
                    let physical = query.create_physical_plan(&session).await;
                    if ansi_mode {
                        let error = physical.unwrap_err();
                        assert_eq!(
                            error.to_string().matches("[CAST_OVERFLOW]").count(),
                            1,
                            "{sql}: {error:?}"
                        );
                        assert!(
                            matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
                            if matches!(cause.as_ref(), ArrowError::CastError(message)
                                if message == "Can't cast value NaN to type Int32")),
                            "{sql}: {error:?}"
                        );
                    } else {
                        collect(physical?, session.task_ctx()).await?;
                    }
                }
            }
        }
    }
    Ok(())
}
