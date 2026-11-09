//! DATEDIFF keeps Spark's INT result fields and day counts.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Date32Type, Int32Type, Int64Type};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::register_recorded_inputs;

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, validate_batch};

fn date_diff_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/date-diff/date-diff-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 216);
    Ok(records)
}

/// The Arrow type family DAR uses for a recorded Spark type.
fn spark_family(spark_type: &Value) -> TestResult<&'static str> {
    Ok(
        match spark_type.as_str().ok_or("expected a primitive type")? {
            "integer" => "int32",
            "long" => "int64",
            "date" => "date32",
            other => return Err(format!("unexpected Spark type {other}").into()),
        },
    )
}

fn dar_family(data_type: &DataType) -> TestResult<&'static str> {
    Ok(match data_type {
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::Date32 => "date32",
        other => return Err(format!("unexpected DAR type {other}").into()),
    })
}

fn spark_fields(schema: &Value) -> TestResult<Value> {
    schema["fields"]
        .as_array()
        .ok_or("missing recorded fields")?
        .iter()
        .map(|field| {
            Ok(
                json!({"name": field["name"], "type": spark_family(&field["type"])?,
                "nullable": field["nullable"], "metadata": field["metadata"]}),
            )
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

fn dar_fields(fields: &arrow::datatypes::Fields, names: Option<&[String]>) -> TestResult<Value> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            Ok(json!({"name": name, "type": dar_family(field.data_type())?,
                "nullable": field.is_nullable(), "metadata": field.metadata()}))
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        other => return Err(format!("unexpected result type {other}").into()),
    })
}

/// Observe analyzed (public names), optimized and executed fields, typed rows and the
/// failing phase in the record's session time zone.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    let mut phase = "resolve";
    let result: TestResult<Vec<Vec<Value>>> = async {
        let options = SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: case["timezone"].as_str().ok_or("missing time zone")?.into(),
            ..SparkSqlOptions::default()
        };
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options,
        )
        .await?;
        observation["analyzed"] = dar_fields(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        )?;
        phase = "optimized";
        let mut state = session.state();
        let extensions = &mut state.config_mut().options_mut().extensions;
        extensions.insert(SparkSqlExpressionSemantics::default());
        extensions.insert(PreserveAggregateStatisticsSchema::default());
        let optimized = state.optimize(&query.logical_plan)?;
        observation["optimized"] =
            dar_fields(optimized.schema().fields(), Some(&query.column_names))?;
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None)?;
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                );
            }
        }
        rows.sort_by_key(|row| json!(row).to_string());
        Ok(rows)
    }
    .await;
    match result {
        Ok(rows) => observation["rows"] = json!(rows),
        Err(error) => {
            observation["error_phase"] = json!(phase);
            observation["error"] = json!(error.to_string());
        }
    }
    observation
}

/// Compare every recorded stage and the typed rows. The corpus records no errors.
fn compare(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differences = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if observation[stage] != spark_fields(&case["schema_stages"][stage])? {
            differences.push(stage);
        }
    }
    if case["expected"]["status"] != "ok" || observation["rows"] != case["expected"]["rows"] {
        differences.push("values");
    }
    Ok(differences)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date_diff_matches_spark() -> TestResult {
    let records = date_diff_references()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
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
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let observation = observe(&session, case).await;
            let differing = compare(case, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "batch": batch_size, "sql": case["sql"], "differing": differing,
                    "expected": case["expected"], "schema_stages": case["schema_stages"],
                    "observed": observation,
                }));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    Ok(())
}

/// Spark configuration must not change native DataFusion date arithmetic. Native SQL has no
/// DATEDIFF, so it subtracts dates directly.
#[tokio::test]
async fn native_date_arithmetic_is_unchanged_by_spark_configuration() -> TestResult {
    let sql = "SELECT DATE '2024-03-01' - DATE '2024-02-28' AS a, \
        DATE '2024-02-28' + INTERVAL '1' DAY AS b";
    let plain = SessionContext::new().sql(sql).await?.collect().await?;
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    )
    .sql(sql)
    .await?
    .collect()
    .await?;
    assert_eq!(plain, configured);
    assert_eq!(plain[0].num_rows(), 1);
    Ok(())
}
