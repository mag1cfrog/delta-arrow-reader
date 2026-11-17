//! DATE plus LTZ TIMESTAMP coercion in IN, ARRAY and VALUES (#1255).

use std::sync::Arc;

use arrow::array::{Array, AsArray, Date32Array, TimestampNanosecondArray};
use arrow::datatypes::{
    DataType, Date32Type, TimeUnit, TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, validate_batch};

fn references() -> TestResult<(Value, Vec<Value>)> {
    let request: Value = serde_json::from_str(include_str!(
        "fixtures/date-timestamp-combination/requests.json"
    ))?;
    let records = include_str!("fixtures/date-timestamp-combination/spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(
        records[0]["effective_settings"]["spark.sql.timestampType"],
        "TIMESTAMP_LTZ"
    );
    let corpora = request["corpora"].as_array().ok_or("missing corpora")?;
    let definitions = corpora
        .iter()
        .flat_map(|corpus| corpus["cases"].as_array().into_iter().flatten())
        .collect::<Vec<_>>();
    assert_eq!(definitions.len(), 20);
    assert_eq!(records.len() - 1, definitions.len());
    for (case, definition) in records[1..].iter().zip(definitions) {
        for key in ["corpus", "id", "sql", "ansi", "timezone", "ordered"] {
            assert_eq!(case[key], definition[key], "source identity {key}");
        }
        assert_eq!(case["expected"]["status"], "ok");
    }
    let metadata = corpora[0]["metadata"].clone();
    for corpus in corpora {
        assert_eq!(corpus["metadata"], metadata);
    }
    Ok((metadata, records.into_iter().skip(1).collect()))
}

fn context(batch_size: usize) -> SessionContext {
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

fn family(data_type: &DataType, timezone: &str) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Int32 => json!("integer"),
        DataType::Int64 => json!("long"),
        DataType::Boolean => json!("boolean"),
        DataType::Date32 => json!("date"),
        DataType::Timestamp(TimeUnit::Microsecond, None) => json!("timestamp_ntz"),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == timezone => {
            json!("timestamp")
        }
        DataType::Timestamp(unit, zone) => {
            json!({"timestamp_unit":format!("{unit:?}"),"zone":zone})
        }
        DataType::List(item) => {
            json!({"type":"array","elementType":family(item.data_type(), timezone)?})
        }
        other => return Err(format!("unexpected output type {other}").into()),
    })
}

fn encode(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        // The baseline's nanosecond output is encoded as micros for row comparison, while
        // family() still distinguishes its wrong executed type. Exact ns guards read ticks.
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            json!((column.as_primitive::<TimestampNanosecondType>().value(row) / 1000).to_string())
        }
        DataType::List(_) => {
            let values = column.as_list::<i32>().value(row);
            json!(
                (0..values.len())
                    .map(|index| encode(values.as_ref(), index))
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
        _ => json!(arrow::util::display::array_value_to_string(column, row)?),
    })
}

async fn observe_plan(
    session: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    timezone: &str,
) -> TestResult<Value> {
    let schema = plan.schema();
    let types = schema
        .fields()
        .iter()
        .map(|field| family(field.data_type(), timezone))
        .collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        validate_batch(schema.as_ref(), &batch)?;
        for row in 0..batch.num_rows() {
            rows.push(json!(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?
            ));
        }
    }
    rows.sort_by_key(Value::to_string);
    Ok(json!({"rows":rows,"types":types}))
}

async fn observe(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let timezone = case["timezone"].as_str().ok_or("missing timezone")?;
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            session_timezone: timezone.into(),
            ..Default::default()
        },
    )
    .await?;
    observe_plan(
        session,
        query.create_physical_plan(session).await?,
        timezone,
    )
    .await
}

#[tokio::test]
async fn date_timestamp_combinations_match_spark() -> TestResult {
    let (metadata, cases) = references()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = context(batch_size);
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session, &metadata, batch_size, 1,
        )?;
        for case in &cases {
            let actual = match observe(&session, case).await {
                Ok(actual) => actual,
                Err(error) => json!({"error":error.to_string()}),
            };
            let mut expected_types = case["schema_stages"]["executed"]["fields"]
                .as_array()
                .ok_or("missing fields")?
                .iter()
                .map(|field| field["type"].clone())
                .collect::<Vec<_>>();
            for data_type in &mut expected_types {
                spark_results::remove_nullability_and_metadata(data_type);
            }
            println!(
                "date_timestamp_observation={}",
                json!({"id":case["id"],"timezone":case["timezone"],
                "ansi":case["ansi"],"batch_size":batch_size,"actual":actual})
            );
            if actual["rows"] != case["expected"]["rows"]
                || actual["types"] != json!(expected_types)
            {
                differences.push(json!({"id":case["id"],"timezone":case["timezone"],"ansi":case["ansi"],
                    "batch_size":batch_size,"actual":actual,"expected_rows":case["expected"]["rows"],"expected_types":expected_types}));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "DATE/TIMESTAMP differences: {differences:#?}"
    );
    Ok(())
}

#[tokio::test]
async fn native_date_timestamp_membership_keeps_utc_midnight() -> TestResult {
    let (metadata, cases) = references()?;
    let session = context(32);
    spark_inputs::register_recorded_inputs_with_partition_count(&session, &metadata, 32, 1)?;
    let expected = cases
        .iter()
        .find(|case| case["timezone"] == "UTC" && case["id"] == "in_columns")
        .ok_or("missing UTC reference")?;
    let plan = session
        .sql("SELECT k, d IN(t) AS r FROM inputs")
        .await?
        .create_physical_plan()
        .await?;
    let actual = observe_plan(&session, plan, "UTC").await?;
    let rows = expected["expected"]["rows"]
        .as_array()
        .ok_or("missing rows")?
        .iter()
        .map(|row| json!([row[0], row[1]]))
        .collect::<Vec<_>>();
    assert_eq!(actual["rows"], json!(rows));
    assert_eq!(actual["types"], json!(["integer", "boolean"]));
    Ok(())
}

#[tokio::test]
async fn nanosecond_inputs_keep_their_existing_ticks() -> TestResult {
    let session = context(1);
    let dates: Arc<dyn Array> = Arc::new(Date32Array::from(vec![Some(0), Some(0), None]));
    let nanos: Arc<dyn Array> =
        Arc::new(TimestampNanosecondArray::from(vec![Some(1), Some(0), None]).with_timezone("UTC"));
    session.register_batch(
        "precise",
        RecordBatch::try_from_iter([("d", dates), ("t", nanos)])?,
    )?;
    for ansi_mode in [false, true] {
        let query = plan_query(
            &session,
            "SELECT d IN(t) AS r, ARRAY(d,t) AS a FROM precise",
            &SparkSqlOptions {
                ansi_mode,
                ..Default::default()
            },
        )
        .await?;
        let batches = collect(
            query.create_physical_plan(&session).await?,
            session.task_ctx(),
        )
        .await?;
        let mut predicates = Vec::new();
        let mut arrays = Vec::new();
        for batch in batches {
            predicates.extend(batch.column(0).as_boolean().iter());
            let array = batch.column(1).as_list::<i32>();
            assert_eq!(
                array.value_type(),
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            );
            for row in 0..array.len() {
                arrays.push(
                    array
                        .value(row)
                        .as_primitive::<TimestampNanosecondType>()
                        .iter()
                        .collect::<Vec<_>>(),
                );
            }
        }
        assert_eq!(predicates, vec![Some(false), Some(true), None]);
        assert_eq!(
            arrays,
            vec![
                vec![Some(0), Some(1)],
                vec![Some(0), Some(0)],
                vec![None, None]
            ]
        );
    }
    Ok(())
}
