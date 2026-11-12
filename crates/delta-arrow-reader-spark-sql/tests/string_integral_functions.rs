//! Compare STRING conversion functions and CAST with independent Spark results.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::{TestResult, capture_native_query_observation, capture_spark_query_observation};

const INPUTS: &str = include_str!("fixtures/string-integral-functions/inputs.json");
const SPARK: &str = include_str!("fixtures/string-integral-functions/spark-results.jsonl");

fn session() -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(1)),
        )
        .build(),
    )
}

fn dictionary_encode(batch: RecordBatch) -> TestResult<RecordBatch> {
    let schema = batch.schema();
    let string_index = schema.index_of("s")?;
    let string_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
    let mut columns = batch.columns().to_vec();
    columns[string_index] = arrow::compute::cast(&columns[string_index], &string_type)?;
    let mut fields = schema.fields().to_vec();
    fields[string_index] = Arc::new(
        schema
            .field(string_index)
            .clone()
            .with_data_type(string_type),
    );
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )?)
}

fn result_types(schema: &Value) -> TestResult<Vec<&str>> {
    schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| {
            field["type"]
                .as_str()
                .ok_or_else(|| "missing result type".into())
        })
        .collect()
}

fn assert_reference_matrix(manifest: &Value, references: &[Value]) -> TestResult {
    let mut planned = BTreeMap::new();
    for case in manifest["cases"].as_array().ok_or("missing cases")? {
        let id = case["id"].as_str().ok_or("missing case ID")?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        for mode in case["ansi_modes"].as_array().ok_or("missing ANSI modes")? {
            let ansi = mode.as_bool().ok_or("expected an ANSI boolean")?;
            assert!(planned.insert((id, ansi), sql).is_none());
        }
    }
    for case in references {
        let id = case["id"].as_str().ok_or("missing reference ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        let sql = planned
            .remove(&(id, ansi))
            .ok_or("unplanned Spark reference")?;
        assert_eq!(case["sql"], sql, "{id}, ANSI={ansi}");
        assert_eq!(case["status"], "ok", "{id}, ANSI={ansi}");
        assert_eq!(
            case["effective_settings"]["spark.sql.ansi.enabled"],
            ansi.to_string()
        );
        assert_eq!(
            case["effective_settings"]["spark.sql.session.timeZone"],
            "UTC"
        );
    }
    assert!(planned.is_empty(), "missing Spark references: {planned:?}");
    Ok(())
}

#[tokio::test]
async fn string_integral_functions_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let references = SPARK
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(manifest["spark_version"], "4.2.0");
    assert_eq!(
        manifest["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_reference_matrix(&manifest, &references)?;

    let mut differences = Vec::new();
    for (storage, dictionary) in [("utf8", false), ("dictionary_uint16_utf8", true)] {
        let session = session();
        for input in manifest["registered_inputs"].as_array().ok_or("missing inputs")? {
            let name = input["name"].as_str().ok_or("missing input table name")?;
            let mut batch = spark_inputs::decode_recorded_batch(input)?;
            if dictionary {
                batch = dictionary_encode(batch)?;
            }
            session.register_batch(name, batch)?;
        }
        for case in &references {
            let mut expected_rows = case["rows"].as_array().ok_or("missing Spark rows")?.clone();
            expected_rows.sort_by_key(Value::to_string);
            let expected_types = result_types(&case["physical_schema"])?;
            let observation = match capture_spark_query_observation(&session, case).await {
                Ok(actual) => actual,
                Err(error) => json!({"error": error.to_string()}),
            };
            let matches = observation.get("error").is_none()
                && result_types(&observation["physical_schema"])? == expected_types
                && observation["result"]["rows"].as_array() == Some(&expected_rows);
            let record = json!({
                "id": case["id"], "ansi": case["ansi"], "sql": case["sql"], "storage": storage,
                "matches": matches, "expected_types": expected_types, "expected_rows": expected_rows,
                "actual": observation,
            });
            eprintln!("STRING_INTEGRAL_FUNCTIONS {record}");
            if !matches {
                differences.push(record.to_string());
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_integral_casts_keep_their_results() -> TestResult {
    let native = SessionContext::new();
    let configured = session();
    for (sql, types, rows) in [
        (
            "SELECT CAST('42' AS TINYINT) AS a, CAST('42' AS SMALLINT) AS b, \
             CAST('42' AS INT) AS c, CAST('42' AS BIGINT) AS d",
            vec!["byte", "short", "integer", "long"],
            json!([["42", "42", "42", "42"]]),
        ),
        (
            "SELECT arrow_cast('42', 'Int32') AS a, TRY_CAST('bad' AS INT) AS b",
            vec!["integer", "integer"],
            json!([["42", null]]),
        ),
    ] {
        let expected = capture_native_query_observation(&native, sql).await?;
        let actual = capture_native_query_observation(&configured, sql).await?;
        let record = json!({"sql": sql, "native": expected, "configured": actual});
        eprintln!("STRING_INTEGRAL_NATIVE {record}");
        assert_eq!(actual, expected, "{sql}");
        assert_eq!(result_types(&actual["physical_schema"])?, types, "{sql}");
        assert_eq!(actual["rows"], rows, "{sql}");
    }

    let sql = "SELECT CAST('bad' AS INT) AS r";
    let expected = capture_native_query_observation(&native, sql).await?;
    let actual = capture_native_query_observation(&configured, sql).await?;
    let record = json!({"sql": sql, "native": expected, "configured": actual});
    eprintln!("STRING_INTEGRAL_NATIVE {record}");
    assert_eq!(actual, expected, "{sql}");
    assert!(actual["error"].is_string(), "{sql}: {actual}");
    Ok(())
}
