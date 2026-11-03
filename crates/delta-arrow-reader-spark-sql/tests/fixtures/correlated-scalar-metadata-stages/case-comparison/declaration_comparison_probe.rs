//! Root runs this immutable probe against a frozen head; mismatches retain actual rows.

use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "/home/hanbo/.local/share/delta-arrow-reader-review/spark-window-completion-2026-11-02/correlated-scalar-declaration-comparison-v2/sources/spark_inputs.rs"]
mod spark_inputs;
#[expect(dead_code, reason = "This probe uses only the frozen schema and row encoders.")]
#[path = "/home/hanbo/.local/share/delta-arrow-reader-review/spark-window-completion-2026-11-02/correlated-scalar-declaration-comparison-v2/sources/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, validate_batch};
use arrow::array::{Array, ListArray, MapArray, StructArray};
use arrow::datatypes::{DataType, Fields};


fn probe_schema_type(data_type: &DataType) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err("MAP entries are not a struct".into());
            };
            if fields.len() != 2 {
                return Err("MAP entries do not have two fields".into());
            }
            json!({"type":"map", "keyType":probe_schema_type(fields[0].data_type())?,
                "valueType":probe_schema_type(fields[1].data_type())?,
                "valueContainsNull":fields[1].is_nullable()})
        }
        DataType::Struct(fields) => capture_schema(fields, None)?,
        DataType::List(field) => json!({"type":"array",
            "elementType":probe_schema_type(field.data_type())?,
            "containsNull":field.is_nullable()}),
        other => spark_results::spark_schema_type(other)?,
    })
}

fn capture_schema(fields: &Fields, names: Option<&[String]>) -> TestResult<Value> {
    Ok(json!({"type":"struct", "fields":fields.iter().enumerate()
        .map(|(index, field)| Ok(json!({
            "name":names.map_or(field.name(), |names| &names[index]),
            "type":probe_schema_type(field.data_type())?, "nullable":field.is_nullable(),
            "metadata":field.metadata()})))
        .collect::<TestResult<Vec<_>>>()?}))
}

fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Map(_, _) => {
            let values = column.as_any().downcast_ref::<MapArray>()
                .ok_or("expected MAP")?.value(row);
            if values.num_columns() != 2 || values.column(0).logical_null_count() != 0 {
                return Err("invalid MAP entries or NULL key".into());
            }
            json!((0..values.len()).map(|index| Ok(vec![
                encode_value(values.column(0).as_ref(), index)?,
                encode_value(values.column(1).as_ref(), index)?,
            ])).collect::<TestResult<Vec<_>>>()?)
        }
        DataType::Struct(fields) => {
            let values = column.as_any().downcast_ref::<StructArray>()
                .ok_or("expected STRUCT")?;
            json!(values.columns().iter().zip(fields).map(|(column, field)| {
                let value = encode_value(column.as_ref(), row)?;
                if value.is_null() && !field.is_nullable() {
                    return Err("nonnullable STRUCT child returned NULL".into());
                }
                Ok(value)
            }).collect::<TestResult<Vec<_>>>()?)
        }
        DataType::List(field) => {
            let values = column.as_any().downcast_ref::<ListArray>()
                .ok_or("expected ARRAY")?.value(row);
            if !field.is_nullable() && values.logical_null_count() != 0 {
                return Err("nonnullable ARRAY element returned NULL".into());
            }
            json!((0..values.len()).map(|index| encode_value(values.as_ref(), index))
                .collect::<TestResult<Vec<_>>>()?)
        }
        _ => spark_results::encode_value(column, row)?,
    })
}

#[tokio::test]
async fn comparison_case_preserves_declared_metadata_and_refined_nullability() -> TestResult {
    let raw = std::fs::read_to_string(
        "/home/hanbo/.local/share/delta-arrow-reader-review/spark-window-completion-2026-11-02/correlated-scalar-declaration-comparison-v2/case-equal-spark.jsonl",
    )?;
    let records = raw.lines().map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 4);
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?)));
        assert!(case.get("expected").is_some() ^ case.get("error").is_some());
    }
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(configure_spark_sql(
            SessionStateBuilder::new().with_default_features().with_config(
                SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2)))
            .build());
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let mut actual = json!({});
            let mut phase = "resolve";
            let observed: TestResult = async {
                let query = plan_query(&session, case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions { ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        session_timezone: "UTC".into(), ..SparkSqlOptions::default() }).await?;
                actual["public_schema"] = capture_schema(query.logical_plan.schema().fields(),
                    Some(&query.column_names))?;
                actual["logical_plan"] = json!(format!("{}", query.logical_plan.display_indent_schema()));
                phase = "physical";
                let physical = query.create_physical_plan(&session).await?;
                let schema = physical.schema();
                actual["executed_schema"] = capture_schema(schema.fields(), None)?;
                actual["arrow_physical_schema"] = json!(format!("{schema:?}"));
                phase = "execute";
                let batches = collect(Arc::clone(&physical), session.task_ctx()).await?;
                actual["executed_schema_after_collect"] = capture_schema(physical.schema().fields(), None)?;
                let mut rows = Vec::new();
                let mut batch_observations = Vec::new();
                for batch in batches {
                    phase = "validate_batch";
                    validate_batch(schema.as_ref(), &batch)?;
                    batch_observations.push(json!({"schema":capture_schema(batch.schema().fields(), None)?,
                        "rows":batch.num_rows()}));
                    for row in 0..batch.num_rows() {
                        rows.push(batch.columns().iter().map(|column| encode_value(column.as_ref(), row))
                            .collect::<TestResult<Vec<_>>>()?);
                    }
                }
                rows.sort_by_key(|row| json!(row).to_string());
                actual["batches"] = json!(batch_observations);
                actual["rows"] = json!(rows);
                Ok(())
            }.await;
            let matches = match &observed {
                Ok(()) if case.get("error").is_none() => {
                    actual["public_schema"] == case["public_schema"]
                        && actual["executed_schema"] == case["executed_schema_before_collect"]
                        && actual["executed_schema_after_collect"] == case["executed_schema_after_collect"]
                        && actual["rows"] == case["expected"]["rows"]
                }
                Ok(()) => false,
                Err(error) => {
                    let engine_error = error.downcast_ref::<DataFusionError>().is_some();
                    actual["error"] = json!({"message":error.to_string(), "phase":phase,
                        "engine_error":engine_error});
                    case["error"]["condition"].as_str().is_some_and(|condition|
                        engine_error && error.to_string().contains(&format!("[{condition}]")))
                }
            };
            eprintln!("declaration_comparison_probe: {}", json!({"id":case["id"],"input":case["input"],
                "ansi":case["ansi"],"batch_size":batch_size,"matches":matches,
                "expected":case,"actual":actual}));
            if !matches {
                differences.push(format!("{} {} ANSI={} batch={batch_size}: expected {case}, got {actual}",
                    case["id"],case["input"],case["ansi"]));
            }
            comparisons += 1;
        }
    }
    assert_eq!(comparisons, 8);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
