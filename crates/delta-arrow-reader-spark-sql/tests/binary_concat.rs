//! Complete CONCAT comparisons and a focused regression gate retain open #780 failures.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Int32Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_result, register_inputs};

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

fn records(capture: &str, count: usize) -> TestResult<Vec<Value>> {
    let records = capture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, count);
    assert!(
        records[1..]
            .iter()
            .all(|case| case["expected"]["status"] == "ok")
    );
    Ok(records)
}

const INVALID_UTF8_ERROR: &str = "physical: Optimizer rule 'simplify_expressions' failed\ncaused by\nArrow error: Invalid argument error: Encountered non UTF-8 data: invalid utf-8 sequence of 1 bytes from index 0";

// The regression gate pins this failure; it remains a strict compatibility failure.
fn known_concat_ws_invalid_utf8_failure(case: &Value, actual: &Value) -> bool {
    let (sql, rows) = match case["id"].as_str() {
        Some("c06_remaining_ws_invalid_binary_value") => {
            ("SELECT concat_ws(',',X'ff') AS r", json!([["\u{fffd}"]]))
        }
        Some("c06_remaining_ws_invalid_binary_byte_roundtrip") => (
            "SELECT hex(CAST(concat_ws(',',X'ff') AS BINARY)) AS r",
            json!([["FF"]]),
        ),
        _ => return false,
    };
    let expected = &case["expected"];
    let field = json!({"type":"struct","fields":[
        {"name":"r","type":"string","nullable":false,"metadata":{}}
    ]});
    case["sql"] == sql
        && case["ansi"].is_boolean()
        && expected["status"] == "ok"
        && expected["rows"] == rows
        && [
            "schema",
            "analyzed_schema",
            "optimized_schema",
            "physical_schema",
        ]
        .into_iter()
        .all(|stage| expected[stage] == field)
        && actual.as_object().is_some_and(|object| object.len() == 6)
        && actual["analyzed_schema"] == expected["analyzed_schema"]
        && actual["analyzed_arrow_schema"]
            .as_str()
            .is_some_and(|schema| !schema.is_empty())
        && actual.get("physical_schema") == Some(&Value::Null)
        && actual.get("physical_arrow_schema") == Some(&Value::Null)
        && actual["error_stage"] == "physical"
        && actual["error"] == INVALID_UTF8_ERROR
        && actual.get("rows").is_none()
}

#[derive(Default)]
struct ComparisonResults {
    keys: HashSet<(String, bool, String, usize)>,
    full_spark_matches: usize,
    open_780_failures: usize,
    unexpected_differences: Vec<String>,
}

async fn compare(
    session: &SessionContext,
    case: &Value,
    representation: &str,
    batch_size: usize,
    results: &mut ComparisonResults,
) -> TestResult {
    assert!(results.keys.insert((
        case["id"].as_str().ok_or("missing ID")?.to_owned(),
        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        representation.to_owned(),
        batch_size,
    )));
    let observed = capture_observation(session, case).await?;
    let expected = &case["expected"];
    let full_match = observed.get("error").is_none()
        && observed.get("error_stage").is_none()
        && observed["analyzed_arrow_schema"].is_string()
        && observed["physical_arrow_schema"].is_string()
        && observed["analyzed_schema"] == expected["analyzed_schema"]
        && observed["physical_schema"] == expected["physical_schema"]
        && observed["rows"] == expected["rows"];
    let status = if full_match {
        results.full_spark_matches += 1;
        "full_spark_match"
    } else if known_concat_ws_invalid_utf8_failure(case, &observed) {
        results.open_780_failures += 1;
        "open_780_strict_failure"
    } else {
        results.unexpected_differences.push(format!(
            "{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",
            case["id"], case["ansi"],
        ));
        "unexpected_difference"
    };
    eprintln!(
        "c06 observation: {}",
        json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,
            "batch_size":batch_size,"status":status,"actual":observed})
    );
    if !full_match {
        eprintln!(
            "c06 strict difference: {}",
            json!({"id":case["id"],"sql":case["sql"],"ansi":case["ansi"],
                "representation":representation,"batch_size":batch_size,
                "full_compatibility_pass":false,"status":status,
                "expected":expected,"actual":observed})
        );
    }
    Ok(())
}

#[tokio::test]
async fn binary_concat_retains_complete_spark_comparisons() -> TestResult {
    let mut results = ComparisonResults::default();
    for (capture, count, has_fixed_inputs) in [
        (
            include_str!("fixtures/binary-concat-spark.jsonl"),
            110,
            false,
        ),
        (
            include_str!("fixtures/binary-concat-null-spark.jsonl"),
            44,
            false,
        ),
        (
            include_str!("fixtures/binary-concat-expanded-spark.jsonl"),
            108,
            true,
        ),
    ] {
        let records = records(capture, count)?;
        compare_representations(&records, &mut results).await?;
        if has_fixed_inputs {
            compare_fixed_representations(&records, &mut results).await?;
        }
    }
    let strict_failure_count = results.keys.len() - results.full_spark_matches;
    // As in the original C06 contract gate, pin existing failures without giving
    // them Spark-match credit. The independent strict audit must still fail.
    let regression_gate_pass = results.keys.len() == 1752
        && results.full_spark_matches == 1728
        && results.open_780_failures == 24
        && results.unexpected_differences.is_empty();
    eprintln!(
        "C06 CONCAT compatibility: {}",
        json!({"scope":"all 262 references and all 1752 observations",
            "compared":results.keys.len(),"full_spark_matches":results.full_spark_matches,
            "full_compatibility_pass":results.keys.len() == 1752 && strict_failure_count == 0,
            "strict_failure_count":strict_failure_count,
            "open_780_strict_failures":results.open_780_failures,
            "unexpected_differences":results.unexpected_differences.len(),
            "regression_gate_pass":regression_gate_pass})
    );
    assert!(
        regression_gate_pass,
        "full matches {}, open #780 strict failures {}, observations {}: {}",
        results.full_spark_matches,
        results.open_780_failures,
        results.keys.len(),
        results.unexpected_differences.join("\n"),
    );
    Ok(())
}

async fn compare_representations(records: &[Value], results: &mut ComparisonResults) -> TestResult {
    let mut compared = 0;
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            register_inputs(&session, &records[0], representation, batch_size)?;
            if representation == 2
                && records[0]["registered_inputs"]
                    .as_array()
                    .is_some_and(|inputs| {
                        inputs
                            .iter()
                            .any(|input| input["name"] == "concat_long_required")
                    })
            {
                let batches = session
                    .table("concat_long_required")
                    .await?
                    .collect()
                    .await?;
                assert!(batches.iter().any(|batch| {
                    batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<BinaryViewArray>()
                        .is_some_and(|array| !array.data_buffers().is_empty())
                }));
            }
            for case in &records[1..] {
                compared += 1;
                compare(
                    &session,
                    case,
                    &representation.to_string(),
                    batch_size,
                    results,
                )
                .await?;
            }
        }
    }
    assert_eq!(compared, (records.len() - 1) * 6);
    Ok(())
}

async fn compare_fixed_representations(
    records: &[Value],
    results: &mut ComparisonResults,
) -> TestResult {
    let mut compared = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        register_fixed_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            if case["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("concat_fixed_"))
            {
                compared += 1;
                compare(&session, case, "FixedSizeBinary", batch_size, results).await?;
            }
        }
    }
    assert_eq!(compared, 90 * 2);
    Ok(())
}

#[test]
fn concat_ws_invalid_utf8_profile_rejects_changed_references_fields_and_causes() -> TestResult {
    let records = records(include_str!("fixtures/binary-concat-null-spark.jsonl"), 44)?;
    let mut checked = 0;
    for case in records[1..].iter().filter(|case| {
        matches!(
            case["id"].as_str(),
            Some(
                "c06_remaining_ws_invalid_binary_value"
                    | "c06_remaining_ws_invalid_binary_byte_roundtrip"
            )
        )
    }) {
        let actual = json!({
            "analyzed_schema":case["expected"]["analyzed_schema"],
            "analyzed_arrow_schema":"DFSchema { inner: Schema { fields: [Field { name: \"#0\", data_type: Utf8 }], metadata: {} }, field_qualifiers: [None], functional_dependencies: FunctionalDependencies { deps: [] } }",
            "physical_schema":null,"physical_arrow_schema":null,
            "error_stage":"physical","error":INVALID_UTF8_ERROR,
        });
        assert!(known_concat_ws_invalid_utf8_failure(case, &actual));
        checked += 1;
        for (key, value) in [
            ("error", json!("unrelated Arrow error")),
            (
                "error",
                json!(INVALID_UTF8_ERROR.replace("index 0", "index 1")),
            ),
            ("error_stage", json!("execute")),
            (
                "physical_schema",
                case["expected"]["physical_schema"].clone(),
            ),
            ("physical_arrow_schema", json!("invented physical output")),
            ("analyzed_arrow_schema", json!("")),
            ("rows", json!([["EFBFBD"]])),
            ("rows", Value::Null),
        ] {
            let mut changed = actual.clone();
            changed[key] = value;
            assert!(
                !known_concat_ws_invalid_utf8_failure(case, &changed),
                "accepted changed {key}"
            );
        }
        for key in [
            "analyzed_arrow_schema",
            "physical_schema",
            "physical_arrow_schema",
        ] {
            let mut changed = actual.clone();
            changed
                .as_object_mut()
                .ok_or("missing observed object")?
                .remove(key);
            assert!(
                !known_concat_ws_invalid_utf8_failure(case, &changed),
                "accepted missing {key}"
            );
        }
        for (property, value) in [
            ("name", json!("other")),
            ("type", json!("binary")),
            ("nullable", json!(true)),
            ("metadata", json!({"source":"other"})),
        ] {
            let mut changed = actual.clone();
            changed["analyzed_schema"]["fields"][0][property] = value;
            assert!(
                !known_concat_ws_invalid_utf8_failure(case, &changed),
                "accepted changed field {property}"
            );
        }
        for (key, value) in [
            ("id", json!("unrelated_case")),
            ("sql", json!("SELECT concat_ws(',',X'fe') AS r")),
            ("ansi", Value::Null),
        ] {
            let mut changed = case.clone();
            changed[key] = value;
            assert!(
                !known_concat_ws_invalid_utf8_failure(&changed, &actual),
                "accepted changed case {key}"
            );
        }
        for (key, value) in [("status", json!("error")), ("rows", json!([["EFBFBD"]]))] {
            let mut changed = case.clone();
            changed["expected"][key] = value;
            assert!(
                !known_concat_ws_invalid_utf8_failure(&changed, &actual),
                "accepted changed expected {key}"
            );
        }
        let mut changed = case.clone();
        changed["expected"]["analyzed_schema"]["fields"][0]["metadata"] = json!({"source":"other"});
        assert!(!known_concat_ws_invalid_utf8_failure(&changed, &actual));
    }
    assert_eq!(checked, 4);
    Ok(())
}

fn register_fixed_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    let widths = metadata["arrow_fixed_binary_widths"]
        .as_object()
        .ok_or("missing Arrow widths")?;
    for input in metadata["registered_inputs"]
        .as_array()
        .ok_or("missing inputs")?
    {
        let name = input["name"].as_str().ok_or("missing table name")?;
        let Some(width) = widths.get(name) else {
            continue;
        };
        let width: i32 = width.as_i64().ok_or("missing width")?.try_into()?;
        let rows = input["rows"].as_array().ok_or("missing rows")?;
        let keys = rows
            .iter()
            .map(|row| Ok(row[0].as_str().ok_or("missing key")?.parse()?))
            .collect::<TestResult<Vec<i32>>>()?;
        let bytes = rows
            .iter()
            .map(|row| {
                row[1]
                    .as_str()
                    .map(|value| {
                        assert!(value.len().is_multiple_of(2));
                        (0..value.len())
                            .step_by(2)
                            .map(|i| Ok(u8::from_str_radix(&value[i..i + 2], 16)?))
                            .collect::<TestResult<Vec<u8>>>()
                    })
                    .transpose()
            })
            .collect::<TestResult<Vec<_>>>()?;
        let padding = Some(vec![0x7f; width.try_into()?]);
        let values = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            std::iter::once(padding.as_deref())
                .chain(bytes.iter().map(|value| value.as_deref()))
                .chain(std::iter::once(padding.as_deref())),
            width,
        )?
        .slice(1, rows.len());
        assert_eq!(values.len(), rows.len());
        let fields = input["schema"]["fields"]
            .as_array()
            .ok_or("missing fields")?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false)
                .with_metadata(serde_json::from_value(fields[0]["metadata"].clone())?),
            Field::new(
                "b",
                DataType::FixedSizeBinary(width),
                fields[1]["nullable"]
                    .as_bool()
                    .ok_or("missing nullability")?,
            )
            .with_metadata(serde_json::from_value(fields[1]["metadata"].clone())?),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(keys)) as ArrayRef,
                Arc::new(values),
            ],
        )?;
        let mut partitions = vec![Vec::new(); 2];
        for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[chunk % 2]
                .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        session.register_table(name, Arc::new(MemTable::try_new(schema, partitions)?))?;
    }
    Ok(())
}

#[tokio::test]
async fn binary_concat_preserves_native_datafusion() -> TestResult {
    let values: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(b"excluded padding!".as_slice()),
        Some(b"long native bytes".as_slice()),
        None,
        Some(b"other byte value!".as_slice()),
        Some(b"excluded padding!".as_slice()),
    ]));
    for data_type in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(17),
    ] {
        let values = cast(&values, &data_type)?.slice(1, 3);
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", data_type, true)
                .with_metadata([("origin".into(), "native".into())].into()),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values])?;
        let plain = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(1)
                .with_target_partitions(2),
        );
        let session = session(1);
        for target in [&plain, &session] {
            target.register_table(
                "native_concat",
                Arc::new(MemTable::try_new(
                    schema.clone(),
                    vec![vec![batch.clone()]],
                )?),
            )?;
        }
        let expected_projection = plain
            .sql("SELECT b FROM native_concat")
            .await?
            .collect()
            .await?;
        let sql = "SELECT concat(b,b) AS pair,concat(b) AS single FROM native_concat";
        let expected = if matches!(schema.field(0).data_type(), DataType::FixedSizeBinary(_)) {
            None
        } else {
            Some(plain.sql(sql).await?.collect().await?)
        };
        for ansi in [None, Some(true), Some(false)] {
            if let Some(ansi) = ansi {
                capture_result(
                    &session,
                    &json!({"sql":"SELECT concat(b,b) AS r FROM native_concat", "ansi":ansi}),
                )
                .await?;
            }
            let projected = session
                .sql("SELECT b FROM native_concat")
                .await?
                .collect()
                .await?;
            assert!(
                projected
                    .iter()
                    .all(|batch| { batch.column(0).data_type() == schema.field(0).data_type() })
            );
            assert_eq!(projected, expected_projection);
            if let Some(expected) = &expected {
                assert_eq!(&session.sql(sql).await?.collect().await?, expected);
            } else {
                for target in [&plain, &session] {
                    let Err(error) = target.sql(sql).await?.collect().await else {
                        return Err("native FixedSizeBinary CONCAT unexpectedly executed".into());
                    };
                    assert!(
                        matches!(&error, DataFusionError::Context(context, cause)
                            if context == "type_coercion"
                            && matches!(cause.as_ref(), DataFusionError::Plan(message)
                                if message == "Cannot automatically convert FixedSizeBinary(17) to Utf8")),
                        "{error:?}"
                    );
                }
            }
        }
    }
    Ok(())
}
