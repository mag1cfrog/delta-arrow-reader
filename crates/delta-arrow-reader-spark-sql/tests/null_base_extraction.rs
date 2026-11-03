//! Ordinary NULL extraction follows Spark's distinct attribute and lambda paths.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, ListArray, NullArray, StructArray, new_empty_array, new_null_array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::error::PlanError;
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_observation, capture_spark_query_result};

const ORIGINAL: &str = include_str!("fixtures/struct-extraction-boundaries-review-v3-spark.jsonl");
const ORIGINAL_NULLS: &str = include_str!("fixtures/struct-extraction-boundaries-815-spark.jsonl");
const REFERENCE: &str = include_str!("fixtures/null-base-extraction-815-spark.jsonl");

fn records(source: &str) -> TestResult<Vec<Value>> {
    Ok(source.lines().map(serde_json::from_str).collect::<Result<_, _>>()?)
}

fn identity(case: &Value) -> TestResult<(&str, &str, bool)> {
    Ok((case["id"].as_str().ok_or("missing ID")?,
        case["input"].as_str().ok_or("missing input")?,
        case["ansi"].as_bool().ok_or("missing ANSI mode")?))
}

#[test]
fn original_extraction_partition_preserves_all_16_records() -> TestResult {
    let controls = include_str!("fixtures/struct-extraction-boundaries-803-spark.jsonl");
    for subset in [controls, ORIGINAL_NULLS] {
        assert_eq!(ORIGINAL.lines().next(), subset.lines().next());
    }
    let mut original = BTreeMap::new();
    for line in ORIGINAL.lines().skip(1) {
        let case: Value = serde_json::from_str(line)?;
        let key = (case["id"].as_str().ok_or("missing ID")?.to_string(), case["ansi"].as_bool().ok_or("missing ANSI mode")?);
        assert!(original.insert(key, line).is_none());
    }
    assert_eq!(original.len(), 16);
    let mut covered = BTreeSet::new();
    for (subset, count, null_partition) in [(controls, 10, false), (ORIGINAL_NULLS, 4, true)] {
        assert_eq!(subset.lines().count() - 1, count);
        for line in subset.lines().skip(1) {
            let case: Value = serde_json::from_str(line)?;
            let key = (case["id"].as_str().ok_or("missing ID")?.to_string(), case["ansi"].as_bool().ok_or("missing ANSI mode")?);
            assert_eq!(case["input"], "constant");
            assert_eq!(matches!(case["id"].as_str(), Some("null_local_base" | "null_computed_base")), null_partition);
            assert_eq!(original.get(&key), Some(&line));
            assert!(covered.insert(key));
        }
    }
    let pending: Vec<_> = original.keys().filter(|key| !covered.contains(*key)).collect();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|key| key.0 == "map_lambda_base"));
    Ok(())
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(configure_spark_sql(
        SessionStateBuilder::new().with_default_features().with_config(
            SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
        ),
    ).build())
}

fn register_inputs(session: &SessionContext, metadata: &Value, batch_size: usize) -> TestResult {
    let child = Arc::new(Field::new("n", DataType::Null, true).with_metadata(
        HashMap::from([("source".into(), "null_child".into())]),
    ));
    let children: Fields = vec![child].into();
    let record_type = DataType::Struct(children.clone());
    let record = |len: usize, valid: Vec<bool>| -> TestResult<ArrayRef> {
        Ok(Arc::new(StructArray::try_new(children.clone(), vec![Arc::new(NullArray::new(len))], Some(NullBuffer::from(valid)))?))
    };
    let list = |values: ArrayRef| -> TestResult<ArrayRef> {
        Ok(Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(values.data_type().clone(), true)),
            OffsetBuffer::from_lengths([2, 0, 0]), values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?))
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Null, true).with_metadata(HashMap::from([("source".into(), "null_column".into())])),
        Field::new("r", record_type.clone(), true),
        Field::new("numbers", DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))), true),
        Field::new("records", DataType::List(Arc::new(Field::new_list_field(record_type, true))), true),
        Field::new("nulls", DataType::List(Arc::new(Field::new_list_field(DataType::Null, true))), true),
    ]));
    let mixed = vec![
        Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef,
        Arc::new(NullArray::new(3)), record(3, vec![true, false, true])?,
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]), None, Some(vec![]),
        ])),
        list(record(2, vec![true, false])?)?, list(Arc::new(NullArray::new(2)))?,
    ];
    let nulls = std::iter::once(Arc::new(Int32Array::from(vec![3])) as ArrayRef)
        .chain(schema.fields().iter().skip(1).map(|field| new_null_array(field.data_type(), 1)))
        .collect();
    let empty = schema.fields().iter().map(|field| new_empty_array(field.data_type())).collect();
    let inputs = metadata["inputs"].as_array().ok_or("missing inputs")?;
    assert_eq!(inputs.len(), 3);
    for (index, (name, columns)) in [("null_mixed", mixed), ("null_nulls", nulls), ("null_empty", empty)].into_iter().enumerate() {
        assert_eq!(inputs[index]["name"], name);
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        spark_results::validate_batch(schema.as_ref(), &batch)?;
        let mut partitions = vec![Vec::new(); 2];
        for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[chunk % 2].push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        assert!(session.register_table(name, Arc::new(MemTable::try_new(schema.clone(), partitions)?))?.is_none());
    }
    Ok(())
}

async fn validate_inputs(session: &SessionContext, metadata: &Value) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing table")?;
        let actual = capture_spark_query_result(session, &json!({"ansi":true,"sql":format!(
            "SELECT id,s,r,numbers,records,nulls FROM {name}"
        )})).await?;
        let mut rows = input["rows"].as_array().ok_or("missing rows")?.clone();
        rows.sort_by_key(Value::to_string);
        assert_eq!(actual, json!({"schema":input["schema"],"rows":rows}), "{name}");
    }
    Ok(())
}

fn matches_planning_error(error: &DataFusionError, case: &Value) -> bool {
    let DataFusionError::External(source) = error else { return false; };
    let Some(PlanError::AnalysisError(message)) = source.downcast_ref::<PlanError>() else { return false; };
    let Some(condition) = case["expected"]["condition"].as_str() else { return false; };
    let parameters = &case["error"]["parameters"];
    let cause = match condition {
        "INVALID_EXTRACT_BASE_FIELD_TYPE" => {
            let data_type = match parameters["other"].as_str() {
                Some("\"VOID\"") => "Null",
                Some("\"INT\"") => "Int32",
                _ => return false,
            };
            format!("field \"missing\" cannot be extracted from {data_type}")
        }
        "FIELD_NOT_FOUND" if parameters["fieldName"] == "`missing`" && parameters["fields"] == "`other`" => {
            "field \"missing\" is absent from STRUCT fields [\"other\"]".into()
        }
        "AMBIGUOUS_REFERENCE_TO_FIELDS" if parameters["field"] == "`item`" && parameters["count"] == "2" => {
            "field \"item\" matches multiple STRUCT children".into()
        }
        "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" if case["id"] == "scalar_array_base"
            && parameters["inputType"] == "\"STRING\""
            && parameters["inputSql"] == "\"missing\""
            && parameters["requiredType"] == "\"INTEGRAL\"" => {
            "field \"missing\" requires a STRUCT array element, got Int32".into()
        }
        _ => return false,
    };
    message == &format!("cannot resolve attribute: [{condition}] {cause}")
}

#[test]
fn error_checks_require_the_actual_resolver_type_fields_and_selector_cause() -> TestResult {
    let references = records(REFERENCE)?;
    for (id, message, cause, wrong_cause) in [
        ("null_lambda_base", "cannot resolve attribute: [INVALID_EXTRACT_BASE_FIELD_TYPE] field \"missing\" cannot be extracted from Null", "from Null", "from Int32"),
        ("scalar_base", "cannot resolve attribute: [INVALID_EXTRACT_BASE_FIELD_TYPE] field \"missing\" cannot be extracted from Int32", "from Int32", "from Null"),
        ("missing_struct", "cannot resolve attribute: [FIELD_NOT_FOUND] field \"missing\" is absent from STRUCT fields [\"other\"]", "[\"other\"]", "[\"unrelated\"]"),
        ("duplicate_struct", "cannot resolve attribute: [AMBIGUOUS_REFERENCE_TO_FIELDS] field \"item\" matches multiple STRUCT children", "multiple", "no"),
        ("scalar_array_base", "cannot resolve attribute: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] field \"missing\" requires a STRUCT array element, got Int32", "got Int32", "got Null"),
    ] {
        let case = references[1..].iter().find(|case| case["id"] == id).ok_or("missing control")?;
        let wrong = message.replace(cause, wrong_cause);
        assert!(matches_planning_error(&DataFusionError::External(Box::new(PlanError::AnalysisError(message.into()))), case));
        assert!(!matches_planning_error(&DataFusionError::Plan(message.into()), case));
        for wrong in [wrong, message.replace("field \"", "field \"unrelated_"), format!("unrelated: {message}")] {
            assert!(!matches_planning_error(&DataFusionError::External(Box::new(PlanError::AnalysisError(wrong))), case));
        }
    }
    Ok(())
}

async fn compare_cases(source: &str, count: usize) -> TestResult {
    let records = records(source)?;
    assert_eq!(records.len() - 1, count);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert!(records[0]["spark_build"].as_str().ok_or("missing Spark build")?.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8"));
    let mut identities = BTreeSet::new();
    for case in &records[1..] { assert!(identities.insert(identity(case)?)); }
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        if !records[0]["inputs"].as_array().ok_or("missing inputs")?.is_empty() {
            register_inputs(&session, &records[0], batch_size)?;
            validate_inputs(&session, &records[0]).await?;
        }
        for case in &records[1..] {
            comparisons += 1;
            let difference = if case["expected"].get("condition").is_some() {
                assert_eq!(case["phase"], "planning");
                assert_eq!(case["failure_stage"], "analyzed");
                assert_eq!(case["schema_stages"], json!({}));
                let result = plan_query(&session, case["sql"].as_str().ok_or("missing SQL")?, &SparkSqlOptions {
                    ansi_mode:case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    session_timezone:"UTC".into(), ..SparkSqlOptions::default()
                }).await;
                match result {
                    Err(error) if matches_planning_error(&error, case) => None,
                    Err(error) => Some(format!("expected {}, got planning {error:?}", case["expected"])),
                    Ok(_) => Some(format!("expected planning {}, got a logical plan", case["expected"])),
                }
            } else {
                assert_eq!(case["phase"], "success");
                assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
                match capture_spark_query_observation(&session, case).await {
                    Ok(actual) if actual["result"] == case["expected"] && actual["physical_schema"] == case["schema_stages"]["executed"] => None,
                    Ok(actual) => Some(format!("expected {} (executed {}), got {actual}", case["expected"], case["schema_stages"]["executed"])),
                    Err(error) => Some(format!("expected {}, got {error:?}", case["expected"])),
                }
            };
            if let Some(difference) = difference {
                differences.push(format!("{} input={} ANSI={} batch={batch_size}: {difference}",case["id"],case["input"],case["ansi"]));
            }
        }
    }
    assert_eq!(comparisons, count * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn original_four_ordinary_null_records_match_spark() -> TestResult {
    compare_cases(ORIGINAL_NULLS, 4).await
}

#[tokio::test]
async fn null_provenance_matches_all_130_independent_records() -> TestResult {
    let references = records(REFERENCE)?;
    assert_eq!(references[1..].iter().filter(|case| case["phase"] == "success").count(), 102);
    assert_eq!(references[1..].iter().filter(|case| case["phase"] == "planning").count(), 28);
    compare_cases(REFERENCE, 130).await
}

async fn native_snapshot(session: &SessionContext, sql: &str) -> TestResult<(String, Vec<RecordBatch>)> {
    let frame = match session.sql(sql).await {
        Ok(frame) => frame,
        Err(error) => return Ok((format!("planning:{error:?}"), vec![])),
    };
    let logical = frame.schema().clone();
    let physical = match frame.create_physical_plan().await {
        Ok(physical) => physical,
        Err(error) => return Ok((format!("physical-planning:{error:?}"), vec![])),
    };
    let schema = physical.schema();
    let batches = match collect(physical, session.task_ctx()).await {
        Ok(batches) => batches,
        Err(error) => return Ok((format!("execution:{error:?}"), vec![])),
    };
    for batch in &batches { spark_results::validate_batch(schema.as_ref(), batch)?; }
    Ok((format!("success:{logical:?}\n{schema:?}"), batches))
}

#[tokio::test]
async fn null_extraction_leaves_native_sql_fields_rows_and_failures_unchanged() -> TestResult {
    let native = SessionContext::new();
    let configured = session(32);
    let queries = [
        "SELECT get_field(NULL,'missing') AS result",
        "SELECT get_field(named_struct('inner',named_struct('item',7)),'inner','item') AS result",
        "SELECT get_field(1,'missing') AS result",
        "SELECT CAST('bad' AS INT) AS result",
    ];
    let mut expected = Vec::new();
    for sql in queries { expected.push(native_snapshot(&native, sql).await?); }
    assert!(expected[0].0.starts_with("success:"));
    assert_eq!(expected[0].1.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let native_null = expected[0].1[0].column(0);
    assert_eq!(native_null.data_type(), &DataType::Null);
    assert_eq!(spark_results::encode_value(native_null.as_ref(), 0)?, Value::Null);
    assert!(expected[1].0.starts_with("success:"));
    assert!(!expected[2].0.starts_with("success:") && expected[2].0.contains("Cannot access field at argument 1: type Int64 is not Struct, Map, or Null"));
    assert!(!expected[3].0.starts_with("success:") && expected[3].0.contains("Cannot cast string 'bad' to value of Int32 type"));
    let mut spark_error = None;
    for after_spark in [false, true] {
        if after_spark {
            spark_error = capture_spark_query_result(&configured, &json!({"ansi":true,"sql":"SELECT (NULL).missing AS result"})).await.err();
        }
        for (sql, snapshot) in queries.iter().zip(&expected) {
            assert_eq!(&native_snapshot(&configured, sql).await?, snapshot, "{sql}, after_spark={after_spark}");
        }
    }
    if let Some(error) = spark_error { return Err(error); }
    Ok(())
}
