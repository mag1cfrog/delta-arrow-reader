//! Ordinary relational results retain values, NULLs, multiplicity and executed types.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_schema, encode_value, remove_nullability_and_metadata, validate_batch,
};

fn case_key(case: &Value) -> TestResult<(String, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing ID")?.to_owned(),
        case["ansi"].as_bool().ok_or("missing ANSI")?,
    ))
}

fn executed_types(schema: &Value) -> TestResult<Value> {
    let mut types = json!(
        schema["fields"]
            .as_array()
            .ok_or("missing fields")?
            .iter()
            .map(|field| field["type"].clone())
            .collect::<Vec<_>>()
    );
    remove_nullability_and_metadata(&mut types);
    Ok(types)
}

fn load_references() -> TestResult<(Vec<Value>, Value, Value)> {
    let original_records = include_str!("fixtures/relational-queries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(original_records[0]["spark_version"], "4.2.0");
    assert_eq!(original_records[0]["timezone"], "UTC");
    assert_eq!(original_records.len() - 1, 128);
    let additional_records =
        include_str!("fixtures/relational-results-coverage/spark-results.jsonl")
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(additional_records[0]["spark_version"], "4.2.0");
    assert_eq!(additional_records[0]["expected_record_count"], 42);
    assert_eq!(additional_records.len() - 1, 42);
    let legacy_request: Value = serde_json::from_str(include_str!(
        "fixtures/relational-results-coverage/legacy-requests.json"
    ))?;
    let coverage_request: Value = serde_json::from_str(include_str!(
        "fixtures/relational-results-coverage/requests.json"
    ))?;
    let legacy_inputs = &legacy_request["corpora"][0]["metadata"]["inputs"];
    let coverage_inputs = &coverage_request["corpora"][0]["metadata"]["inputs"];
    let mut definitions = BTreeMap::new();
    for request in [&legacy_request, &coverage_request] {
        assert_eq!(
            request["corpora"]
                .as_array()
                .ok_or("missing corpora")?
                .len(),
            1
        );
        for case in request["corpora"][0]["cases"]
            .as_array()
            .ok_or("missing cases")?
        {
            assert!(definitions.insert(case_key(case)?, case.clone()).is_none());
        }
    }
    let recorded_inputs = &additional_records[0]["corpora"][0]["metadata"]["inputs"];
    assert_eq!(recorded_inputs, coverage_inputs);
    let legacy_names = original_records[0]["inputs"]
        .as_array()
        .ok_or("missing original inputs")?
        .iter()
        .map(|input| input["name"].as_str().ok_or("missing name"))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut extra_inputs = Vec::new();
    for input in coverage_inputs.as_array().ok_or("missing new inputs")? {
        let name = input["name"].as_str().ok_or("missing name")?;
        if legacy_names.contains(name) {
            let legacy = legacy_inputs
                .as_array()
                .ok_or("missing legacy inputs")?
                .iter()
                .find(|old| old["name"] == input["name"])
                .ok_or("missing legacy declaration")?;
            assert_eq!(legacy, input);
        } else {
            extra_inputs.push(input.clone());
        }
    }
    assert_eq!(
        extra_inputs
            .iter()
            .map(|input| input["name"].as_str().ok_or("missing name"))
            .collect::<Result<BTreeSet<_>, _>>()?,
        BTreeSet::from(["bag_left", "bag_right"])
    );
    let mut cases = original_records[1..].to_vec();
    for record in &additional_records[1..] {
        assert_eq!(record["status"], "ok");
        cases.push(json!({"id":record["id"],"ansi":record["ansi"],"sql":record["sql"],"ordered":record["ordered"],"expected":{"status":"ok","rows":record["rows"],"schema":record["schema"]}}));
    }
    let mut observed = BTreeSet::new();
    for case in &mut cases {
        let key = case_key(case)?;
        assert!(observed.insert(key.clone()));
        let definition = definitions.get(&key).ok_or("missing source definition")?;
        assert_eq!(case["sql"], definition["sql"]);
        if case.get("ordered").is_some() {
            assert_eq!(case["ordered"], definition["ordered"]);
        }
        case["ordered"] = definition["ordered"].clone();
        assert_eq!(
            case.get("sql_null_rows"),
            approved_in_null_rows(case).as_ref()
        );
    }
    assert_eq!(observed, definitions.into_keys().collect());
    assert_eq!(cases.len(), 170);
    Ok((
        cases,
        original_records[0].clone(),
        json!({"inputs":extra_inputs}),
    ))
}

async fn assert_provider_partitions(
    session: &SessionContext,
    metadata: &Value,
    expected: usize,
) -> TestResult {
    let state = session.state();
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing input name")?;
        let scan = session
            .table_provider(name)
            .await?
            .scan(&state, None, &[], None)
            .await?;
        assert_eq!(
            scan.output_partitioning().partition_count(),
            expected,
            "{name}"
        );
    }
    Ok(())
}

async fn assert_native_count(session: &SessionContext) -> TestResult {
    let batches = session
        .sql("SELECT count(*) FROM left_inputs")
        .await?
        .collect()
        .await?;
    assert_eq!(encode_value(batches[0].column(0).as_ref(), 0)?, json!("5"));
    Ok(())
}

#[tokio::test]
async fn relational_queries_match_independent_references() -> TestResult {
    let (cases, original_inputs, extra_inputs) = load_references()?;
    let mut differences = Vec::new();
    let mut spark_contexts = 0;
    let mut truth_contexts = 0;
    let mut validation_contexts = 0;
    for (provider_partitions, input_batch, target_partitions, output_batch) in
        [(1, 1, 1, 1), (3, 2, 4, 3), (2, 32, 2, 32)]
    {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(output_batch)
                            .with_target_partitions(target_partitions),
                    ),
            )
            .build(),
        );
        register_relational_inputs(&session, &original_inputs, input_batch, provider_partitions)?;
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &extra_inputs,
            input_batch,
            provider_partitions,
        )?;
        assert_provider_partitions(&session, &original_inputs, provider_partitions).await?;
        assert_provider_partitions(&session, &extra_inputs, provider_partitions).await?;
        assert_native_count(&session).await?;
        for case in &cases {
            let actual = execute_relational_query(&session, case).await;
            let expected = &case["expected"];
            let truth = approved_in_null_rows(case);
            let matches = match &actual {
                Ok(actual) if expected["status"] == "ok" => {
                    let mut rows = truth.as_ref().unwrap_or(&expected["rows"]).clone();
                    if case["ordered"] != true {
                        rows.as_array_mut()
                            .ok_or("missing rows")?
                            .sort_by_key(Value::to_string);
                    }
                    let matched = actual["rows"] == rows
                        && executed_types(&actual["schema"])?
                            == executed_types(&expected["schema"])?;
                    if truth.is_some() {
                        truth_contexts += 1;
                    } else {
                        spark_contexts += 1;
                    }
                    matched
                }
                Err(error) if case["id"] == "scalar_many" && expected["status"] == "error" => {
                    validation_contexts += 1;
                    error
                        .to_string()
                        .contains("Scalar subquery returned more than one row")
                }
                _ => false,
            };
            println!(
                "RELATIONAL_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"provider_partitions":provider_partitions,"input_batch":input_batch,"target_partitions":target_partitions,"output_batch":output_batch,"ordered":case["ordered"],"raw_spark":expected,"approved_in_truth":truth,"actual":actual.as_ref().map_err(ToString::to_string),"matches":matches})
            );
            if !matches {
                differences.push(format!("{} ANSI={} layout=({provider_partitions},{input_batch},{target_partitions},{output_batch}): expected {expected}, got {actual:?}",case["id"],case["ansi"]));
            }
        }
        assert_native_count(&session).await?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    assert_eq!(
        (spark_contexts, truth_contexts, validation_contexts),
        (480, 24, 6)
    );
    println!(
        "RELATIONAL_COMPARISONS {}",
        json!({"raw_spark_success_contexts":spark_contexts,"approved_in_truth_contexts":truth_contexts,"own_scalar_validation_contexts":validation_contexts})
    );
    Ok(())
}

// The approved projected-IN policy retains SQL three-valued results, including empty inputs.
fn approved_in_null_rows(case: &Value) -> Option<Value> {
    let values = match case["id"].as_str()? {
        "in" => [None, None, Some("true"), Some("true"), None],
        "not_in" => [None, None, Some("false"), Some("false"), None],
        "in_empty" => [Some("false"); 5],
        "not_in_empty" => [Some("true"); 5],
        _ => return None,
    };
    Some(json!(
        [None, Some("1"), Some("2"), Some("2"), Some("4")]
            .into_iter()
            .zip(values)
            .map(|(key, found)| json!([key, found]))
            .collect::<Vec<_>>()
    ))
}

fn register_relational_inputs(
    session: &SessionContext,
    reference: &Value,
    batch_size: usize,
    partition_count: usize,
) -> TestResult {
    for source in reference["inputs"].as_array().ok_or("missing inputs")? {
        let names: Vec<String> = serde_json::from_value(source["columns"].clone())?;
        let types: Vec<String> = serde_json::from_value(source["types"].clone())?;
        let rows: Vec<Vec<Value>> = serde_json::from_value(source["rows"].clone())?;
        assert_eq!(names.len(), types.len());
        assert!(rows.iter().all(|row| row.len() == names.len()));
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (index, (name, data_type)) in names.iter().zip(&types).enumerate() {
            let values = json!(
                rows.iter()
                    .map(|row| row[index].clone())
                    .collect::<Vec<_>>()
            );
            let array: ArrayRef = match data_type.as_str() {
                "integer" => Arc::new(Int32Array::from(
                    serde_json::from_value::<Vec<Option<i32>>>(values)?,
                )),
                "long" => Arc::new(Int64Array::from(
                    serde_json::from_value::<Vec<Option<i64>>>(values)?,
                )),
                "string" => Arc::new(StringArray::from_iter(serde_json::from_value::<
                    Vec<Option<String>>,
                >(values)?)),
                "double" => {
                    let values: Vec<Option<String>> = serde_json::from_value(values)?;
                    let values = values
                        .iter()
                        .map(|v| v.as_deref().map(str::parse::<f64>).transpose())
                        .collect::<Result<Vec<_>, _>>()?;
                    Arc::new(Float64Array::from(values))
                }
                other => return Err(format!("unexpected input type: {other}").into()),
            };
            fields.push(Field::new(name, array.data_type().clone(), true));
            columns.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        let input = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let batches = if input.num_rows() == 0 {
            vec![input]
        } else {
            (0..input.num_rows())
                .step_by(batch_size)
                .map(|start| input.slice(start, batch_size.min(input.num_rows() - start)))
                .collect()
        };
        let mut partitions = vec![Vec::new(); partition_count];
        for (index, batch) in batches.into_iter().enumerate() {
            partitions[index % partition_count].push(batch);
        }
        session.register_table(
            source["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(schema, partitions)?),
        )?;
    }
    Ok(())
}

async fn execute_relational_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    let captured_schema = capture_schema(schema.fields(), None)?;
    let output_partitions = physical.output_partitioning().partition_count();
    let mut output_batches = Vec::new();
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        validate_batch(schema.as_ref(), &batch)?;
        output_batches.push(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for column in batch.columns() {
                let value = if column.is_null(row) {
                    Value::Null
                } else if column.data_type() == &DataType::Float64 {
                    let value = column.as_primitive::<Float64Type>().value(row);
                    json!(if value.is_nan() {
                        "nan".into()
                    } else if value == 0.0 {
                        "0".into()
                    } else {
                        value.to_string()
                    })
                } else {
                    encode_value(column.as_ref(), row)?
                };
                values.push(value);
            }
            rows.push(values);
        }
    }
    if case["ordered"] != true {
        rows.sort_by_key(|row| json!(row).to_string());
    }
    Ok(
        json!({"rows":rows,"schema":captured_schema,"output_partitions":output_partitions,"output_batches":output_batches}),
    )
}
