//! Collection counts follow container nullability without changing native SQL.

use std::collections::BTreeSet;

use arrow::datatypes::{DataType, Field};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::error::PlanError;
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
#[expect(
    dead_code,
    reason = "Explicit one-partition registration leaves the default two-partition wrapper unused."
)]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_schema, encode_value};

const REFERENCE: &str = include_str!("fixtures/collection-length-fields/spark-reference.jsonl");
const ORIGINAL: &str = include_str!("fixtures/nested-lambda-captures-original-spark.jsonl");

fn create_collection_length_session(batch_size: usize) -> SessionContext {
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

fn assert_recorded_input_fidelity(input: &Value) -> TestResult<RecordBatch> {
    let batch = spark_inputs::decode_recorded_batch(input)?;
    let schema = batch.schema();
    assert_eq!(capture_schema(schema.fields(), None)?, input["schema"]);
    spark_results::validate_batch(schema.as_ref(), &batch)?;
    let rows = (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| encode_value(column.as_ref(), row))
                .collect::<TestResult<Vec<_>>>()
        })
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(json!(rows), input["rows"]);
    Ok(batch)
}

#[test]
fn recorded_collection_length_inputs_preserve_fields_and_rows() -> TestResult {
    for source in [REFERENCE, ORIGINAL] {
        let header: Value = serde_json::from_str(source.lines().next().ok_or("missing header")?)?;
        for input in header["inputs"].as_array().ok_or("missing inputs")? {
            assert_recorded_input_fidelity(input)?;
        }
    }
    Ok(())
}

#[test]
fn recorded_map_inputs_preserve_entries_nulls_and_nested_lists() -> TestResult {
    let map_with_nullable_values = json!({
        "type":"map", "keyType":"string", "valueType":"integer", "valueContainsNull":true,
    });
    let mut map_with_required_values = map_with_nullable_values.clone();
    map_with_required_values["valueContainsNull"] = json!(false);
    let input = json!({
        "schema":{"type":"struct", "fields":[
            {"name":"map", "type":map_with_nullable_values,
                "nullable":true, "metadata":{"source":"map"}},
            {"name":"maps", "type":{"type":"array", "elementType":map_with_nullable_values,
                "containsNull":true}, "nullable":true, "metadata":{}},
            {"name":"strict_maps", "type":{"type":"array", "elementType":map_with_required_values,
                "containsNull":false}, "nullable":false, "metadata":{}}
        ]},
        "rows":[
            [[["z","1"],["a",null],["z","2"]], [null,[],[["b",null]]], [[["q","3"]]]],
            [null,null,[[]]],
            [[],[],[]]
        ]
    });
    let batch = assert_recorded_input_fidelity(&input)?;
    let schema = batch.schema();
    let DataType::Map(entries, ordered) = schema.field(0).data_type() else {
        return Err("expected Map field".into());
    };
    assert!(!ordered);
    assert_eq!(
        entries.as_ref(),
        &Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        )
    );
    for (column, contains_null) in [(1, true), (2, false)] {
        let DataType::List(item) = schema.field(column).data_type() else {
            return Err("expected List field".into());
        };
        assert_eq!(item.name(), "item");
        assert_eq!(item.is_nullable(), contains_null);
        assert!(item.metadata().is_empty());
    }
    for (column, invalid_value) in [
        (0, json!([[null, "1"]])),
        (0, json!([null])),
        (2, json!([[["q", null]]])),
    ] {
        let mut invalid = input.clone();
        invalid["rows"][0][column] = invalid_value;
        assert!(spark_inputs::decode_recorded_batch(&invalid).is_err());
    }
    Ok(())
}

fn matches_collection_length_type_error(error: &DataFusionError, id: &str) -> bool {
    let DataFusionError::External(source) = error else {
        return false;
    };
    let Some(source) = source.downcast_ref::<PlanError>() else {
        return false;
    };
    if !id.starts_with("array_size_") {
        return matches!(source, PlanError::InvalidArgument(message)
            if message == "size expects List or Map as argument, got Int32");
    }
    let PlanError::DataFusionError(error) = source else {
        return false;
    };
    let DataFusionError::Plan(message) = error.find_root() else {
        return false;
    };
    let Some(details) = message.strip_prefix(
        "Failed to coerce arguments to satisfy a call to 'array_length' function: coercion from ",
    ) else {
        return false;
    };
    let Some((argument, signature)) = details.split_once(" to the signature ") else {
        return false;
    };
    let correct_type = if matches!(id, "array_size_map_literals" | "array_size_invalid_map") {
        argument.starts_with("Map(")
    } else {
        argument == "Int32"
    };
    let Some(help) =
        signature.strip_prefix("OneOf(ArraySignature(array), ArraySignature(array, index)) failed")
    else {
        return false;
    };
    correct_type
        && (help.is_empty()
            || help.starts_with(
                ". No function matches the given name and argument types 'array_length(",
            ))
}

fn matches_bad_integer_cast(error: &DataFusionError) -> bool {
    matches!(error.find_root(), DataFusionError::ArrowError(error, _)
        if matches!(error.as_ref(), ArrowError::CastError(message)
            if message == "Cannot cast string 'bad' to value of Int32 type"))
}

async fn observe_collection_length_query(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    let mut phase = "resolve";
    let mut public = None;
    let mut public_raw = None;
    let mut optimized = None;
    let mut optimized_raw = None;
    let mut optimized_names_match = false;
    let mut optimized_error = None;
    let mut before = None;
    let mut executed = None;
    let mut physical_raw = None;
    let mut rows_raw = None;
    let actual: TestResult<Vec<Vec<Value>>> = async {
        let mut options = SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        };
        if let Some(legacy) = case.get("legacy_size_of_null") {
            options.legacy_size_of_null = legacy.as_bool().ok_or("missing legacy mode")?;
        }
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options,
        )
        .await?;
        phase = "public_schema";
        assert_eq!(
            query.column_names.len(),
            query.logical_plan.schema().fields().len()
        );
        public_raw = Some(format!("{:?}", query.logical_plan.schema()));
        public = Some(capture_schema(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        )?);
        let mut state = session.state();
        let extensions = &mut state.config_mut().options_mut().extensions;
        extensions.insert(SparkSqlExpressionSemantics::default());
        extensions.insert(PreserveAggregateStatisticsSchema::default());
        // Observe optimization separately so its errors do not replace the physical-plan phase.
        match state.optimize(&query.logical_plan) {
            Ok(plan) => {
                optimized_names_match = plan.schema().fields().len() == query.column_names.len()
                    && plan
                        .schema()
                        .fields()
                        .iter()
                        .zip(query.logical_plan.schema().fields())
                        .all(|(actual, resolved)| actual.name() == resolved.name());
                optimized_raw = Some(capture_schema(plan.schema().fields(), None)?);
                if optimized_names_match {
                    optimized = Some(capture_schema(
                        plan.schema().fields(),
                        Some(&query.column_names),
                    )?);
                }
            }
            Err(error) => optimized_error = Some(format!("{error:?}")),
        }
        phase = "create_physical_plan";
        let physical = query.create_physical_plan(session).await?;
        let schema = physical.schema();
        phase = "physical_schema";
        before = Some(capture_schema(schema.fields(), None)?);
        physical_raw = Some(format!("{schema:?}"));
        let batches = collect(physical.clone(), session.task_ctx()).await;
        executed = Some(capture_schema(physical.schema().fields(), None)?);
        phase = "collect";
        let batches = batches?;
        phase = "encode";
        let mut rows = Vec::new();
        for batch in batches {
            spark_results::validate_batch(schema.as_ref(), &batch)?;
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
        rows_raw = Some(rows.clone());
        rows.sort_by_key(|row| json!(row).to_string());
        Ok(rows)
    }
    .await;
    let matches = if case["phase"] == "success" {
        public.as_ref() == case["expected"].get("schema")
            && public.as_ref() == case["schema_stages"].get("analyzed")
            && optimized_names_match
            && optimized.as_ref() == case["schema_stages"].get("optimized")
            && before.as_ref() == case["schema_stages"].get("executed")
            && executed.as_ref() == case["schema_stages"].get("executed")
            && actual
                .as_ref()
                .is_ok_and(|rows| json!(rows) == case["expected"]["rows"])
    } else {
        actual
            .as_ref()
            .err()
            .and_then(|error| error.downcast_ref::<DataFusionError>())
            .is_some_and(|error| {
                match (
                    case["phase"].as_str(),
                    case["expected"]["condition"].as_str(),
                ) {
                    (Some("planning"), Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")) => {
                        phase == "resolve"
                            && matches_collection_length_type_error(
                                error,
                                case["id"].as_str().unwrap_or(""),
                            )
                    }
                    // Spark's collect action includes lazy optimization and physical preparation.
                    (Some("execution"), Some("CAST_INVALID_INPUT")) => {
                        matches!(phase, "create_physical_plan" | "collect")
                            && matches_bad_integer_cast(error)
                    }
                    _ => false,
                }
            })
    };
    let mut observation = json!({
        "id":case["id"], "input":case["input"], "ansi":case["ansi"],
        "sql":case["sql"],
        "public_schema":public, "logical_schema_raw":public_raw,
        "optimized_schema":optimized, "optimized_schema_raw":optimized_raw,
        "optimized_names_match":optimized_names_match, "optimized_error":optimized_error,
        "before_collect":before, "physical_schema":executed, "physical_schema_raw":physical_raw,
        "rows":actual.as_ref().ok(), "rows_raw":rows_raw, "native_phase":phase,
        "error":actual.as_ref().err().map(ToString::to_string),
        "error_debug":actual.as_ref().err().map(|error| format!("{error:?}")), "matches":matches,
    });
    if let Some(legacy) = case.get("legacy_size_of_null") {
        observation["legacy_size_of_null"] = legacy.clone();
    }
    Ok(observation)
}

#[test]
fn collection_length_error_checks_reject_unrelated_native_causes() {
    let scalar = || {
        DataFusionError::External(Box::new(PlanError::InvalidArgument(
            "size expects List or Map as argument, got Int32".into(),
        )))
    };
    assert!(matches_collection_length_type_error(
        &scalar(),
        "size_invalid_scalar"
    ));
    assert!(!matches_collection_length_type_error(
        &scalar(),
        "array_size_invalid_scalar"
    ));
    assert!(!matches_collection_length_type_error(
        &DataFusionError::Plan("size expects List or Map as argument, got Int32".into()),
        "size_invalid_scalar",
    ));
    assert!(!matches_collection_length_type_error(
        &DataFusionError::External(Box::new(PlanError::AnalysisError(
            "size expects List or Map as argument, got Int32".into(),
        ))),
        "size_invalid_scalar",
    ));
    let coercion = "Failed to coerce arguments to satisfy a call to 'array_length' function: coercion from Int32 to the signature OneOf(ArraySignature(array), ArraySignature(array, index)) failed. No function matches the given name and argument types 'array_length(Int32)'.";
    let native = |message| {
        DataFusionError::External(Box::new(PlanError::DataFusionError(DataFusionError::Plan(
            message,
        ))))
    };
    assert!(matches_collection_length_type_error(
        &native(coercion.into()),
        "array_size_invalid_scalar",
    ));
    assert!(!matches_collection_length_type_error(
        &native(coercion.into()),
        "array_size_invalid_map",
    ));
    for message in [
        coercion.replace("array_length", "other_function"),
        coercion.replace("Int32", "Int64"),
    ] {
        assert!(!matches_collection_length_type_error(
            &native(message),
            "array_size_invalid_scalar",
        ));
    }
    let cast = "Cannot cast string 'bad' to value of Int32 type";
    assert!(matches_bad_integer_cast(&DataFusionError::ArrowError(
        Box::new(ArrowError::CastError(cast.into())),
        None,
    )));
    for error in [
        DataFusionError::Plan(cast.into()),
        DataFusionError::Execution(cast.into()),
        DataFusionError::ArrowError(
            Box::new(ArrowError::CastError(cast.replace("Int32", "Int64"))),
            None,
        ),
        DataFusionError::ArrowError(
            Box::new(ArrowError::CastError(cast.replace("bad", "other"))),
            None,
        ),
        DataFusionError::Execution("unrelated failure".into()).context(cast),
    ] {
        assert!(!matches_bad_integer_cast(&error));
    }
}

#[tokio::test]
async fn collection_length_fields_counts_and_action_errors_match_spark() -> TestResult {
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 144);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"]
            .as_str()
            .ok_or("missing Spark build")?
            .contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    );
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["case_sensitive"], false);
    assert_eq!(records[0]["default_legacy_size_of_null"], "true");
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["input"].as_str().ok_or("missing provider")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                case["legacy_size_of_null"]
                    .as_bool()
                    .ok_or("missing legacy mode")?,
            ))
        })
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(identities.len(), 144);
    for (phase, count) in [("success", 98), ("planning", 40), ("execution", 6)] {
        assert_eq!(
            records[1..]
                .iter()
                .filter(|case| case["phase"] == phase)
                .count(),
            count
        );
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = create_collection_length_session(batch_size);
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &records[0],
            batch_size,
            1,
        )?;
        let config_before = format!("{:?}", session.state().config_options());
        for case in &records[1..] {
            let mut observed = observe_collection_length_query(&session, case).await?;
            observed["batch"] = json!(batch_size);
            eprintln!("COLLECTION_LENGTH_OBSERVATION {observed}");
            if observed["matches"] != true {
                differences.push(format!(
                    "batch={batch_size}, expected={case}, actual={observed}"
                ));
            }
        }
        assert_eq!(
            config_before,
            format!("{:?}", session.state().config_options())
        );
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn original_size_transform_consumers_match_all_six_spark_records() -> TestResult {
    let records = ORIGINAL
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 92);
    let cases: Vec<_> = records[1..]
        .iter()
        .filter(|case| case["id"] == "scalar_consumer")
        .collect();
    assert_eq!(cases.len(), 6);
    assert_eq!(cases.iter().filter(|case| case["ansi"] == true).count(), 3);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = create_collection_length_session(batch_size);
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &records[0],
            batch_size,
            1,
        )?;
        for case in &cases {
            assert!(case.get("legacy_size_of_null").is_none());
            let mut observed = observe_collection_length_query(&session, case).await?;
            assert!(observed.get("legacy_size_of_null").is_none());
            observed["batch"] = json!(batch_size);
            eprintln!("COLLECTION_LENGTH_ORIGINAL_OBSERVATION {observed}");
            if observed["matches"] != true {
                differences.push(format!(
                    "batch={batch_size}, expected={case}, actual={observed}"
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn observe_native_collection_lengths(
    session: &SessionContext,
    sql: &str,
) -> TestResult<Value> {
    let frame = session.sql(sql).await?;
    let public = format!("{:?}", frame.schema());
    let physical = frame.create_physical_plan().await?;
    let schema = physical.schema();
    for field in schema.fields().iter().filter(|field| field.name() != "id") {
        assert_eq!(field.data_type(), &DataType::UInt64);
        assert!(field.metadata().is_empty());
    }
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        spark_results::validate_batch(schema.as_ref(), &batch)?;
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
    Ok(json!({
        "public_schema_raw":public, "physical_schema_raw":format!("{schema:?}"), "rows":rows,
    }))
}

#[tokio::test]
async fn spark_collection_lengths_preserve_native_counts_fields_and_session_config() -> TestResult {
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for batch_size in [1, 32] {
        let native =
            SessionContext::new_with_config(SessionConfig::new().with_batch_size(batch_size));
        let configured = create_collection_length_session(batch_size);
        for session in [&native, &configured] {
            spark_inputs::register_recorded_inputs_with_partition_count(
                session,
                &records[0],
                batch_size,
                1,
            )?;
        }
        let configuration = format!("{:?}", configured.state().config_options());
        let queries = [
            (
                "SELECT id,array_length(required_array) AS array_count,cardinality(required_map) AS map_count FROM size_fields_mixed",
                json!([
                    ["0", "2", "2"],
                    ["1", "0", "0"],
                    ["2", "1", "1"],
                    ["3", "2", "1"]
                ]),
            ),
            (
                "SELECT array_length([[1,2],[3,4]],2) AS dimension_count,cardinality([[1,2],[3,4]]) AS deep_count",
                json!([["2", "4"]]),
            ),
        ];
        for after_spark in [false, true] {
            if after_spark {
                let observed = observe_collection_length_query(&configured, &records[1]).await?;
                assert!(observed["error"].is_null());
                assert_eq!(observed["rows"], records[1]["expected"]["rows"]);
            }
            for (sql, expected_rows) in &queries {
                let expected = observe_native_collection_lengths(&native, sql).await?;
                let actual = observe_native_collection_lengths(&configured, sql).await?;
                assert_eq!(
                    actual, expected,
                    "{sql}, batch={batch_size}, after Spark={after_spark}"
                );
                assert_eq!(&actual["rows"], expected_rows);
                eprintln!(
                    "COLLECTION_LENGTH_NATIVE_CONTROL {}",
                    json!({
                        "sql":sql, "batch":batch_size, "after_spark":after_spark, "observation":actual,
                    })
                );
            }
        }
        let configuration_after = format!("{:?}", configured.state().config_options());
        assert_eq!(configuration, configuration_after);
        eprintln!(
            "COLLECTION_LENGTH_CONFIGURATION_CONTROL {}",
            json!({
                "batch":batch_size, "before":configuration, "after":configuration_after,
            })
        );
    }
    Ok(())
}
