//! Compare ordinary aggregate and window results across independent execution layouts.

use std::collections::BTreeSet;

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::{TestResult, encode_value};

fn load_spark_reference_records() -> TestResult<Vec<Value>> {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/aggregate-window-results/cases.json"))?;
    let records = include_str!("fixtures/aggregate-window-results/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["inputs"], manifest["inputs"]);
    let definitions = manifest["cases"].as_array().ok_or("missing cases")?;
    let mut expected = BTreeSet::new();
    for definition in definitions {
        let id = definition["id"].as_str().ok_or("missing case ID")?;
        for ansi in [false, true] {
            assert!(
                expected.insert((id.to_owned(), ansi)),
                "duplicate case: {id}"
            );
        }
    }
    let mut captured = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing reference ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(
            captured.insert((id.to_owned(), ansi)),
            "duplicate reference: {id}"
        );
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == id)
            .ok_or("unexpected reference ID")?;
        for key in ["sql", "ordered"] {
            assert_eq!(case[key], definition[key], "{id}: {key}");
        }
        assert!(
            case["schema"]["fields"].is_array(),
            "{id}: missing executed types"
        );
        assert!(case["rows"].is_array(), "{id}: missing rows");
    }
    assert_eq!(captured, expected);
    Ok(records)
}

#[tokio::test]
async fn ordinary_aggregates_and_windows_match_spark_results() -> TestResult {
    let references = load_spark_reference_records()?;
    let mut differences = Vec::new();
    // Change each input/execution setting alone, then exercise a distributed combination.
    for (provider_partitions, input_batch_size, target_partitions, output_batch_size) in [
        (1, 1, 1, 1),
        (3, 1, 1, 1),
        (1, 4, 1, 1),
        (1, 1, 3, 1),
        (1, 1, 1, 32),
        (2, 3, 3, 4),
    ] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(output_batch_size)
                            .with_target_partitions(target_partitions),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &references[0],
            input_batch_size,
            provider_partitions,
        )?;
        for case in &references[1..] {
            let mut actual = spark_results::capture_executed_query_result(&session, case)
                .await
                .unwrap_or_else(|error| json!({"error":error.to_string()}));
            let mut expected = json!({"rows":case["rows"],"schema":case["schema"]});
            spark_results::remove_nullability_and_metadata(&mut actual);
            spark_results::remove_nullability_and_metadata(&mut expected);
            println!(
                "AGGREGATE_WINDOW_RESULT {}",
                json!({"id":case["id"],"ansi":case["ansi"],
                    "provider_partitions":provider_partitions,"input_batch_size":input_batch_size,
                    "target_partitions":target_partitions,"output_batch_size":output_batch_size,
                    "actual":actual,"matches":actual==expected})
            );
            if actual != expected {
                differences.push(format!(
                    "id={} ANSI={} providers={provider_partitions} input_batch={input_batch_size} target={target_partitions} output_batch={output_batch_size}: expected {expected}; actual {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn load_regression_window_observations(input: &str) -> TestResult<Vec<Value>> {
    Ok(input
        .lines()
        .skip(1)
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

#[tokio::test]
async fn affected_regression_windows_match_spark_and_controls_match_dar() -> TestResult {
    let input: Value = serde_json::from_str(include_str!("../benches/functions_1406/input.json"))?;
    let fields = input["schema"]["fields"]
        .as_array()
        .ok_or("missing input fields")?;
    let positions = ["id", "grp", "p", "d"].map(|name| {
        fields
            .iter()
            .position(|field| field["name"] == name)
            .expect("missing benchmark column")
    });
    let bench = json!({
        "name":"bench",
        "schema":{"type":"struct","fields":positions.map(|position| &fields[position])},
        "rows":input["rows"].as_array().ok_or("missing input rows")?.iter()
            .map(|row| positions.map(|position| &row[position])).collect::<Vec<_>>(),
    });
    let reference = load_regression_window_observations(include_str!(
        "fixtures/regression-windows-1512/spark-results.jsonl"
    ))?;
    let baseline = load_regression_window_observations(include_str!(
        "fixtures/regression-windows-1512/dar-baseline.jsonl"
    ))?;
    assert_eq!(reference.len(), 42);
    assert_eq!(baseline.len(), reference.len());

    let mut differences = Vec::new();
    for batch_size in [8192, 4] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_target_partitions(1)
                            .with_batch_size(batch_size),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &json!({"inputs":[bench]}),
            batch_size,
            1,
        )?;

        for (spark, before) in reference.iter().zip(&baseline) {
            assert_eq!(spark["id"], before["id"]);
            assert_eq!(spark["ansi"], before["ansi"]);
            let sql = spark["sql"].as_str().ok_or("missing reference SQL")?;
            let case = spark["id"].as_str().ok_or("missing reference ID")?;
            let ansi_mode = spark["ansi"].as_bool().ok_or("missing ANSI mode")?;
            let query = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let physical = query.create_physical_plan(&session).await?;
            let schema = physical.schema();
            let expected_fields = spark["executed"].as_array().ok_or("missing Spark fields")?;
            assert_eq!(schema.fields().len(), expected_fields.len(), "{case}");
            let window = schema.field(0).name() == "id";
            let corrected = |name: &str| {
                window
                    && name != "id"
                    && matches!(
                        case.strip_prefix("original_").unwrap_or(name),
                        "regr_avgy"
                            | "regr_intercept"
                            | "regr_r2"
                            | "regr_slope"
                            | "regr_sxx"
                            | "regr_sxy"
                            | "regr_syy"
                    )
            };

            for (index, (field, spark_field)) in
                schema.fields().iter().zip(expected_fields).enumerate()
            {
                let name = field.name();
                assert_eq!(name.as_str(), spark_field["name"]);
                if corrected(name) {
                    assert_eq!(spark_field["type"], "double");
                    assert_eq!(field.data_type(), &DataType::Float64, "{case}/{name}");
                } else {
                    assert_eq!(
                        format!("{:?}", field.data_type()),
                        before["arrow_types"][index],
                        "{case}/{name}"
                    );
                }
            }

            let mut actual = Vec::new();
            for batch in collect(physical, session.task_ctx()).await? {
                assert_eq!(batch.schema().fields(), schema.fields(), "{case}");
                for row in 0..batch.num_rows() {
                    actual.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| encode_value(column.as_ref(), row))
                            .collect::<TestResult<Vec<_>>>()?,
                    );
                }
            }
            if window {
                actual.sort_by_key(|row| row[0].as_str().unwrap().parse::<i64>().unwrap());
            }
            let expected = spark["rows"].as_array().ok_or("missing Spark rows")?;
            let prior = before["rows"].as_array().ok_or("missing DAR rows")?;
            assert_eq!(actual.len(), expected.len(), "{case}");
            assert_eq!(actual.len(), prior.len(), "{case}");
            for (row, values) in actual.iter().enumerate() {
                for (column, (value, field)) in values.iter().zip(schema.fields()).enumerate() {
                    let name = field.name();
                    let expected = if corrected(name) {
                        &expected[row][column]
                    } else {
                        &prior[row][column]
                    };
                    if value != expected {
                        let id = if window {
                            values[0].to_string()
                        } else {
                            row.to_string()
                        };
                        differences.push(format!(
                            "{case}/{name} row {id}, ANSI {ansi_mode}, batch size {batch_size}: got {value}, expected {expected}"
                        ));
                    }
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
