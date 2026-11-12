//! Compare approximate percentile values and executed types with Spark.

use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

#[tokio::test]
async fn approximate_percentiles_match_spark_ranks_and_numeric_types() -> TestResult {
    let references = include_str!("fixtures/approx-percentile-ranks/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(references[0]["spark_version"], "4.2.0");
    assert_eq!(references.len() - 1, 140);
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(batch)
                .with_target_partitions(2),
        );
        spark_inputs::register_recorded_inputs(&session, &references[0], batch)?;
        let single_partition_session = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(batch)
                .with_target_partitions(1),
        );
        for case in references[1..].iter().filter(|case| {
            !case["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("compressed_"))
        }) {
            let case_session = if case["single_partition"] == true {
                &single_partition_session
            } else {
                &session
            };
            let mut actual = spark_results::capture_executed_query_result(case_session, case)
                .await
                .unwrap_or_else(|error| json!({"error":error.to_string()}));
            let mut expected = json!({"rows":case["rows"],"schema":case["schema"]});
            spark_results::remove_nullability_and_metadata(&mut actual);
            spark_results::remove_nullability_and_metadata(&mut expected);
            println!(
                "APPROX_PERCENTILE {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,"actual":actual})
            );
            if actual != expected {
                differences.push(format!(
                    "id={} ansi={} batch={batch}: expected {expected}; actual {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn compressed_percentiles_match_spark_source_partition_topology() -> TestResult {
    let references = include_str!("fixtures/approx-percentile-ranks/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let mut differences = Vec::new();
    let mut checked = 0;
    for batch in [32, 8192] {
        for case in references[1..].iter().filter(|case| {
            case["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("compressed_p"))
        }) {
            let source_partitions = if case["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("compressed_p1_"))
            {
                1
            } else {
                2
            };
            // Preserve Spark's partial-aggregate topology when the optimizer repartitions sources.
            let session = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_batch_size(batch)
                    .with_target_partitions(source_partitions),
            );
            let mut actual = spark_results::capture_executed_query_result(&session, case).await?;
            let mut expected = json!({"rows":case["rows"],"schema":case["schema"]});
            spark_results::remove_nullability_and_metadata(&mut actual);
            spark_results::remove_nullability_and_metadata(&mut expected);
            println!(
                "APPROX_COMPRESSED {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,"actual":actual})
            );
            if actual != expected {
                differences.push(format!(
                    "id={} ansi={} batch={batch}: expected {expected}; actual {actual}",
                    case["id"], case["ansi"]
                ));
            }
            checked += 1;
        }
    }
    assert_eq!(checked, 24);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_datafusion_continuous_percentile_still_interpolates() -> TestResult {
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Float64Type};
    let session = SessionContext::new();
    let batches = session.sql("SELECT APPROX_PERCENTILE_CONT(v,0.5) AS r FROM (VALUES (1.0::DOUBLE),(2.0::DOUBLE),(3.0::DOUBLE),(4.0::DOUBLE)) AS t(v)").await?.collect().await?;
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Float64);
    assert_eq!(
        batches[0].column(0).as_primitive::<Float64Type>().value(0),
        2.5
    );
    Ok(())
}

#[tokio::test]
async fn repartitioned_percentiles_keep_spark_rank_accuracy_and_input_membership() -> TestResult {
    let references = include_str!("fixtures/approx-percentile-ranks/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let probabilities = [0.0, 0.01, 0.25, 0.5, 0.51, 0.9, 1.0];
    let mut checked = 0;
    for configured_target in [Some(2), None] {
        for batch in [32, 8192] {
            let config = SessionConfig::new().with_batch_size(batch);
            let config = if let Some(target) = configured_target {
                config.with_target_partitions(target)
            } else {
                config
            };
            let session = SessionContext::new_with_config(config);
            for case in references[1..].iter().filter(|case| {
                case["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("compressed_p1_") || id.contains("_p3_"))
            }) {
                let id = case["id"].as_str().ok_or("missing ID")?;
                let accuracy: f64 = id
                    .rsplit_once("accuracy")
                    .ok_or("missing accuracy")?
                    .1
                    .parse()?;
                let error = 1.0 / accuracy;
                let query = delta_arrow_reader_spark_sql::plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &delta_arrow_reader_spark_sql::SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        ..Default::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                println!(
                    "APPROX_REPARTITIONED_PLAN {}",
                    json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,"configured_target":configured_target,"target_partitions":session.state().config().target_partitions(),"plan":datafusion::physical_plan::displayable(physical.as_ref()).indent(true).to_string()})
                );
                let mut actual =
                    spark_results::capture_executed_query_result(&session, case).await?;
                let mut expected_schema = case["schema"].clone();
                spark_results::remove_nullability_and_metadata(&mut actual);
                spark_results::remove_nullability_and_metadata(&mut expected_schema);
                assert_eq!(actual["schema"], expected_schema);
                let values = actual["rows"][0][0]
                    .as_array()
                    .ok_or("missing probabilities")?;
                assert_eq!(values.len(), probabilities.len());
                for (encoded, probability) in values.iter().zip(probabilities) {
                    let value = f64::from_bits(u64::from_str_radix(
                        encoded.as_str().ok_or("missing DOUBLE bits")?,
                        16,
                    )?);
                    assert!(
                        value.is_finite()
                            && value.fract() == 0.0
                            && (0.0..=if id.contains("_p3_") {
                                180000.0
                            } else {
                                120000.0
                            })
                                .contains(&value),
                        "{id}: result must be an input value"
                    );
                    // Spark's GK contract: floor((p-error)*N) <= rank(x) <= ceil((p+error)*N).
                    let size = if id.contains("_p3_") {
                        180001.0
                    } else {
                        120001.0
                    };
                    let rank = value + 1.0;
                    assert!(
                        ((probability - error) * size).floor() <= rank
                            && rank <= ((probability + error) * size).ceil(),
                        "{id}: p={probability}, rank={rank}, error={error}"
                    );
                }
                println!(
                    "APPROX_REPARTITIONED {}",
                    json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,"configured_target":configured_target,"target_partitions":session.state().config().target_partitions(),"actual":actual})
                );
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 40);
    Ok(())
}
