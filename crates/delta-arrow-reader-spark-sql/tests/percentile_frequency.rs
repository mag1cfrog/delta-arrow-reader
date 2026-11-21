//! Compare exact weighted numeric percentiles with independent Spark results.

use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

#[tokio::test]
async fn percentile_frequencies_match_spark_values_and_executed_types() -> TestResult {
    let references = include_str!("fixtures/percentile-frequency/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(references[0]["spark_version"], "4.2.0");
    assert_eq!(references.len() - 1, 52);
    let mut differences = Vec::new();
    for partitions in [1, 3] {
        for batch in [1, 32] {
            let session = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_batch_size(batch)
                    .with_target_partitions(partitions),
            );
            spark_inputs::register_recorded_inputs(&session, &references[0], batch)?;
            for case in &references[1..] {
                let mut actual = spark_results::capture_executed_query_result(&session, case)
                    .await
                    .unwrap_or_else(|error| json!({"error":error.to_string()}));
                let mut expected = json!({"rows":case["rows"],"schema":case["schema"]});
                spark_results::remove_nullability_and_metadata(&mut actual);
                spark_results::remove_nullability_and_metadata(&mut expected);
                println!(
                    "PERCENTILE_FREQUENCY {}",
                    json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,
                        "partitions":partitions,"actual":actual})
                );
                if actual != expected {
                    differences.push(format!(
                        "id={} ansi={} batch={batch} partitions={partitions}: expected {expected}; actual {actual}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_continuous_percentile_keeps_its_interpolation() -> TestResult {
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Float64Type};

    let batches = SessionContext::new()
        .sql(
            "SELECT PERCENTILE_CONT(v,0.25) AS r FROM (VALUES (1.0::DOUBLE),(9.0::DOUBLE)) AS t(v)",
        )
        .await?
        .collect()
        .await?;
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Float64);
    assert_eq!(
        batches[0].column(0).as_primitive::<Float64Type>().value(0),
        3.0
    );
    Ok(())
}

#[tokio::test]
async fn unweighted_double_percentiles_match_unit_frequency_in_window_and_plain_queries()
-> TestResult {
    let recorded_inputs = serde_json::from_str::<Value>(
        include_str!("fixtures/percentile-frequency/spark-results.jsonl")
            .lines()
            .next()
            .ok_or("Missing percentile inputs")?,
    )?;
    let session = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(4)
            .with_target_partitions(1),
    );
    spark_inputs::register_recorded_inputs(&session, &recorded_inputs, 4)?;
    for ansi in [false, true] {
        for (prefix, suffix) in [
            ("", " FROM samples"),
            ("", " FROM samples WHERE false"),
            (
                "id, ",
                " OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
            ),
            (
                "id, ",
                " OVER (PARTITION BY g ORDER BY id RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
            ),
            (
                "id, ",
                " OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
            ),
            ("id, ", " OVER (PARTITION BY g) FROM samples ORDER BY id"),
            (
                "id, ",
                " OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) FROM samples ORDER BY id",
            ),
        ] {
            let sql =
                |weight| format!("SELECT {prefix}percentile(v,array(0.0,0.5,1.0){weight}){suffix}");
            let compare = |statement| json!({"sql":statement,"ansi":ansi,"ordered":true});
            let direct =
                spark_results::capture_executed_query_result(&session, &compare(sql(""))).await?;
            let weighted = spark_results::capture_executed_query_result(
                &session,
                &compare(sql(",CAST(1 AS BIGINT)")),
            )
            .await?;
            assert_eq!(direct["rows"], weighted["rows"], "ANSI {ansi}: {suffix}");
            for result in [&direct, &weighted] {
                let percentile_field = result["schema"]["fields"]
                    .as_array()
                    .and_then(|fields| fields.last())
                    .ok_or("Missing percentile output field")?;
                assert_eq!(percentile_field["type"]["type"], "array");
                assert_eq!(percentile_field["type"]["elementType"], "double");
            }
        }
    }
    Ok(())
}
