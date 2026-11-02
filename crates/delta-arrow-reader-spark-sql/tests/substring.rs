//! Unicode and binary substring boundaries match independent Spark observations.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_result, register_inputs};

#[tokio::test]
async fn substring_matches_spark() -> TestResult {
    let mut differences = Vec::new();
    let mut compared = 0;
    for (capture, count) in [
        (include_str!("fixtures/substring-spark.jsonl"), 304),
        (
            include_str!("fixtures/substring-boundaries-spark.jsonl"),
            194,
        ),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records.len() - 1, count);
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = SessionContext::new_with_state(
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
                );
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    compared += 1;
                    let actual = capture_result(&session, case).await;
                    let observed = match &actual {
                        Ok(value) => value.clone(),
                        Err(error) => json!({"error":error.to_string()}),
                    };
                    eprintln!(
                        "c06 observation: {}",
                        json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"actual":observed})
                    );
                    let expected = &case["expected"];
                    let matches = match expected["condition"].as_str() {
                        Some(condition) => actual.as_ref().is_err_and(|error| {
                            error.to_string().contains(&format!("[{condition}]"))
                        }),
                        None => actual.as_ref().is_ok_and(|value| {
                            value["analyzed_schema"] == expected["analyzed_schema"]
                                && value["physical_schema"] == expected["physical_schema"]
                                && value["rows"] == expected["rows"]
                        }),
                    };
                    if !matches {
                        differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["id"],case["ansi"]));
                    }
                }
            }
        }
    }
    assert_eq!(compared, (304 + 194) * 6);
    eprintln!("C06 compared {compared}, mismatches {}", differences.len());
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn substring_preserves_native_sql_and_fixed_binary_inputs() -> TestResult {
    use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let native_sql =
        "SELECT substr('abcdef',-10,6) AS a,substr('abcdef',0,2) AS b,concat('a',NULL) AS c";
    let plain = SessionContext::new();
    let expected = plain.sql(native_sql).await?.collect().await?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    let values = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [
            Some([0u8, 0xff, 0x61]),
            Some([0, 0xff, 0x61]),
            None,
            Some([0, 0xff, 0x61]),
            Some([0, 0xff, 0x61]),
        ]
        .into_iter(),
        3,
    )?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("b", DataType::FixedSizeBinary(3), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
            Arc::new(values),
        ],
    )?;
    let sliced = batch.slice(1, 3);
    assert_eq!(sliced.column(1).null_count(), 1);
    session.register_batch("fixed_binary", sliced.clone())?;
    session.register_batch("fixed_empty", sliced.slice(0, 0))?;
    for ansi in [true, false] {
        let result = capture_result(
            &session,
            &json!({"sql":"SELECT substring(b,2,2) AS r FROM fixed_binary ORDER BY k","ansi":ansi}),
        )
        .await?;
        // The independent nul_ff_a_middle reference observes these same byte boundaries.
        assert_eq!(result["rows"], json!([["ff61"], [null], ["ff61"]]));
        let expected_schema = json!({"type":"struct","fields":[{"name":"r","type":"binary","nullable":true,"metadata":{}}]});
        assert_eq!(result["analyzed_schema"], expected_schema);
        assert_eq!(result["physical_schema"], expected_schema);
        let empty = capture_result(
            &session,
            &json!({"sql":"SELECT substring(b,2,2) AS r FROM fixed_empty","ansi":ansi}),
        )
        .await?;
        assert_eq!(empty["rows"], json!([]));
        assert_eq!(empty["analyzed_schema"], expected_schema);
        assert_eq!(empty["physical_schema"], expected_schema);
    }
    assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    Ok(())
}
