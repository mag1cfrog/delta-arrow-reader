//! Temporary observation-only CAST IS NULL witness; no business-parity assertion.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

#[tokio::test]
async fn observe_floating_cast_is_null_preservation_796() -> TestResult {
    let path = std::env::var("DAR_796_CAST_IS_NULL_REFERENCE")?;
    let source = std::env::var("DAR_796_CAST_IS_NULL_SOURCE")?;
    if source.is_empty() {
        return Err("DAR_796_CAST_IS_NULL_SOURCE must identify this engine source".into());
    }
    let records = std::fs::read_to_string(&path)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 13, "header plus twelve independent references");
    let header = &records[0];
    assert_eq!(header["case_count"], 6);
    assert_eq!(header["observations"], 12);
    assert_eq!(header["native_observations"], 24);
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(
        header["manifest_sha256"],
        "82d80c94200af57b4e3aa15d373b54f9759dbfca8cce2e4194db14272b62d0e9"
    );
    assert_eq!(header["timezone"], "UTC");
    assert_eq!(header["native"]["batch_sizes"], json!([1, 32]));
    assert_eq!(header["inputs"].as_array().ok_or("missing inputs")?.len(), 5);
    let ids = [
        "double_huge_literal_is_null",
        "float_nan_literal_is_null",
        "float_required_column_is_null",
        "double_required_column_is_null",
        "double_nan_try_is_null",
        "double_fraction_literal_is_null",
    ];
    let expected = ids
        .iter()
        .flat_map(|id| [true, false].map(|ansi| ((*id).to_owned(), ansi)))
        .collect::<BTreeSet<_>>();
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing id")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(identities.insert((id.to_owned(), ansi)));
        assert_eq!(case["settings"]["spark.sql.ansi.enabled"], ansi.to_string());
        assert_eq!(case["settings"]["spark.sql.session.timeZone"], "UTC");
    }
    assert_eq!(identities, expected);

    let mut comparisons = 0;
    let mut contexts = BTreeSet::new();
    let mut observer_errors = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new().with_default_features().with_config(
                    SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
                ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs_with_slices(&session, header, batch_size)?;
        for case in &records[1..] {
            let observation = match spark_results::compare_exact_spark_query_stages_with_optimized(
                &session, case,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => json!({"observer_error":error.to_string()}),
            };
            if let Some(error) = observation.get("observer_error") {
                observer_errors.push(format!("{} ANSI={} batch={batch_size}: {error}", case["id"], case["ansi"]));
            } else if [
                "public_schema",
                "before_collect",
                "physical_schema",
                "rows",
                "rows_raw",
                "native_phase",
                "error",
                "optimized_schema",
                "optimized_observation",
                "optimized_error",
                "matches",
                "optimized_matches",
            ]
            .iter()
            .any(|key| observation.get(*key).is_none())
            {
                observer_errors.push(format!("{} ANSI={} batch={batch_size}: incomplete observer object", case["id"], case["ansi"]));
            }
            assert!(contexts.insert((case["id"].to_string(), case["ansi"].to_string(), batch_size)));
            eprintln!(
                "CAST_IS_NULL_796_OBSERVATION {}",
                json!({"source":&source,"source_type":case["source_type"],
                    "source_form":case["source_form"],"id":case["id"],"ansi":case["ansi"],
                    "batch":batch_size,"reference_path":&path,"reference":case,
                    "observation":observation})
            );
            comparisons += 1;
        }
    }
    eprintln!(
        "CAST_IS_NULL_796_SUMMARY {}",
        json!({"source":source,"references":12,"observations":comparisons,
            "unique_contexts":contexts.len(),"observer_errors":&observer_errors,
            "business_parity_asserted":false})
    );
    assert_eq!(comparisons, 24);
    assert_eq!(contexts.len(), 24);
    assert!(observer_errors.is_empty(), "{}", observer_errors.join("\n"));
    Ok(())
}
