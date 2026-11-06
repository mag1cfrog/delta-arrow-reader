//! Literal extraction preparation retains resolved Fields and native SQL isolation.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[expect(dead_code, reason = "This target captures complete failed-query observations.")]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation};

#[tokio::test]
async fn literal_group_errors_keep_resolved_fields_and_query_local_marker() -> TestResult {
    let references = include_str!("fixtures/regexp-extract-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let native_sql = "SELECT regexp_match('abc123','([0-9]+)') AS r";
    let plain = SessionContext::new();
    let native = plain.sql(native_sql).await?.collect().await?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    assert_eq!(session.sql(native_sql).await?.collect().await?, native);
    let mut checked = 0;
    for case in references.iter().skip(1).filter(|case| {
        case["id"] == "regexp_extract_literal_missing_index"
            || case["id"] == "regexp_extract_default_no_group"
    }) {
        assert_eq!(case["expected"]["phase"], "optimized_schema");
        let actual = capture_observation(&session, case).await?;
        assert_eq!(actual["analyzed_schema"], case["expected"]["analyzed_schema"]);
        assert!(actual["analyzed_arrow_schema"].is_string());
        assert_eq!(actual["error_stage"], "physical");
        assert_eq!(actual["physical_schema"], Value::Null);
        assert_eq!(actual["physical_arrow_schema"], Value::Null);
        assert!(actual.get("rows").is_none() && actual.get("optimized_schema").is_none());
        let count = case["expected"]["parameters"]["groupCount"]
            .as_str()
            .ok_or("missing group count")?;
        let index = case["expected"]["parameters"]["groupIndex"]
            .as_str()
            .ok_or("missing group index")?;
        let error = actual["error"].as_str().ok_or("missing error")?;
        assert!(error.starts_with("physical: "));
        assert!(
            error.ends_with(&format!(
                "Spark `regexp_extract` function: [INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] Expects group index between 0 and {count}, but got {index}"
            )),
            "{error}"
        );
        assert_eq!(session.sql(native_sql).await?.collect().await?, native);
        eprintln!("c06 literal preparation regression: {}", json!({
            "id":case["id"],"sql":case["sql"],"ansi":case["ansi"],"actual":actual,
            "full_compatibility_pass":false
        }));
        checked += 1;
    }
    assert_eq!(checked, 4);
    Ok(())
}
