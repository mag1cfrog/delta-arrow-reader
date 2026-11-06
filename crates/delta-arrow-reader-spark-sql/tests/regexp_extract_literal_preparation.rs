//! Literal extraction preparation retains resolved Fields and native SQL isolation.

use datafusion::arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[expect(
    dead_code,
    reason = "This target captures complete failed-query observations."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation};

fn assert_query_marker_absent(session: &SessionContext) {
    assert!(
        session
            .state()
            .config_options()
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_none()
    );
}

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
    assert_query_marker_absent(&session);
    assert_eq!(session.sql(native_sql).await?.collect().await?, native);
    let mut checked = 0;
    for case in references.iter().skip(1).filter(|case| {
        case["id"] == "regexp_extract_literal_missing_index"
            || case["id"] == "regexp_extract_default_no_group"
    }) {
        assert_eq!(case["expected"]["phase"], "optimized_schema");
        assert_query_marker_absent(&session);
        let actual = capture_observation(&session, case).await?;
        assert_query_marker_absent(&session);
        assert_eq!(
            actual["analyzed_schema"],
            case["expected"]["analyzed_schema"]
        );
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
        eprintln!(
            "c06 literal preparation regression: {}",
            json!({
                "id":case["id"],"sql":case["sql"],"ansi":case["ansi"],"actual":actual,
                "full_compatibility_pass":false
            })
        );
        checked += 1;
    }
    assert_eq!(checked, 4);
    Ok(())
}

#[tokio::test]
async fn set_resolution_keeps_literal_fields_before_preparation_failure() -> TestResult {
    let references = include_str!("fixtures/regexp-extract-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let mut checked = 0;
    for case in references
        .iter()
        .skip(1)
        .filter(|case| case["id"] == "regexp_extract_literal_missing_index")
    {
        assert_query_marker_absent(&session);
        let sql = format!(
            "{} UNION SELECT 'b' AS r",
            case["sql"].as_str().ok_or("missing SQL")?
        );
        let query = plan_query(
            &session,
            &sql,
            &SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                ..Default::default()
            },
        )
        .await?;
        let resolved = query.logical_plan.schema().clone();
        assert_eq!(resolved.fields().len(), 1);
        assert_eq!(resolved.field(0).data_type(), &DataType::Utf8);
        assert_eq!(query.column_names, vec!["r".to_string()]);
        assert_query_marker_absent(&session);
        let error = match query.create_physical_plan(&session).await {
            Ok(_) => return Err("expected literal group preparation failure".into()),
            Err(error) => error.to_string(),
        };
        assert_eq!(query.logical_plan.schema(), &resolved);
        assert_query_marker_absent(&session);
        let count = case["expected"]["parameters"]["groupCount"]
            .as_str()
            .ok_or("missing group count")?;
        let index = case["expected"]["parameters"]["groupIndex"]
            .as_str()
            .ok_or("missing group index")?;
        assert!(
            error.ends_with(&format!(
                "Spark `regexp_extract` function: [INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] Expects group index between 0 and {count}, but got {index}"
            )),
            "{error}"
        );
        // The set-op exercises resolver simplification; only the underlying
        // literal group cause is taken from the independent Spark reference.
        eprintln!(
            "c06 literal set resolution: {}",
            json!({
                "sql":sql,"ansi":case["ansi"],"resolved_fields":format!("{resolved:?}"),
                "column_names":query.column_names,"physical_error":error,"spark_set_oracle":false
            })
        );
        checked += 1;
    }
    assert_eq!(checked, 2);
    Ok(())
}
