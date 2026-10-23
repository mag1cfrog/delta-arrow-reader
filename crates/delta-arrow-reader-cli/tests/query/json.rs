//! Typed query results, bounded stdout, and recovery through the public CLI.

use serde_json::{Value, json};

use super::*;
use crate::capabilities::assert_output_matches_schema;

pub(crate) fn assert_query_success_and_decode_json(output: &Output) -> TestResult<Value> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout)?;
    assert_output_matches_schema("rows", &result);
    let completion = assert_single_terminal_record(&output.stderr, "completion");
    assert_output_matches_schema("completion", &completion);
    assert_eq!(completion["format_version"], 2);
    assert_eq!(completion["command"], "query");
    for field in [
        "batches",
        "stop_reason",
        "has_more",
        "scope",
        "completion",
        "terminal",
    ] {
        assert!(completion.get(field).is_none(), "{field}");
    }
    assert_eq!(result["completion"], "complete");
    assert_eq!(result["scope"], "query");
    assert!(result.get("stop_reason").is_none());
    assert!(result.get("has_more").is_none());
    assert_eq!(completion["rows"], result["row_count"]);
    assert_eq!(completion["bindings"], result["bindings"]);
    assert_eq!(completion["output_bytes"], output.stdout.len().to_string());
    assert_eq!(
        result["row_count"],
        result["rows"].as_array().unwrap().len().to_string()
    );
    Ok(result)
}

#[test]
fn json_queries_preserve_exact_values_and_snapshot_selection() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT CAST('9007199254740993' AS BIGINT) AS id, CAST('-12.34' AS DECIMAL(10,2)) AS amount, TIMESTAMP '2020-01-01 00:00:00' AS at FROM t LIMIT 1",
    )?;
    let result = assert_query_success_and_decode_json(&run_with_timeout(
        fixture.command().args(["--format", "json"]),
    )?)?;
    assert_eq!(
        result["rows"],
        json!([["9007199254740993", "-1234", "1577836800000000000"]])
    );
    assert_eq!(
        result["schema"]["fields"][1]["data_type"],
        json!({"Decimal128": [10, 2]})
    );
    assert_eq!(
        result["schema"]["fields"][2]["data_type"],
        json!({"Timestamp": ["Nanosecond", null]})
    );
    assert!(!result["schema"].to_string().contains("metadata"));

    for (version, expected_rows) in [
        ("0", 12),
        ("1", read_expected_rows("deletion_vectors")?.num_rows()),
    ] {
        let fixture = SqlCommandFixture::new(
            fixture_path("deletion_vectors").join("table"),
            "SELECT id FROM t ORDER BY id",
        )?;
        let output = run_with_timeout(fixture.command().args([
            "--format",
            "json",
            "--table-version",
            version,
        ]))?;
        let result = assert_query_success_and_decode_json(&output)?;
        assert_eq!(result["bindings"][0]["table_version"], version);
        assert_eq!(result["rows"].as_array().unwrap().len(), expected_rows);
        let arrow = assert_query_success_and_decode_arrow(&run_with_timeout(
            fixture.command().args(["--table-version", version]),
        )?)?;
        let values = arrow
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            result["rows"],
            json!(
                values
                    .values()
                    .iter()
                    .map(|value| vec![value.to_string()])
                    .collect::<Vec<_>>()
            )
        );
    }
    Ok(())
}

#[test]
fn json_limits_accept_exact_results_and_never_return_successful_prefixes() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT id FROM t ORDER BY id LIMIT 3",
    )?;
    let output = run_with_timeout(
        fixture
            .command()
            .args(["--format", "json", "--max-rows", "3"]),
    )?;
    assert_query_success_and_decode_json(&output)?;
    let exact_bytes = output.stdout.len();
    let exact = run_with_timeout(fixture.command().args([
        "--format",
        "json",
        "--max-output-bytes",
        &exact_bytes.to_string(),
    ]))?;
    assert_eq!(
        assert_query_success_and_decode_json(&exact)?,
        assert_query_success_and_decode_json(&output)?
    );
    for (flag, value, reason) in [
        (
            "--max-output-bytes",
            (exact_bytes - 1).to_string(),
            "json_output_limit_exceeded",
        ),
        (
            "--max-output-bytes",
            "1".to_owned(),
            "json_output_limit_exceeded",
        ),
        ("--max-rows", "2".to_owned(), "output_row_limit_exceeded"),
        ("--max-rows", "0".to_owned(), "output_row_limit_exceeded"),
    ] {
        let output = run_with_timeout(fixture.command().args(["--format", "json", flag, &value]))?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let diagnostic = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(diagnostic["reason"], reason);
        assert_eq!(
            diagnostic["bindings"],
            json!([{"alias": "t", "table_version": "0"}])
        );
        assert_eq!(diagnostic["recovery"], "adjust_budget");
        assert_eq!(diagnostic["argument"], flag);
        if reason == "json_output_limit_exceeded" {
            let hint = diagnostic["hint"].as_str().unwrap();
            assert!(hint.contains("--format arrow"));
            assert!(hint.contains("Explicitly increase --max-output-bytes for the complete JSON"));
            assert!(hint.contains("redirected stdout and an explicit byte budget"));
            assert!(
                hint.contains("same SQL, authorization, table bindings, and snapshot versions")
            );
            assert!(!hint.contains("aggregate"));
        } else {
            let hint = diagnostic["hint"].as_str().unwrap();
            assert!(hint.contains("Explicitly increase --max-rows"));
            assert!(
                hint.contains("same SQL, authorization, table bindings, and snapshot versions")
            );
            assert!(!hint.contains("LIMIT"));
            let recovered = run_with_timeout(fixture.command().args([
                "--format",
                "json",
                diagnostic["argument"].as_str().unwrap(),
                "3",
            ]))?;
            assert_eq!(
                assert_query_success_and_decode_json(&recovered)?["rows"],
                json!([["1"], ["2"], ["3"]])
            );
        }
    }
    let empty = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT id FROM t WHERE false",
    )?;
    let result = assert_query_success_and_decode_json(&run_with_timeout(empty.command().args([
        "--format",
        "json",
        "--max-rows",
        "0",
    ]))?)?;
    assert_eq!(result["rows"], json!([]));
    assert_eq!(result["schema"]["fields"].as_array().unwrap().len(), 1);
    Ok(())
}

#[test]
fn oversized_values_and_wide_schemas_need_deliberate_output_choices() -> TestResult {
    let huge = "中文\n'\"".repeat(2000);
    let sql = format!(
        "SELECT '{}' AS value FROM t LIMIT 1",
        huge.replace('\'', "''")
    );
    let columns = (0..1000)
        .map(|index| format!("1 AS column_{index}"))
        .collect::<Vec<_>>()
        .join(",");
    for sql in [sql, format!("SELECT {columns} FROM t WHERE false")] {
        let fixture = SqlCommandFixture::new(fixture_path("partitioned").join("table"), sql)?;
        let output = run_with_timeout(fixture.command().args(["--format", "json"]))?;
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(output.stdout.is_empty());
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["reason"],
            "json_output_limit_exceeded"
        );
        let result =
            assert_query_success_and_decode_json(&run_with_timeout(fixture.command().args([
                "--format",
                "json",
                "--max-output-bytes",
                "131072",
            ]))?)?;
        if result["rows"].as_array().unwrap().is_empty() {
            assert_eq!(result["schema"]["fields"].as_array().unwrap().len(), 1000);
        } else {
            assert_eq!(result["rows"], json!([[huge]]));
        }
    }
    Ok(())
}

#[test]
fn unsupported_json_types_offer_arrow_recovery_even_for_empty_results() -> TestResult {
    for suffix in ["LIMIT 1", "WHERE false"] {
        let fixture = SqlCommandFixture::new(
            fixture_path("partitioned").join("table"),
            format!("SELECT INTERVAL '1 month' AS value FROM t {suffix}"),
        )?;
        let output = run_with_timeout(fixture.command().args(["--format", "json"]))?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let diagnostic = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(diagnostic["code"], "unsupported_json_type");
        assert_eq!(diagnostic["recovery"], "change_request");
        let hint = diagnostic["hint"].as_str().unwrap();
        assert!(hint.contains("--format arrow"));
        assert!(hint.contains("redirected stdout and an explicit byte budget"));
        let overflow = run_with_timeout(fixture.command().args([
            "--format",
            "arrow",
            "--max-output-bytes",
            "1",
        ]))?;
        assert_eq!(overflow.status.code(), Some(1));
        assert_eq!(
            assert_single_terminal_record(&overflow.stderr, "diagnostic")["reason"],
            "output_byte_limit_exceeded"
        );
        assert_query_success_and_decode_arrow(&run_with_timeout(fixture.command().args([
            "--format",
            "arrow",
            "--max-output-bytes",
            "8192",
        ]))?)?;
    }
    Ok(())
}
