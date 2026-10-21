use super::*;
use std::io::Write;

#[test]
fn native_metrics_remain_exact_and_trace_counts_must_reconcile() -> Result<()> {
    for (raw, expected) in [
        ("0", 0),
        ("-0.0e-100", 0),
        ("1.23e+5", 123000),
        ("+0042.000", 42),
        ("9007199254740991", 9007199254740991),
    ] {
        assert_eq!(counter(raw)?, expected);
    }
    for invalid in [
        "9007199254740992",
        "9007199254740990.1",
        "1.5",
        "-1",
        "NaN",
        "Inf",
        "1e99",
        "1e-100",
        "1.2.3",
        ".",
    ] {
        assert!(counter(invalid).is_err(), "{invalid}");
    }
    let before = parse_metrics(
        "# comment\nminio_api_requests_total{name=\"GetObject\"} 1\nminio_api_requests_traffic_sent_bytes{type=\"s3\"} 10\n",
    )?;
    let after = parse_metrics(
        "minio_api_requests_total{name=\"GetObject\"} 3\nminio_api_requests_traffic_sent_bytes{type=\"s3\"} 2e1\nminio_api_requests_canceled_total{type=\"s3\"} 1\n",
    )?;
    let counts = BTreeMap::from([("GetObject".into(), 2), ("HeadObject".into(), 0)]);
    assert_eq!(
        reconcile(&before, &after, &counts, 10)?,
        json!({"requests":{"GetObject":2},"response_bytes":10,"rejected":0,"canceled":1})
    );
    for field in ["active", "rejected", "response_bytes"] {
        let mut changed = after.clone();
        changed[field] = json!(1);
        assert!(reconcile(&before, &changed, &counts, 10).is_err());
    }
    assert!(reconcile(&after, &before, &counts, 10).is_err());
    assert!(reconcile(&before, &after, &BTreeMap::new(), 10).is_err());
    assert!(parse_metrics("minio_api_requests_total{name=\"GetObject\"} 1").is_err());
    Ok(())
}

fn raw() -> Value {
    json!({"funcname":"s3.GetObject", "http": {
        "request":{"path":"/selective-read/table/a%20b.parquet", "method":"GET", "time":"1970-01-01T00:00:01.000000001Z",
            "headers":{"Authorization":["private-signature"], "Range":["bytes=0-3"], "amz-sdk-request":["attempt=2; max=3"], "amz-sdk-invocation-id":["retry"]}},
        "response":{"time":"1970-01-01T00:00:02Z", "statuscode":206,
            "headers":{"Content-Length":["4"], "Content-Range":["bytes 0-3/8"], "x-amz-request-id":["request"]}},
        "stats":{"outputbytes":3}}})
}

#[test]
fn redaction_scope_and_nanosecond_boundaries_are_preserved() -> Result<()> {
    assert_eq!(timestamp("1970-01-01T00:00:01.000000001Z")?, 1_000_000_001);
    for invalid in [
        "2023-02-29T00:00:00Z",
        "2024-01-01T24:00:00Z",
        "2024-01-01T00:00:60Z",
        "2024-01-01T00:00:00.1234567890Z",
        "2024-01-01T00:00:00+00:00",
    ] {
        assert!(timestamp(invalid).is_err());
    }
    assert_eq!(unquote("a+%2Fb%25%xx%FF"), "a+/b%%xx\u{fffd}");
    let mut input = raw();
    let record = sanitize(&input, "table/", "run", "case")?;
    assert_eq!(record["object"], "table/a b.parquet");
    assert_eq!(record["sdk_attempt"], 2);
    assert_eq!(record["range"], "bytes=0-3");
    assert_eq!(record["incomplete_body"], true);
    assert_eq!(record["started_ns"], 1_000_000_001u64);
    assert!(!serde_json::to_string(&record)?.contains("private-signature"));
    assert!(sanitize(&input, "another/", "run", "case").is_err());
    for (key, category) in [
        ("table/_delta_log/checkpoint.parquet", "delta_log"),
        ("table/deletion_vector_a.bin", "deletion_vector"),
        ("table/file.bin", "other"),
    ] {
        input["http"]["request"]["path"] = json!(format!("/selective-read/{key}"));
        assert_eq!(
            sanitize(&input, "table/", "run", "case")?["object_class"],
            category
        );
    }
    input["funcname"] = json!("s3.ListObjectsV2");
    input["http"]["request"]["path"] = json!("/selective-read");
    assert!(sanitize(&input, "table/", "run", "case").is_err());
    input["http"]["request"]["rawquery"] = json!("list-type=2&prefix=table%2F");
    assert_eq!(
        sanitize(&input, "table/", "run", "case")?["object_class"],
        "listing"
    );
    input["http"]["request"]["rawquery"] = json!("prefix=another%2F");
    assert!(sanitize(&input, "table/", "run", "case").is_err());
    Ok(())
}

#[test]
fn diagnostics_count_every_attempt_and_do_not_invent_post_stream_bytes() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut file = File::create(root.path().join("requests.jsonl"))?;
    let first = sanitize(&raw(), "table/", "run", "case")?;
    let mut head = first.clone();
    head["method"] = json!("HEAD");
    head["response_bytes"] = json!(0);
    head["incomplete_body"] = json!(false);
    head["sdk_attempt"] = Value::Null;
    let mut missing = first.clone();
    missing["status"] = json!(404);
    missing["object"] = json!("table/missing.parquet");
    for row in [&first, &head, &missing] {
        writeln!(file, "{}", serde_json::to_string(row)?)?;
    }
    let result = summarize(
        root.path(),
        &json!({"diagnostic_events":[{"event":"stream_complete","time_ns":1000000001}]}),
    )?;
    assert_eq!(result["requests"], 3);
    assert_eq!(result["response_bytes"], 6);
    assert_eq!(result["by_api_status"]["GetObject/404"], 1);
    assert_eq!(result["explicit_retry_attempts"], 2);
    assert_eq!(result["incomplete_responses"], 2);
    assert_eq!(result["started_after_final_stream"], 3);
    assert_eq!(result["ended_after_final_stream"], 3);
    assert_eq!(result["response_bytes_after_stream"], Value::Null);
    assert_eq!(
        result["touched_parquet_objects"],
        json!(["table/a b.parquet", "table/missing.parquet"])
    );
    assert_eq!(result["parquet_head_objects"], json!(["table/a b.parquet"]));
    assert_eq!(
        summarize(root.path(), &json!({}))?["last_stream_complete_ns"],
        Value::Null
    );
    Ok(())
}
