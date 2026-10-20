use std::{
    fs::{self, File},
    path::Path,
};

use super::*;
use crate::{selective_read::supervise::monotonic_ns, selective_read_build::digest};
use session::Observer;

pub(super) fn known_requests(directory: &Path, output: &Path) -> Result<Value> {
    fs::create_dir(output)?;
    let scope = format!(
        "_checks/{}-{}/",
        std::process::id(),
        storage::timestamp_ns()?
    );
    let body: Vec<_> = (0..65536).map(|n| n as u8).collect();
    let small = output.join("small.parquet");
    crate::artifacts::write_new_file(&small, &body)?;
    let large = output.join("large.bin");
    File::create_new(&large)?.set_len(64 * 1024 * 1024)?;
    let _lock = storage::exclusive(directory)?;
    storage::verify_server(directory)?;
    for (name, source) in [
        ("small.parquet", &small),
        ("large.bin", &large),
        ("deletion_vector_known.bin", &small),
        ("_delta_log/checkpoint.parquet", &small),
    ] {
        storage::put_verified(
            directory,
            &format!("{scope}{name}"),
            source,
            &digest(source)?,
            false,
        )?;
    }
    let hash = digest(&small)?;
    storage::put_verified(
        directory,
        &format!("{scope}small.parquet"),
        &small,
        &hash,
        false,
    )?;
    require(
        storage::put_verified(
            directory,
            &format!("{scope}small.parquet"),
            &small,
            &"00".repeat(32),
            false,
        )
        .is_err(),
        "changed immutable object was accepted",
    )?;
    for _ in 0..2 {
        require(
            storage::put_verified(
                directory,
                &format!("{scope}checksum.parquet"),
                &small,
                &hash,
                true,
            )? == "server-sha256",
            "checksum upload used the wrong verification",
        )?;
    }
    for name in ["bad-checksum.parquet", "checksum.parquet"] {
        require(
            storage::put_verified(
                directory,
                &format!("{scope}{name}"),
                &small,
                &"00".repeat(32),
                true,
            )
            .is_err(),
            "bad upload checksum was accepted",
        )?;
    }
    let payload = json!({"table_uri":format!("s3://{}/{scope}",storage::BUCKET),"run_id":"known","case_id":"known-requests"});
    storage::save(&output.join("request.json"), &payload)?;
    let mut trace = Observer::start(directory, output)?;
    let mut expected = Vec::new();
    for (name, method, range, attempt, bytes) in [
        ("small.parquet", "GET", None, None, Some(body.as_slice())),
        (
            "small.parquet",
            "GET",
            Some("bytes=11-23"),
            None,
            Some(&body[11..24]),
        ),
        (
            "small.parquet",
            "GET",
            Some("bytes=-8"),
            None,
            Some(&body[body.len() - 8..]),
        ),
        ("small.parquet", "HEAD", None, None, None),
        (
            "deletion_vector_known.bin",
            "GET",
            None,
            None,
            Some(body.as_slice()),
        ),
        (
            "_delta_log/checkpoint.parquet",
            "GET",
            None,
            None,
            Some(body.as_slice()),
        ),
        ("absent.bin", "GET", None, Some(1), None),
        ("absent.bin", "GET", None, Some(2), None),
    ] {
        let key = format!("{scope}{name}");
        let mut options = vec!["--max-time".to_owned(), "10".to_owned()];
        if method == "HEAD" {
            options.push("--head".into());
        }
        if let Some(range) = range {
            options.extend(["--header".into(), format!("Range: {range}")]);
        }
        if let Some(attempt) = attempt {
            options.extend([
                "--header".into(),
                "amz-sdk-invocation-id: known-retry".into(),
                "--header".into(),
                format!("amz-sdk-request: attempt={attempt}"),
            ]);
        }
        let value = storage::response(storage::curl(
            directory,
            &format!("/{}/{key}", storage::BUCKET),
            &options.iter().map(String::as_str).collect::<Vec<_>>(),
        )?)?;
        if let Some(bytes) = bytes {
            require(value == bytes, "response body differs")?;
        }
        if attempt.is_some() {
            require(
                String::from_utf8_lossy(&value).contains("NoSuchKey"),
                "missing S3 error body",
            )?;
        }
        expected.push(json!({"method":method,"object":key,"range":range,"sdk_attempt":attempt,"response_bytes":if method=="HEAD" {0} else {value.len()}}));
    }
    let listing = storage::request(
        directory,
        &format!("/{}?list-type=2&prefix={scope}", storage::BUCKET),
        &[],
    )?;
    // End a synthetic consumer stream while an independent response is still active.
    let stream_complete = storage::timestamp_ns()?;
    let partial = storage::curl(
        directory,
        &format!("/{}/{scope}large.bin", storage::BUCKET),
        &["--limit-rate", "16k", "--max-time", "0.3"],
    )?
    .wait_with_output()?;
    let client_exited = storage::timestamp_ns()?;
    require(
        partial.status.code() == Some(28)
            && !partial.stdout.is_empty()
            && partial.stdout.len() < 64 * 1024 * 1024,
        "large response was not interrupted",
    )?;
    trace.finish(monotonic_ns()? + 60_000_000_000)?;
    drop(trace);
    let proof = storage::read_json(&output.join("capture.json"))?;
    let records: Vec<Value> = fs::read_to_string(output.join("requests.jsonl"))?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()?;
    require(
        records.len() == expected.len() + 2,
        "trace omitted or added requests",
    )?;
    for expected in expected {
        let matching: Vec<_> = records
            .iter()
            .filter(|r| {
                ["method", "object", "range", "sdk_attempt"]
                    .iter()
                    .all(|key| r[key] == expected[key])
            })
            .collect();
        require(
            matching.len() == 1,
            "expected exactly one matching trace record",
        )?;
        let record = matching[0];
        let status = if !expected["sdk_attempt"].is_null() {
            404
        } else if !expected["range"].is_null() {
            206
        } else {
            200
        };
        require(
            record["response_bytes"] == expected["response_bytes"]
                && record["incomplete_body"] == false
                && record["status"] == status,
            "trace response bytes or status differ",
        )?;
        if expected["method"] == "HEAD" {
            require(
                record["advertised_content_length"] == body.len(),
                "HEAD length differs",
            )?;
        }
        if !expected["sdk_attempt"].is_null() {
            require(
                record["sdk_invocation_id"] == "known-retry",
                "retry identity changed",
            )?;
        }
    }
    let listed: Vec<_> = records
        .iter()
        .filter(|r| r["api"] == "ListObjectsV2")
        .collect();
    require(
        listed.len() == 1 && listed[0]["response_bytes"] == listing.len(),
        "listing byte count differs",
    )?;
    let interrupted: Vec<_> = records
        .iter()
        .filter(|r| {
            r["object"]
                .as_str()
                .is_some_and(|key| key.ends_with("large.bin"))
        })
        .collect();
    require(interrupted.len() == 1, "missing interrupted response")?;
    let interrupted = interrupted[0];
    let accepted = integer(&interrupted["response_bytes"])?;
    require(
        interrupted["incomplete_body"] == true
            && partial.stdout.len() as u64 <= accepted
            && accepted < 64 * 1024 * 1024
            && integer(&proof["drained_ns"])? > client_exited,
        "interrupted response or drain boundary differs",
    )?;
    let io = summarize(
        output,
        &json!({"diagnostic_events":[{"event":"stream_complete","time_ns":stream_complete}]}),
    )?;
    require(
        io["requests"] == 10
            && io["explicit_retry_attempts"] == 1
            && io["incomplete_responses"] == 1
            && io["started_after_final_stream"] == 1
            && integer(&io["ended_after_final_stream"])? >= 1
            && io["touched_parquet_objects"] == json!([format!("{scope}small.parquet")])
            && io["by_class"]["deletion_vector"]["requests"] == 1
            && io["by_class"]["delta_log"]["requests"] == 1,
        "diagnostic summary differs",
    )?;
    let mut counts = BTreeMap::new();
    let mut size = 0;
    for record in &records {
        *counts.entry(text(&record["api"])?.to_owned()).or_default() += 1;
        size += integer(&record["response_bytes"])?;
    }
    *counts.entry("HeadObject".into()).or_default() += 1; // Native deltas include the end marker.
    reconcile(&proof["before"], &proof["after"], &counts, size)?;
    for difference in [-1, 1] {
        let mut altered = counts.clone();
        *altered.get_mut("GetObject").ok_or("missing GET counts")? =
            (counts["GetObject"] as i64 + difference) as u64;
        require(
            reconcile(&proof["before"], &proof["after"], &altered, size).is_err(),
            "changed request count was accepted",
        )?;
    }
    require(
        reconcile(&proof["before"], &proof["after"], &counts, size + 1).is_err(),
        "changed byte count was accepted",
    )?;
    for key in ["active", "rejected"] {
        let mut altered = proof["after"].clone();
        altered[key] = json!(integer(&altered[key])? + 1);
        require(
            reconcile(&proof["before"], &altered, &counts, size).is_err(),
            "active or rejected request was accepted",
        )?;
    }
    let unrelated = output.join("unrelated");
    fs::create_dir(&unrelated)?;
    storage::save(
        &unrelated.join("request.json"),
        &json!({"table_uri":format!("s3://{}/{scope}different/",storage::BUCKET),"run_id":"unrelated","case_id":"rejected"}),
    )?;
    let mut trace = Observer::start(directory, &unrelated)?;
    storage::request(
        directory,
        &format!("/{}/{scope}small.parquet", storage::BUCKET),
        &[],
    )?;
    require(
        trace.finish(monotonic_ns()? + 60_000_000_000).is_err(),
        "unrelated traffic was accepted",
    )?;
    drop(trace);
    require(
        storage::read_json(&unrelated.join("capture.json"))?["failure_reason"]
            == "unrelated object request"
            && fs::read_to_string(unrelated.join("requests.jsonl"))?.is_empty(),
        "unrelated trace was not redacted",
    )?;
    let result = json!({"status":"passed","capture":proof,"io":io,"client_received_partial_bytes":partial.stdout.len(),
        "server_accepted_partial_bytes":accepted,"advertised_partial_bytes":64*1024*1024,"controller_sha256":digest(&std::env::current_exe()?)?});
    storage::save(&output.join("checks.json"), &result)?;
    Ok(result)
}
