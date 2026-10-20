use super::*;
use crate::selective_read::{field, invoke};

pub(in crate::selective_read) mod batch;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::selective_read) struct Request {
    directory: PathBuf,
    output: PathBuf,
    binary: PathBuf,
    baseline_binary: Option<PathBuf>,
    reference: PathBuf,
    protocols: PathBuf,
    payload: Value,
    mode: String,
    remote: bool,
    network_warmup: bool,
    no_warmup: bool,
}

fn configure<'a>(input: &'a Request, env: &mut BTreeMap<String, String>) -> Result<&'a Path> {
    env.remove("RUST_LOG");
    let binary = if let Some(baseline) = &input.baseline_binary {
        require(
            matches!(input.mode.as_str(), "baseline" | "candidate")
                && input.payload["execution_mode"] == "reuse"
                && input.payload["concurrent_queries"].is_null()
                && !input.network_warmup
                && !input.no_warmup,
            "two-build comparison requires sequential reuse with default warmup",
        )?;
        env.insert("DAR_INTRA_PAGE_READS".into(), "default".into());
        env.insert("DAR_NETWORK_WARMUP".into(), "default".into());
        if input.mode == "baseline" {
            baseline
        } else {
            &input.binary
        }
    } else {
        require(
            matches!(input.mode.as_str(), "off" | "auto")
                && !(input.network_warmup && input.no_warmup),
            "invalid partial-page or warmup mode",
        )?;
        env.insert("DAR_INTRA_PAGE_READS".into(), input.mode.clone());
        env.insert(
            "DAR_NETWORK_WARMUP".into(),
            if input.no_warmup {
                "none"
            } else if input.network_warmup && input.mode == "auto" {
                "on"
            } else {
                "off"
            }
            .into(),
        );
        &input.binary
    };
    if input.payload["purpose"] == "validation" {
        env.insert(
            "RUST_LOG".into(),
            concat!(
                "delta_arrow_reader::diagnostics::intra_page=debug,",
                "delta_arrow_reader::diagnostics::parquet_range_planning=debug,",
                "delta_arrow_reader::diagnostics::network_warmup=debug"
            )
            .into(),
        );
    }
    Ok(binary)
}

fn metrics(capture: Value, path: &Path) -> Result<Value> {
    if capture.is_null() {
        return Ok(capture);
    }
    let mut events = Vec::new();
    let mut identical = BTreeMap::new();
    let mut statuses = BTreeMap::new();
    let mut incomplete = 0;
    for line in fs::read_to_string(path)?.lines() {
        let row: Value = serde_json::from_str(line)?;
        for (field, change) in [("started_ns", 1), ("ended_ns", -1)] {
            events.push((
                row[field].as_u64().ok_or("invalid request timestamp")?,
                change,
            ));
        }
        let range: Option<String> = serde_json::from_value(field(&row, "range")?.clone())?;
        *identical
            .entry((
                text(&row["method"])?.to_owned(),
                text(&row["object"])?.to_owned(),
                range,
            ))
            .or_insert(0) += 1;
        *statuses
            .entry(row["status"].as_u64().ok_or("invalid HTTP status")?)
            .or_insert(0) += 1;
        incomplete += usize::from(
            row["incomplete_body"]
                .as_bool()
                .ok_or("missing body completion flag")?,
        );
    }
    events.sort_unstable();
    let (mut active, mut peak) = (0, 0);
    for (_, change) in events {
        active += change;
        peak = peak.max(active);
    }
    require(active == 0, "unbalanced request intervals")?;
    Ok(
        json!({"response_bytes":capture["response_bytes"],"requests":capture["requests"],
        "peak_active_requests":peak,"by_class":capture["by_class"],"incomplete_bodies":incomplete,
        "http_statuses":statuses,"repeated_identical_requests":identical.values().map(|n| n-1).sum::<usize>(),
        "retries":null,"retry_note":"Repeated ranges may be separate reads; the proxy cannot identify SDK retries."}),
    )
}

pub(in crate::selective_read) fn run(input: Request) -> Result<Value> {
    let mut environment = serde_json::from_value(storage::reader_environment(&input.directory)?)?;
    let binary = configure(&input, &mut environment)?;
    if input.remote {
        network::control(
            &input.directory,
            true,
            input.payload["purpose"] != "timing",
            10.0,
        )?;
    }
    let prefix = serde_json::from_value(storage::reader_prefix(&input.directory, None)?)?;
    let record = invoke::run(&invoke::Request {
        binary: binary.to_owned(),
        payload: input.payload.clone(),
        output: input.output.clone(),
        reference: Some(input.reference),
        env: Some(environment),
        command_prefix: prefix,
        supervised: true,
        protocols: input.protocols,
    })?;
    storage::save(&input.output.join("observation.json"), &record)?;
    let mut capture = if input.remote {
        network::capture(&input.directory, &input.output)?
    } else {
        Value::Null
    };
    if input.payload["purpose"] != "timing" {
        capture = metrics(capture, &input.output.join("network-requests.jsonl"))?;
    }
    Ok(json!({"record":record,"network":capture}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_builds_use_defaults_and_policies_keep_distinct_warmup_modes() -> Result<()> {
        let mut input: Request = serde_json::from_value(
            json!({"directory":"state","output":"output",
            "binary":"candidate","baseline_binary":"baseline","reference":"reference","protocols":"protocols",
            "payload":{"execution_mode":"reuse","purpose":"timing"},"mode":"baseline","remote":true,
            "network_warmup":false,"no_warmup":false}),
        )?;
        for mode in ["baseline", "candidate"] {
            input.mode = mode.into();
            let mut env = BTreeMap::from([("RUST_LOG".into(), "trace".into())]);
            assert_eq!(configure(&input, &mut env)?, Path::new(mode));
            assert_eq!(
                env,
                BTreeMap::from([
                    ("DAR_INTRA_PAGE_READS".into(), "default".into()),
                    ("DAR_NETWORK_WARMUP".into(), "default".into())
                ])
            );
        }
        input.network_warmup = true;
        assert!(configure(&input, &mut BTreeMap::new()).is_err());
        input.baseline_binary = None;
        for (mode, warmup, none, expected) in [
            ("off", false, false, "off"),
            ("auto", true, false, "on"),
            ("off", true, false, "off"),
            ("auto", false, true, "none"),
        ] {
            input.mode = mode.into();
            input.network_warmup = warmup;
            input.no_warmup = none;
            input.payload["purpose"] = json!("validation");
            let mut env = BTreeMap::new();
            assert_eq!(configure(&input, &mut env)?, Path::new("candidate"));
            assert_eq!(env["DAR_INTRA_PAGE_READS"], mode);
            assert_eq!(env["DAR_NETWORK_WARMUP"], expected);
            assert!(env["RUST_LOG"].contains("network_warmup=debug"));
        }
        input.network_warmup = true;
        assert!(configure(&input, &mut BTreeMap::new()).is_err());
        Ok(())
    }

    #[test]
    fn overlapping_repeated_ranges_are_not_reported_as_sdk_retries() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("requests.jsonl");
        let mut bytes = Vec::new();
        for (start, end) in [(1, 4), (2, 3), (4, 5)] {
            serde_json::to_writer(
                &mut bytes,
                &json!({"method":"GET","object":"part.parquet","range":"bytes=0-7",
                "incomplete_body":false,"status":206,"started_ns":start,"ended_ns":end}),
            )?;
            bytes.push(b'\n');
        }
        fs::write(&path, bytes)?;
        let value = metrics(
            json!({"requests":3,"response_bytes":24,"by_class":{}}),
            &path,
        )?;
        assert_eq!(value["peak_active_requests"], 2);
        assert_eq!(value["repeated_identical_requests"], 2);
        assert_eq!(value["http_statuses"], json!({"206":3}));
        assert!(value["retries"].is_null());
        assert!(metrics(Value::Null, Path::new("missing"))?.is_null());
        Ok(())
    }
}
