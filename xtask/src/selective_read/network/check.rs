use std::{
    io::Read,
    thread,
    time::{Duration, Instant},
};

use super::*;
use crate::{artifacts::reader_sha256, selective_read::observe};

fn drain(directory: &Path) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while control(directory, false, false, 10.0)?["active"] != 0 {
        require(Instant::now() < deadline, "proxy failed to drain")?;
        thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

pub(in crate::selective_read) fn calibrate(directory: &Path, output: &Path) -> Result<()> {
    fs::create_dir(output)?;
    let _lock = storage::exclusive(directory)?;
    storage::verify_server(directory)?;
    let config = config(directory)?.ok_or("a network profile is required")?;
    let profile = &config["profile"];
    require(
        *profile == json!({"latency_ms":200,"jitter_ms":20,"mbps":150,"seed":profile["seed"]}),
        "calibration requires 200 +/- 20 ms and 150 Mbit/s",
    )?;
    let body: Vec<_> = (0..8 * 1024 * 1024).map(|n| n as u8).collect();
    let source = output.join("payload.bin");
    crate::artifacts::write_new_file(&source, &body)?;
    let key = format!(
        "_checks/network-{}-{}/payload.parquet",
        std::process::id(),
        timestamp_ns()?
    );
    storage::put_verified(directory, &key, &source, &digest(&source)?, false)?;
    let upstream = url::Url::parse(storage::endpoint(&storage::state(directory)?)?)?;
    let proxy = url::Url::parse(storage::endpoint(&config)?)?;
    let connection = format!(
        "127.0.0.1:{}:127.0.0.1:{}",
        upstream.port().ok_or("missing storage port")?,
        proxy.port().ok_or("missing proxy port")?
    );
    let fetch = |key: &str, options: &[&str]| -> Result<std::process::Child> {
        let mut params = vec![
            "--fail-with-body",
            "--max-time",
            "15",
            "--no-buffer",
            "--connect-to",
            &connection,
        ];
        params.extend_from_slice(options);
        storage::curl(directory, &format!("/{}/{key}", storage::BUCKET), &params)
    };
    control(directory, true, true, 10.0)?;
    let started = Instant::now();
    let mut process = fetch(&key, &[])?;
    let mut first = vec![0; 32768];
    let received = process
        .stdout
        .as_mut()
        .ok_or("missing curl stdout")?
        .read_exact(&mut first);
    if let Err(error) = received {
        let _ = process.kill();
        let _ = process.wait();
        return Err(error.into());
    }
    let first_seconds = started.elapsed().as_secs_f64();
    let response = process.wait_with_output()?;
    let seconds = started.elapsed().as_secs_f64();
    require(response.status.success(), "serial network request failed")?;
    first.extend(response.stdout);
    require(
        first == body && (0.17..0.5).contains(&first_seconds) && seconds - first_seconds > 0.35,
        "serial bytes, first-payload latency or bandwidth differs",
    )?;
    for (range, expected) in [("11-23", &body[11..24]), ("-8", &body[body.len() - 8..])] {
        let response = fetch(&key, &["--range", range])?.wait_with_output()?;
        require(
            response.status.success() && response.stdout == expected,
            "range bytes differ",
        )?;
    }
    let response = fetch(&key, &["--head"])?.wait_with_output()?;
    require(
        response.status.success()
            && String::from_utf8(response.stdout)?
                .to_ascii_lowercase()
                .contains(&format!("content-length: {}", body.len())),
        "HEAD length differs",
    )?;
    drain(directory)?;
    let serial = finish(output, control(directory, false, false, 10.0)?)?;
    require(
        serial["requests"] == 4 && serial["response_bytes"] == body.len() + 21,
        "serial proxy totals differ",
    )?;
    for line in fs::read_to_string(output.join("network-requests.jsonl"))?.lines() {
        let record: Value = serde_json::from_str(line)?;
        require(
            !text(&record["request_id"])?.is_empty()
                && record["delay_us"]
                    .as_u64()
                    .is_some_and(|delay| (180000..=220000).contains(&delay))
                && record["incomplete_body"] == false,
            "invalid jitter or incomplete response",
        )?;
    }
    control(directory, true, false, 10.0)?;
    let started = Instant::now();
    thread::scope(|scope| -> Result<()> {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| -> std::result::Result<(), String> {
                    (|| -> Result<()> {
                        let response =
                            fetch(&key, &["--range", "0-4194303"])?.wait_with_output()?;
                        require(
                            response.status.success() && response.stdout == body[..4194304],
                            "parallel range bytes differ",
                        )
                    })()
                    .map_err(|e| e.to_string())
                })
            })
            .collect();
        for worker in workers {
            worker.join().map_err(|_| "parallel client panicked")??;
        }
        Ok(())
    })?;
    let parallel_seconds = started.elapsed().as_secs_f64();
    drain(directory)?;
    let parallel = control(directory, false, false, 10.0)?;
    require(
        parallel["requests"] == 4
            && parallel["response_bytes"] == 16 * 1024 * 1024
            && parallel["records"].as_array().is_none_or(Vec::is_empty)
            && (1.03..1.6).contains(&parallel_seconds),
        "parallel requests do not share the declared bandwidth",
    )?;
    control(directory, true, true, 10.0)?;
    for attempt in [1, 2] {
        let response = fetch(
            &format!("{key}.absent"),
            &[
                "--header",
                "amz-sdk-invocation-id: network-check",
                "--header",
                &format!("amz-sdk-request: attempt={attempt}"),
            ],
        )?
        .wait_with_output()?;
        require(
            response.status.code() == Some(22)
                && String::from_utf8_lossy(&response.stdout).contains("NoSuchKey"),
            "missing object did not return its S3 error",
        )?;
    }
    drain(directory)?;
    let retries = control(directory, false, false, 10.0)?;
    require(
        retries["requests"] == 2
            && array(&retries["records"])?
                .iter()
                .all(|r| r["status"] == 404),
        "retry attempts were not retained",
    )?;
    save(&output.join("retry-check.json"), &retries)?;
    control(directory, true, true, 10.0)?;
    let before = observe::metrics(directory, 10.0)?;
    let cancelled = fetch(&key, &["--max-time", "0.3"])?.wait_with_output()?;
    require(
        cancelled.status.code() == Some(28)
            && !cancelled.stdout.is_empty()
            && cancelled.stdout.len() < body.len(),
        "client cancellation did not interrupt a body",
    )?;
    drain(directory)?;
    let proxy = control(directory, false, false, 10.0)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let after = loop {
        let after = observe::metrics(directory, 10.0)?;
        if after["active"] == 0 {
            break after;
        }
        require(Instant::now() < deadline, "upstream failed to drain")?;
        thread::sleep(Duration::from_millis(10));
    };
    let upstream_bytes = after["response_bytes"]
        .as_u64()
        .ok_or("missing server byte counter")?
        .checked_sub(
            before["response_bytes"]
                .as_u64()
                .ok_or("missing server byte counter")?,
        )
        .ok_or("server counters reset")?;
    let proxy_bytes = proxy["response_bytes"]
        .as_u64()
        .ok_or("missing proxy byte counter")?;
    require(
        proxy["requests"] == 1
            && proxy["records"][0]["incomplete_body"] == true
            && cancelled.stdout.len() as u64 <= proxy_bytes
            && proxy_bytes < body.len() as u64
            && upstream_bytes >= proxy_bytes,
        "cancellation byte boundaries differ",
    )?;
    save(
        &output.join("cancellation-check.json"),
        &json!({"proxy":proxy,"upstream_bytes":upstream_bytes,"consumed_bytes":cancelled.stdout.len()}),
    )?;
    save(
        &output.join("calibration.json"),
        &json!({"status":"passed","profile":profile,"first_payload_seconds":first_seconds,
        "serial_seconds":seconds,"parallel_seconds":parallel_seconds,"serial_bytes":body.len(),"parallel_bytes":16*1024*1024,
        "sha256":reader_sha256(body.as_slice())?,
        "boundary":BOUNDARY,"explicit_retry_attempts":2,"cancelled_transfer":"passed"}),
    )?;
    control(directory, true, false, 10.0)?;
    Ok(())
}
