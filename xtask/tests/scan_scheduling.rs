//! Scan scheduling CLI checks for fixture reuse, reader failures, and summaries.

#![cfg(unix)]

use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::Value;

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

fn root() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
}

fn binary(directory: &Path, label: &str, status: &str) -> Result<PathBuf> {
    let path = directory.join(label);
    fs::write(
        &path,
        format!(
            r#"#!/bin/sh
set -eu
case "$1" in
prepare)
    test "{label}" != old || exit 17
    mkdir "$2"
    printf '{{"shape":"%s","writer":"new"}}\n' "$3" > "$2/fixture.json"
    cat "$2/fixture.json"
    ;;
run)
    test -f "$2/fixture.json"
    printf '{{"status":"{status}","writer":"new","elapsed_us":100,"first_batch_us":10,"rows_per_second":1000,"process_peak_rss_bytes":8192}}\n'
    ;;
esac
"#
        ),
    )?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
    Ok(path)
}

fn run(directory: &Path, name: &str, labels: &[&str], extra: &[&str]) -> Result<(Output, PathBuf)> {
    let output = directory.join(name);
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command
        .current_dir(root())
        .arg("scan-scheduling")
        .arg("--output-dir")
        .arg(&output);
    for (flag, value) in [
        ("--repetitions", "1"),
        ("--probe-repetitions", "1"),
        ("--suite", "performance"),
        ("--case-filter", r"^(tiny|unequal-tiny)-local-.*-direct$"),
    ] {
        if !extra.contains(&flag) {
            command.args([flag, value]);
        }
    }
    for label in labels {
        command
            .arg("--binary")
            .arg(format!("{label}={}", directory.join(label).display()));
    }
    Ok((command.args(extra).output()?, output))
}

fn runs(output: &Path) -> Result<Vec<Value>> {
    fs::read_to_string(output.join("runs.jsonl"))?
        .lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

#[test]
fn fixture_selection_is_independent_of_comparison_order() -> Result {
    let temporary = tempfile::tempdir()?;
    binary(temporary.path(), "old", "ok")?;
    binary(temporary.path(), "new", "ok")?;
    let mut identities = Vec::new();
    for (index, labels) in [["old", "new"].as_slice(), &["new", "old"], &["new"]]
        .into_iter()
        .enumerate()
    {
        let extra = if labels.len() == 1 {
            vec![]
        } else {
            vec!["--fixture-binary", "new"]
        };
        let (result, output) = run(
            temporary.path(),
            &format!("results-{index}"),
            labels,
            &extra,
        )?;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let metadata: Value = serde_json::from_slice(&fs::read(output.join("metadata.json"))?)?;
        assert_eq!(metadata["schema_version"], 2);
        assert_eq!(metadata["fixture_binary"], "new");
        assert_eq!(metadata["ordering_algorithm"], "sha256-seed-case-v1");
        assert!(metadata["finished_at"].is_string());
        assert!(metadata["cpu_affinity"].is_array() || metadata["cpu_affinity"].is_null());
        for key in [
            "runner_sha256",
            "runner_executable_sha256",
            "production_source_sha256",
        ] {
            assert!(metadata.get(key).is_none());
        }
        identities.push(metadata["fixtures"].clone());
        let samples = runs(&output)?;
        assert_eq!(samples.len(), 4 * labels.len());
        assert!(
            samples
                .iter()
                .all(|sample| sample["status"] == "ok" && sample["writer"] == "new")
        );
        assert!(
            samples
                .iter()
                .all(|sample| sample["cpu_seconds"].as_f64().unwrap() >= 0.0)
        );
        for label in labels {
            assert_eq!(
                fs::read(output.join("bin").join(label))?,
                fs::read(temporary.path().join(label))?
            );
        }
        assert!(output.join("source/xtask/src/scan_scheduling.rs").is_file());
        assert!(output.join("source/xtask/src/command.rs").is_file());
        let summaries: Vec<BTreeMap<String, String>> =
            csv::Reader::from_path(output.join("summary.csv"))?
                .deserialize()
                .collect::<std::result::Result<_, _>>()?;
        assert_eq!(summaries.len(), 2 * labels.len());
        assert!(
            summaries
                .iter()
                .all(|row| row["runs"] == "1" && row["median_elapsed_us"] == "100")
        );
    }
    assert!(identities.windows(2).all(|pair| pair[0] == pair[1]));
    Ok(())
}

#[test]
fn invalid_inputs_are_rejected_before_creating_output() -> Result {
    let temporary = tempfile::tempdir()?;
    binary(temporary.path(), "old", "ok")?;
    binary(temporary.path(), "new", "ok")?;
    for (index, (labels, extra)) in [
        (vec!["old", "new"], vec![]),
        (vec!["old", "new"], vec!["--fixture-binary", "missing"]),
        (vec!["new"], vec!["--fixture-binary", "missing"]),
        (vec!["new", "new"], vec![]),
        (vec!["new"], vec!["--timeout-seconds", "0"]),
        (vec!["new"], vec!["--probe-timeout-seconds", "121"]),
        (vec!["new"], vec!["--repetitions", "129"]),
        (vec!["new"], vec!["--case-filter", "["]),
        (vec!["new"], vec!["--case-filter", "^missing$"]),
        (vec!["../new"], vec![]),
    ]
    .into_iter()
    .enumerate()
    {
        let (result, output) = run(
            temporary.path(),
            &format!("invalid-{index}"),
            &labels,
            &extra,
        )?;
        assert_eq!(
            result.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(!output.exists());
    }
    let output = temporary.path().join("existing");
    fs::create_dir(&output)?;
    fs::write(output.join("keep"), "untouched")?;
    let (result, _) = run(temporary.path(), "existing", &["new"], &[])?;
    assert!(!result.status.success());
    assert_eq!(fs::read_to_string(output.join("keep"))?, "untouched");
    Ok(())
}

#[test]
fn progress_timeouts_are_recorded_without_warmups() -> Result {
    let temporary = tempfile::tempdir()?;
    binary(temporary.path(), "new", "timeout")?;
    let output = temporary.path().join("progress");
    let result = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .current_dir(root())
        .args([
            "scan-scheduling",
            "--suite",
            "progress",
            "--probe-repetitions",
            "2",
            "--case-filter",
            "^small-local-p16-c1-pf2-d0-lnone-direct$",
        ])
        .arg("--binary")
        .arg(format!("new={}", temporary.path().join("new").display()))
        .arg("--output-dir")
        .arg(&output)
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let samples = runs(&output)?;
    assert_eq!(samples.len(), 2);
    assert!(
        samples
            .iter()
            .all(|sample| sample["phase"] == "measured" && sample["status"] == "timeout")
    );
    assert_eq!(samples[0]["repetition"], 1);
    assert_eq!(samples[1]["repetition"], 2);
    let rows: Vec<BTreeMap<String, String>> = csv::Reader::from_path(output.join("summary.csv"))?
        .deserialize()
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(rows[0]["timeouts"], "2");
    assert_eq!(rows[0]["median_elapsed_us"], "");
    Ok(())
}

#[test]
fn failed_performance_control_stops_and_keeps_evidence() -> Result {
    let temporary = tempfile::tempdir()?;
    binary(temporary.path(), "new", "timeout")?;
    let (result, output) = run(temporary.path(), "failure", &["new"], &[])?;
    assert!(!result.status.success());
    let samples = runs(&output)?;
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0]["phase"], "warmup");
    assert_eq!(samples[0]["status"], "timeout");
    let metadata: Value = serde_json::from_slice(&fs::read(output.join("metadata.json"))?)?;
    assert!(metadata.get("finished_at").is_none());
    Ok(())
}
