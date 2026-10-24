//! Validation record creation and immutable artifact handoff checks.

#![allow(clippy::unwrap_used)]
#![cfg(unix)]

use std::{error::Error, fs, process::Command};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

type TestResult = Result<(), Box<dyn Error>>;

fn ci() -> Value {
    serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml")).unwrap()
}

fn steps(job: &Value) -> &[Value] {
    job["steps"].as_array().unwrap()
}

#[test]
fn record_job_runs_after_approved_validation_without_building_or_downloading_artifacts() {
    let ci = ci();
    let record = &ci["jobs"]["release-cli-record"];
    assert_eq!(
        record["needs"],
        json!([
            "changes",
            "release-approval",
            "release-cli",
            "release-cli-images"
        ])
    );
    assert_eq!(record["permissions"], json!({"contents": "read"}));
    assert!(record["environment"].is_null());
    assert!(record["continue-on-error"].is_null());
    assert_eq!(
        record["if"]
            .as_str()
            .unwrap()
            .split_whitespace()
            .collect::<String>(),
        "needs.changes.outputs.release_pr=='true'&&needs.release-approval.result=='success'&&needs.release-cli.result=='success'&&needs.release-cli.outputs.validated=='true'&&needs.release-cli-images.result=='success'&&needs.release-cli-images.outputs.validated=='true'"
    );
    let checkout = &steps(record)[0];
    assert_eq!(checkout["with"]["ref"], "${{ github.sha }}");
    assert_eq!(checkout["with"]["fetch-depth"], 2);
    assert_eq!(checkout["with"]["persist-credentials"], false);
    let create = steps(record)
        .iter()
        .find(|step| step["id"] == "record")
        .unwrap();
    assert_eq!(create["env"]["CLI_JOB_RESULTS"], "${{ toJSON(needs) }}");
    let run = create["run"].as_str().unwrap();
    for required in [
        "cli record-validation",
        "--event \"$GITHUB_EVENT_PATH\"",
        "--source-commit \"$GITHUB_SHA\"",
        "--run-id \"$GITHUB_RUN_ID\"",
        "--run-attempt \"$GITHUB_RUN_ATTEMPT\"",
        "--job-results",
    ] {
        assert!(run.contains(required), "{required}");
    }
    for forbidden in [
        "download-artifact",
        "docker",
        "skopeo",
        "cli build",
        "cli test",
        "secrets.",
    ] {
        assert!(!record.to_string().contains(forbidden), "{forbidden}");
    }
    for step in steps(record) {
        assert!(step["if"].is_null());
        assert!(step["continue-on-error"].is_null());
    }
    assert_eq!(
        ci["jobs"]["changes"]["outputs"]["pr_head_sha"],
        "${{ github.event.pull_request.head.sha }}"
    );
    assert!(
        ci["jobs"]["ci"]["needs"]
            .as_array()
            .unwrap()
            .contains(&json!("release-cli-record"))
    );
    assert_eq!(ci["jobs"]["ci"]["if"], "always()");
}

#[test]
fn record_is_only_validated_after_an_immutable_upload_succeeds() -> TestResult {
    let ci = ci();
    let record = &ci["jobs"]["release-cli-record"];
    let steps = steps(record);
    let upload = &steps[steps.len() - 2];
    let complete = &steps[steps.len() - 1];
    assert_eq!(upload["id"], "upload");
    assert_eq!(upload["uses"], "actions/upload-artifact@v6");
    assert_eq!(
        upload["with"]["path"],
        "${{ runner.temp }}/cli-validation.json"
    );
    assert_eq!(upload["with"]["if-no-files-found"], "error");
    assert_eq!(upload["with"]["retention-days"], 90);
    assert!(upload["with"]["overwrite"].is_null());
    assert_eq!(
        record["env"]["CLI_RECORD_NAME"],
        "dar-cli-validation-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}"
    );
    assert_eq!(
        record["outputs"]["validated"],
        "${{ steps.complete.outputs.validated }}"
    );
    assert_eq!(
        record["outputs"]["artifact-id"],
        "${{ steps.upload.outputs.artifact-id }}"
    );
    let root = tempfile::tempdir()?;
    for (id, url) in [
        ("123", "https://example.invalid/artifact"),
        ("", "url"),
        ("0", "url"),
        ("invalid", "url"),
        ("123", ""),
    ] {
        let output_file = root.path().join("output");
        fs::write(&output_file, "")?;
        let output = Command::new("bash")
            .args(["-euo", "pipefail", "-c", complete["run"].as_str().unwrap()])
            .env("ARTIFACT_ID", id)
            .env("ARTIFACT_URL", url)
            .env("CLI_RECORD_NAME", "record-fixture")
            .env("GITHUB_OUTPUT", &output_file)
            .env("GITHUB_STEP_SUMMARY", root.path().join("summary"))
            .output()?;
        let expected = id == "123" && !url.is_empty();
        assert_eq!(output.status.success(), expected, "{output:?}");
        assert_eq!(
            fs::read_to_string(output_file)?.contains("validated=true"),
            expected
        );
    }
    Ok(())
}

#[test]
fn binary_and_image_handoffs_return_the_uploaded_manifest_hash_and_retain_all_inputs() -> TestResult
{
    for (workflow, directory, manifest) in [
        (
            include_str!("../../.github/workflows/cli-build.yml"),
            "cli-artifacts",
            "cli-artifacts.json",
        ),
        (
            include_str!("../../.github/workflows/cli-images.yml"),
            "cli-images",
            "cli-images.json",
        ),
    ] {
        let workflow: Value = serde_yaml_ng::from_str(workflow)?;
        for job in workflow["jobs"].as_object().unwrap().values() {
            for step in steps(job) {
                if step["uses"] == "actions/upload-artifact@v6" {
                    assert_eq!(step["with"]["retention-days"], 90);
                    if step["name"] == "Retain installed CLI invocation records" {
                        assert!(
                            step["with"]["name"]
                                .as_str()
                                .unwrap()
                                .contains("github.run_attempt")
                        );
                        assert_eq!(step["if"], "always()");
                    }
                }
            }
        }
        let validate = &workflow["jobs"]["validate"];
        assert_eq!(
            workflow["on"]["workflow_call"]["outputs"]["manifest-sha256"]["value"],
            "${{ jobs.validate.outputs.manifest-sha256 }}"
        );
        assert_eq!(
            validate["outputs"]["manifest-sha256"],
            "${{ steps.complete.outputs.manifest-sha256 }}"
        );
        let script = steps(validate).last().unwrap()["run"].as_str().unwrap();
        let root = tempfile::tempdir()?;
        fs::create_dir(root.path().join(directory))?;
        let contents = b"verified manifest fixture\n";
        let manifest_path = root.path().join(directory).join(manifest);
        fs::write(&manifest_path, contents)?;
        for (id, has_manifest) in [("123", true), ("", true), ("123", false)] {
            if !has_manifest {
                fs::remove_file(&manifest_path)?;
            }
            let outputs = root.path().join("outputs");
            fs::write(&outputs, "")?;
            let output = Command::new("bash")
                .args(["-euo", "pipefail", "-c", script])
                .env("RUNNER_TEMP", root.path())
                .env("GITHUB_OUTPUT", &outputs)
                .env("GITHUB_STEP_SUMMARY", root.path().join("summary"))
                .env("ARTIFACT_ID", id)
                .env("ARTIFACT_URL", "https://example.invalid/artifact")
                .env("CLI_ARTIFACT_NAME", "artifact-fixture")
                .env("CLI_SOURCE_COMMIT", "a".repeat(40))
                .env("CLI_VERSION", "1.2.3")
                .env("CLI_RUST_TOOLCHAIN", "1.94.1")
                .output()?;
            let expected = id == "123" && has_manifest;
            assert_eq!(output.status.success(), expected, "{output:?}");
            let text = fs::read_to_string(&outputs)?;
            assert_eq!(text.contains("validated=true"), expected);
            if expected {
                assert!(
                    text.contains(&format!("manifest-sha256={:x}\n", Sha256::digest(contents)))
                );
            }
        }
    }
    Ok(())
}
