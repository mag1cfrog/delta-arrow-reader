#![cfg(unix)]

use std::{collections::BTreeSet, fs, process::Command};

use serde_json::{Value, json};

mod support;
use support::{Result, WheelFixture};

fn workflow() -> Value {
    serde_yaml_ng::from_str(include_str!("../../.github/workflows/pypi.yml")).unwrap()
}

#[test]
fn build_only_is_the_default() {
    let workflow = workflow();
    let inputs = &workflow["on"]["workflow_dispatch"]["inputs"];
    assert_eq!(inputs["publish-to"]["default"], "none");
    assert_eq!(
        inputs["publish-to"]["options"],
        json!(["none", "testpypi", "pypi"])
    );
}

#[test]
fn automatic_release_requires_a_published_event() {
    let workflow = workflow();
    let events: Vec<_> = workflow["on"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(events, ["release", "workflow_dispatch"]);
    assert_eq!(workflow["on"]["release"], json!({"types": ["published"]}));
}

#[test]
fn only_upload_jobs_can_request_oidc() {
    let workflow = workflow();
    assert_eq!(workflow["permissions"], json!({"contents": "read"}));
    for (name, job) in workflow["jobs"].as_object().unwrap() {
        assert_eq!(
            job["permissions"]["id-token"] == "write",
            matches!(name.as_str(), "publish-testpypi" | "publish-pypi"),
            "{name}",
        );
    }
}

#[test]
fn publisher_identity_and_upload_gates_match_the_selected_index() {
    let workflow = workflow();
    for (target, project_url, upload_url) in [
        (
            "testpypi",
            "https://test.pypi.org",
            "https://test.pypi.org/legacy/",
        ),
        (
            "pypi",
            "https://pypi.org",
            "https://upload.pypi.org/legacy/",
        ),
    ] {
        let upload = &workflow["jobs"][format!("publish-{target}")];
        assert_eq!(
            upload["environment"],
            json!({
                "name": format!("{target}-release"),
                "url": format!("{project_url}/project/delta-arrow-reader/"),
            })
        );
        let needs: BTreeSet<_> = upload["needs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|job| job.as_str().unwrap())
            .collect();
        assert_eq!(needs, BTreeSet::from(["prepare", "check"]));
        let events = if target == "pypi" {
            "(github.event_name == 'workflow_dispatch' || github.event_name == 'release')"
        } else {
            "github.event_name == 'workflow_dispatch'"
        };
        assert_eq!(
            upload["if"]
                .as_str()
                .unwrap()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" "),
            format!(
                "github.repository == 'mag1cfrog/delta-arrow-reader' && {events} && \
                 needs.prepare.outputs.publish-to == '{target}' && needs.check.result == 'success'"
            ),
        );
        let publish = upload["steps"].as_array().unwrap().last().unwrap();
        assert_eq!(publish["uses"], "pypa/gh-action-pypi-publish@release/v1");
        assert_eq!(
            publish["with"],
            json!({
                "repository-url": upload_url,
                "packages-dir": "dist/",
                "skip-existing": false,
            })
        );
    }
}

#[test]
fn upload_reuses_artifacts_and_only_sends_missing_files() -> Result<()> {
    let workflow = workflow();
    for target in ["testpypi", "pypi"] {
        let upload = &workflow["jobs"][format!("publish-{target}")];
        assert_eq!(
            upload["concurrency"],
            json!({
                "group": format!("python-{target}-") + "${{ needs.prepare.outputs.expected-version }}",
                "cancel-in-progress": false,
            })
        );
        assert_eq!(
            upload["env"],
            json!({
                "EXPECTED_VERSION": "${{ needs.prepare.outputs.expected-version }}",
            })
        );
        let steps = upload["steps"].as_array().unwrap();
        assert_eq!(
            steps[0]["with"],
            json!({
                "ref": "${{ needs.prepare.outputs.source-ref }}",
                "persist-credentials": false,
            })
        );
        let download = steps
            .iter()
            .find(|step| {
                step["uses"]
                    .as_str()
                    .is_some_and(|action| action.starts_with("actions/download-artifact@"))
            })
            .unwrap();
        assert_eq!(
            download["with"],
            json!({
                "pattern": "delta-arrow-reader-*-wheel",
                "path": "wheel-artifacts",
            })
        );
        let stage = &steps[steps.len() - 2];
        assert_eq!(stage["id"], "stage");
        let script = format!(
            "cargo() {{ printf '%s\\n' \"$@\"; }}\n{}",
            stage["run"].as_str().unwrap()
        );
        let output = Command::new("bash")
            .args(["-e", "-o", "pipefail", "-c", &script])
            .env("EXPECTED_VERSION", "1.2.3")
            .output()?;
        assert!(output.status.success(), "{target}: {output:?}");
        assert_eq!(
            String::from_utf8(output.stdout)?
                .lines()
                .collect::<Vec<_>>(),
            [
                "xtask",
                "stage-python-wheels",
                "wheel-artifacts",
                "--expected-version",
                "1.2.3",
                "--index",
                target,
                "--destination",
                "dist",
            ]
        );
        assert_eq!(
            steps.last().unwrap()["if"],
            "steps.stage.outputs.has-files == 'true'"
        );
    }
    Ok(())
}

#[test]
fn upload_waits_for_successful_artifact_validation() -> Result<()> {
    let workflow = workflow();
    let check = &workflow["jobs"]["check"];
    let needs: BTreeSet<_> = check["needs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|job| job.as_str().unwrap())
        .collect();
    assert_eq!(needs, BTreeSet::from(["prepare", "build"]));
    assert_eq!(check["if"], "always()");
    assert_eq!(
        check["env"],
        json!({
            "PREPARE_RESULT": "${{ needs.prepare.result }}",
            "BUILD_RESULT": "${{ needs.build.result }}",
            "WHEELS_VALIDATED": "${{ needs.build.outputs.validated }}",
        })
    );
    for prepare in ["success", "failure", "skipped", "cancelled"] {
        for build in ["success", "failure", "skipped", "cancelled"] {
            for validated in ["true", "false", ""] {
                let output = Command::new("bash")
                    .args([
                        "-e",
                        "-o",
                        "pipefail",
                        "-c",
                        check["steps"][0]["run"].as_str().unwrap(),
                    ])
                    .env("PREPARE_RESULT", prepare)
                    .env("BUILD_RESULT", build)
                    .env("WHEELS_VALIDATED", validated)
                    .output()?;
                let expected = prepare == "success" && build == "success" && validated == "true";
                assert_eq!(
                    output.status.success(),
                    expected,
                    "{prepare}, {build}, {validated}: {output:?}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn incomplete_artifacts_cannot_reach_the_upload_directory() -> Result<()> {
    let workflow = workflow();
    let fixture = WheelFixture::new("workflow-upload")?;
    let directory = &fixture.directory;
    fs::create_dir(directory.join("wheel-artifacts"))?;
    for target in ["testpypi", "pypi"] {
        let steps = workflow["jobs"][format!("publish-{target}")]["steps"]
            .as_array()
            .unwrap();
        let stage = steps.iter().find(|step| step["id"] == "stage").unwrap();
        let script = stage["run"]
            .as_str()
            .unwrap()
            .replace("cargo xtask", "\"$XTASK_BINARY\"");
        let output = Command::new("bash")
            .args(["-e", "-o", "pipefail", "-c", &script])
            .current_dir(directory)
            .env("XTASK_BINARY", env!("CARGO_BIN_EXE_xtask"))
            .env("EXPECTED_VERSION", "1.2.3")
            .env("GITHUB_OUTPUT", directory.join("outputs"))
            .output()?;
        assert!(!output.status.success(), "{target}: {output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("expected artifacts"),
            "{output:?}"
        );
        assert!(!directory.join("dist").exists());
        assert!(!directory.join("outputs").exists());
    }
    Ok(())
}

#[test]
fn builder_reports_validation_only_after_successful_builds_and_complete_artifacts() -> Result<()> {
    let builder: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/pypi-build.yml"))?;
    assert_eq!(
        builder["on"]["workflow_call"]["outputs"]["validated"]["value"],
        "${{ jobs.validate.outputs.validated }}"
    );
    let validation = &builder["jobs"]["validate"];
    assert_eq!(validation["needs"], "build");
    assert_eq!(validation["if"], "${{ always() && !inputs.cache-only }}");
    assert_eq!(
        validation["outputs"]["validated"],
        "${{ steps.verify.outputs.validated }}"
    );
    assert_eq!(
        validation["env"],
        json!({
            "BUILD_RESULT": "${{ needs.build.result }}",
            "EXPECTED_VERSION": "${{ inputs.expected-version }}",
        })
    );
    let steps = validation["steps"].as_array().unwrap();
    let verify = steps.iter().find(|step| step["id"] == "verify").unwrap();
    let script = format!(
        "{}\n{}",
        steps[0]["run"].as_str().unwrap(),
        verify["run"].as_str().unwrap()
    )
    .replace("cargo xtask", "\"$XTASK_BINARY\"");
    let fixture = WheelFixture::new("workflow-validation")?;
    let (artifacts, wheels) = fixture.create_artifacts()?;
    let configured_artifacts: BTreeSet<_> =
        builder["jobs"]["build"]["strategy"]["matrix"]["include"]
            .as_array()
            .unwrap()
            .iter()
            .map(|platform| platform["artifact"].as_str().unwrap().to_owned())
            .collect();
    let artifact_names: BTreeSet<_> = fs::read_dir(artifacts)?
        .map(|entry| entry.map(|entry| entry.file_name().into_string().unwrap()))
        .collect::<std::io::Result<_>>()?;
    assert_eq!(artifact_names, configured_artifacts);
    let output_file = fixture.directory.join("outputs");
    for complete in [true, false] {
        if !complete {
            fs::remove_file(&wheels[0])?;
        }
        for build in ["success", "failure", "skipped", "cancelled"] {
            fs::write(&output_file, "existing=kept\n")?;
            let output = Command::new("bash")
                .args(["-e", "-o", "pipefail", "-c", &script])
                .current_dir(&fixture.directory)
                .env("XTASK_BINARY", env!("CARGO_BIN_EXE_xtask"))
                .env("EXPECTED_VERSION", "1.2.3")
                .env("BUILD_RESULT", build)
                .env("GITHUB_OUTPUT", &output_file)
                .output()?;
            let expected = complete && build == "success";
            assert_eq!(
                output.status.success(),
                expected,
                "complete={complete}, build={build}: {output:?}"
            );
            assert_eq!(
                fs::read_to_string(&output_file)?,
                if expected {
                    "existing=kept\nvalidated=true\n"
                } else {
                    "existing=kept\n"
                }
            );
        }
    }
    Ok(())
}
