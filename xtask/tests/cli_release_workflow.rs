#![cfg(unix)]

use std::{error::Error, fs, os::unix::fs::PermissionsExt, process::Command};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

type TestResult = Result<(), Box<dyn Error>>;

fn workflow() -> Value {
    serde_yaml_ng::from_str(include_str!("../../.github/workflows/cli-release.yml")).unwrap()
}

fn steps(job: &Value) -> &[Value] {
    job["steps"].as_array().unwrap()
}

fn named_step<'a>(job: &'a Value, name: &str) -> &'a Value {
    steps(job).iter().find(|step| step["name"] == name).unwrap()
}

#[test]
fn only_published_releases_or_explicit_recovery_can_start_publication() {
    let workflow = workflow();
    assert_eq!(workflow["on"]["release"], json!({"types":["published"]}));
    assert_eq!(workflow["on"].as_object().unwrap().len(), 2);
    let inputs = &workflow["on"]["workflow_dispatch"]["inputs"];
    assert_eq!(inputs["mode"]["default"], "rehearse");
    assert_eq!(inputs["mode"]["options"], json!(["rehearse", "publish"]));
    for name in ["tag", "validation-run-id", "record-artifact-id"] {
        assert_eq!(inputs[name]["required"], true);
        assert_eq!(inputs[name]["type"], "string");
        assert!(inputs[name]["default"].is_null());
    }
    assert_eq!(workflow["permissions"], json!({"contents":"read"}));
    assert_eq!(workflow["defaults"]["run"]["shell"], "bash");
    assert_eq!(
        workflow["concurrency"],
        json!({
            "group":"cli-release-${{ github.event.release.tag_name || inputs.tag }}",
            "cancel-in-progress":false
        })
    );
    let prepare = &workflow["jobs"]["prepare"];
    assert_eq!(
        prepare["if"],
        "github.repository == 'mag1cfrog/delta-arrow-reader'"
    );
    assert_eq!(
        named_step(prepare, "Check event and select validation")["run"],
        "cargo xtask cli check-release-request"
    );

    let release_plz: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/release-plz.yml")).unwrap();
    let release = &release_plz["jobs"]["release"];
    assert_eq!(
        named_step(release, "Generate GitHub token")["uses"],
        "actions/create-github-app-token@v3"
    );
    assert_eq!(
        named_step(release, "Run release-plz")["env"]["GITHUB_TOKEN"],
        "${{ steps.generate-token.outputs.token }}"
    );
}

#[test]
fn verified_artifacts_and_tools_cross_separate_permission_boundaries() {
    let workflow = workflow();
    let jobs = &workflow["jobs"];
    for (name, permissions) in [
        (
            "prepare",
            json!({"contents":"read", "actions":"read", "pull-requests":"read", "packages":"read"}),
        ),
        (
            "assets",
            json!({"contents":"write", "actions":"read", "pull-requests":"read", "packages":"read"}),
        ),
        (
            "images",
            json!({"contents":"read", "actions":"read", "pull-requests":"read", "packages":"write"}),
        ),
        (
            "delivery",
            json!({"contents":"read", "actions":"read", "pull-requests":"read"}),
        ),
        (
            "notes",
            json!({"contents":"write", "actions":"read", "pull-requests":"read"}),
        ),
    ] {
        assert_eq!(jobs[name]["permissions"], permissions, "{name}");
    }
    let prepare = &jobs["prepare"];
    let checkouts: Vec<_> = steps(prepare)
        .iter()
        .filter(|step| step["uses"] == "actions/checkout@v5")
        .collect();
    assert_eq!(checkouts.len(), 2);
    assert_eq!(checkouts[0]["with"]["ref"], "${{ github.workflow_sha }}");
    assert_eq!(
        checkouts[1]["with"]["ref"],
        "${{ steps.request.outputs.release-commit }}"
    );
    assert!(
        checkouts
            .iter()
            .all(|step| step["with"]["persist-credentials"] == false)
    );
    let upload = named_step(prepare, "Save the verified plan and publisher");
    assert_eq!(upload["uses"], "actions/upload-artifact@v6");
    assert!(upload["with"]["overwrite"].is_null());
    assert_eq!(upload["with"]["if-no-files-found"], "error");
    let rehearsal = named_step(prepare, "Rehearse all publication operations")["run"]
        .as_str()
        .unwrap();
    for command in [
        "update-release-notes",
        "upload-release-assets",
        "upload-release-images",
    ] {
        assert!(
            rehearsal
                .lines()
                .any(|line| line.contains(command) && line.contains("--dry-run"))
        );
    }
    assert!(rehearsal.contains("publisher-sha256=$(sha256sum"));
    assert!(rehearsal.contains("plan-sha256=$plan_sha256"));
    for name in ["assets", "images", "delivery", "notes"] {
        let job = &jobs[name];
        assert_eq!(
            job["env"]["PLAN_SHA256"],
            "${{ needs.prepare.outputs.plan-sha256 }}"
        );
        assert_eq!(
            job["env"]["PUBLISHER_SHA256"],
            "${{ needs.prepare.outputs.publisher-sha256 }}"
        );
        let downloads: Vec<_> = steps(job)
            .iter()
            .filter(|step| step["uses"] == "actions/download-artifact@v6")
            .collect();
        assert_eq!(downloads.len(), 1);
        assert_eq!(
            downloads[0]["with"],
            json!({
                "artifact-ids":"${{ needs.prepare.outputs.artifact-id }}",
                "path":"${{ runner.temp }}/cli-publication"
            })
        );
        assert!(
            steps(job)
                .iter()
                .all(|step| step["uses"] != "actions/checkout@v5")
        );
        let scripts: Vec<_> = steps(job)
            .iter()
            .filter_map(|step| step["run"].as_str())
            .collect();
        assert!(!scripts.join("\n").contains("cargo "));
        let verify = scripts
            .iter()
            .position(|script| script.contains("sha256sum --check"))
            .unwrap();
        let execute = scripts
            .iter()
            .position(|script| script.contains("xtask cli"))
            .unwrap();
        assert!(verify < execute);
    }
    let claim = named_step(
        &jobs["assets"],
        "Record the selected validation and upload missing assets",
    )["run"]
        .as_str()
        .unwrap();
    assert!(
        claim.find("update-release-notes").unwrap() < claim.find("upload-release-assets").unwrap()
    );
    assert_eq!(jobs["assets"]["needs"], "prepare");
    assert_eq!(jobs["images"]["needs"], json!(["prepare", "assets"]));
    assert_eq!(
        jobs["delivery"]["needs"],
        json!(["prepare", "assets", "images"])
    );
    assert_eq!(jobs["notes"]["needs"], json!(["prepare", "delivery"]));
    assert_eq!(
        jobs["notes"]["if"],
        "needs.prepare.outputs.publish == 'true' && needs.delivery.outputs.verified == 'true'"
    );
    for job in jobs.as_object().unwrap().values() {
        assert!(job["continue-on-error"].is_null());
        assert!(job["uses"].is_null());
        assert!(
            steps(job)
                .iter()
                .all(|step| step["continue-on-error"].is_null())
        );
    }
    let text = workflow.to_string();
    for forbidden in [
        "cli-build.yml",
        "cli-images.yml",
        "build-archive",
        "build-image",
        "test-image",
        "test-archive",
        "docker run",
        "cargo test",
        "--clobber",
        "latest",
        "id-token",
    ] {
        assert!(
            !text.contains(forbidden),
            "unexpected publication capability: {forbidden}"
        );
    }
}

#[test]
fn changed_handoff_tools_are_rejected_before_becoming_executable() -> TestResult {
    let workflow = workflow();
    for name in ["assets", "images", "delivery", "notes"] {
        let script = named_step(
            &workflow["jobs"][name],
            "Verify publication tools before execution",
        )["run"]
            .as_str()
            .unwrap();
        for corrupt in ["neither", "xtask", "regctl"] {
            let root = tempfile::tempdir()?;
            for tool in ["xtask", "regctl"] {
                let path = root.path().join(tool);
                fs::write(&path, if corrupt == tool { "changed" } else { tool })?;
                fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
            }
            let path_file = root.path().join("github-path");
            let output = Command::new("bash")
                .args(["-e", "-o", "pipefail", "-c", script])
                .env("PUBLICATION", root.path())
                .env("GITHUB_PATH", &path_file)
                .env(
                    "PUBLISHER_SHA256",
                    format!("{:x}", Sha256::digest(b"xtask")),
                )
                .env("REGCTL_SHA256", format!("{:x}", Sha256::digest(b"regctl")))
                .output()?;
            assert_eq!(
                output.status.success(),
                corrupt == "neither",
                "{name}/{corrupt}: {output:?}"
            );
            assert_eq!(path_file.exists(), corrupt == "neither");
            for tool in ["xtask", "regctl"] {
                assert_eq!(
                    fs::metadata(root.path().join(tool))?.permissions().mode() & 0o111 != 0,
                    corrupt == "neither"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn both_download_checks_must_pass_before_delivery_is_reported() -> TestResult {
    let workflow = workflow();
    let script = named_step(
        &workflow["jobs"]["delivery"],
        "Download and verify published CLI bytes",
    )["run"]
        .as_str()
        .unwrap();
    let root = tempfile::tempdir()?;
    let tool = root.path().join("xtask");
    fs::write(
        &tool,
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$2\" >> \"$PUBLICATION/calls\"\n[ \"$2\" != \"$FAIL_COMMAND\" ]\n",
    )?;
    fs::set_permissions(tool, fs::Permissions::from_mode(0o700))?;
    for failure in ["neither", "verify-release-assets", "verify-release-images"] {
        let handoff = root.path().join("output");
        fs::write(&handoff, "")?;
        fs::write(root.path().join("calls"), "")?;
        let output = Command::new("bash")
            .args(["-e", "-o", "pipefail", "-c", script])
            .env(
                "PATH",
                format!("{}:{}", root.path().display(), std::env::var("PATH")?),
            )
            .env("PUBLICATION", root.path())
            .env("PLAN_SHA256", "producer-hash")
            .env("GITHUB_OUTPUT", &handoff)
            .env("FAIL_COMMAND", failure)
            .output()?;
        assert_eq!(
            output.status.success(),
            failure == "neither",
            "{failure}: {output:?}"
        );
        assert_eq!(
            fs::read_to_string(handoff)?.contains("verified=true"),
            failure == "neither"
        );
        assert_eq!(
            fs::read_to_string(root.path().join("calls"))?,
            if failure == "verify-release-assets" {
                "verify-release-assets\n"
            } else {
                "verify-release-assets\nverify-release-images\n"
            }
        );
    }
    Ok(())
}

#[test]
fn failed_or_skipped_jobs_cannot_report_complete_publication() -> TestResult {
    let workflow = workflow();
    let result = &workflow["jobs"]["result"];
    assert_eq!(result["if"], "always()");
    assert_eq!(
        result["needs"],
        json!(["prepare", "assets", "images", "delivery", "notes"])
    );
    let script = steps(result)[0]["run"].as_str().unwrap();
    let root = tempfile::tempdir()?;
    for publish in ["true", "false"] {
        let job_result = if publish == "true" {
            "success"
        } else {
            "skipped"
        };
        let valid = [
            ("PREPARE", "success"),
            ("PUBLISH", publish),
            ("ASSETS", job_result),
            ("IMAGES", job_result),
            ("DELIVERY", job_result),
            ("NOTES", job_result),
            ("VERIFIED", "true"),
        ];
        for (key, expected) in valid {
            for value in [expected, "failure", "cancelled", "skipped", "success", ""] {
                let summary = root.path().join("summary");
                fs::write(&summary, "")?;
                let output = Command::new("bash")
                    .args(["-e", "-o", "pipefail", "-c", script])
                    .envs(valid)
                    .env(key, value)
                    .env("GITHUB_STEP_SUMMARY", &summary)
                    .output()?;
                let passes = value == expected || (publish == "false" && key == "VERIFIED");
                assert_eq!(
                    output.status.success(),
                    passes,
                    "{publish}/{key}/{value}: {output:?}"
                );
                assert_eq!(!fs::read_to_string(summary)?.is_empty(), passes);
            }
        }
    }
    Ok(())
}

#[test]
fn ordinary_ci_checks_publication_logic_without_running_publication() {
    let filters: Value =
        serde_json::from_str(include_str!("../../.github/path-filters.json")).unwrap();
    let path = json!(".github/workflows/cli-release.yml");
    assert!(filters["tooling"].as_array().unwrap().contains(&path));
    for name in ["code", "python", "cli"] {
        assert!(!filters[name].as_array().unwrap().contains(&path));
    }
    let ci: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml")).unwrap();
    let tooling = &ci["jobs"]["tooling-checks"];
    assert!(
        named_step(tooling, "Check CI workflows")["with"]["args"]
            .as_str()
            .unwrap()
            .contains(".github/workflows/cli-release.yml")
    );
    let scripts = steps(tooling)
        .iter()
        .filter_map(|step| step["run"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    for target in [
        "cli_release_request",
        "cli_release_notes",
        "cli_release_workflow",
    ] {
        assert!(scripts.contains(&format!("--test {target}")));
    }
    assert!(
        !ci.to_string()
            .contains("./.github/workflows/cli-release.yml")
    );
}
