#![cfg(unix)]

use std::{error::Error, fs, process::Command};

use serde_json::{Value, json};

fn workflow() -> Value {
    serde_yaml_ng::from_str(include_str!("../../.github/workflows/cli-build.yml")).unwrap()
}

#[test]
fn cli_builder_requires_an_explicit_source_commit() {
    let workflow = workflow();
    let events: Vec<_> = workflow["on"].as_object().unwrap().keys().collect();
    assert_eq!(events, ["workflow_call", "workflow_dispatch"]);
    for event in events {
        let source = &workflow["on"][event]["inputs"]["source-ref"];
        assert_eq!(source["required"], true);
        assert!(source["default"].is_null());
    }
}

#[test]
fn release_cli_build_waits_for_approval_and_blocks_the_ci_summary() {
    let ci: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml")).unwrap();
    assert!(ci["on"].get("pull_request").is_some());
    assert_eq!(ci["concurrency"]["cancel-in-progress"], true);
    assert_eq!(
        ci["concurrency"]["group"],
        "${{ github.workflow }}-${{ github.ref }}"
    );
    let jobs = &ci["jobs"];
    let release_pr = jobs["changes"]["outputs"]["release_pr"].as_str().unwrap();
    assert_eq!(
        release_pr.split_whitespace().collect::<String>(),
        "${{ github.repository == 'mag1cfrog/delta-arrow-reader' &&
            github.event.pull_request.head.repo.full_name == github.repository &&
            github.event.pull_request.base.ref == 'main' &&
            github.event.pull_request.user.login == 'release-plz-bot-mag1cfrog[bot]' &&
            startsWith(github.head_ref, 'release-plz-') }}"
            .split_whitespace()
            .collect::<String>()
    );
    let version = jobs["changes"]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["id"] == "package-version")
        .unwrap();
    assert!(version["if"].is_null());

    let approval = &jobs["release-approval"];
    assert_eq!(approval["environment"], "release-checks");
    assert_eq!(approval["needs"], "changes");
    for name in ["release-approval", "release-cli"] {
        assert_eq!(
            jobs[name]["if"],
            "needs.changes.outputs.release_pr == 'true'"
        );
        assert!(
            jobs["ci"]["needs"]
                .as_array()
                .unwrap()
                .contains(&json!(name))
        );
    }
    assert_eq!(jobs["ci"]["if"], "always()");
    let build = &jobs["release-cli"];
    assert_eq!(build["uses"], "./.github/workflows/cli-build.yml");
    assert_eq!(build["needs"], json!(["changes", "release-approval"]));
    assert_eq!(
        build["with"],
        json!({
            "source-ref": "${{ github.sha }}",
            "expected-version": "${{ needs.changes.outputs.package_version }}",
        })
    );
    for (name, job) in jobs.as_object().unwrap() {
        if job["uses"] == "./.github/workflows/cli-build.yml" {
            assert_eq!(name, "release-cli", "ungated CLI builder: {name}");
        }
    }
}

#[test]
fn ci_requires_approved_and_validated_cli_artifacts_only_for_release_prs()
-> Result<(), Box<dyn Error>> {
    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    let script = ci["jobs"]["ci"]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["name"] == "Check results")
        .unwrap()["run"]
        .as_str()
        .unwrap();
    for code in ["true", "false"] {
        for release_pr in ["true", "false"] {
            for approval in ["success", "failure", "skipped", "cancelled", ""] {
                for build in ["success", "failure", "skipped", "cancelled", ""] {
                    for validated in ["true", "false", ""] {
                        let values = [
                            ("changes.outputs.code", code),
                            ("changes.outputs.python", "false"),
                            ("changes.outputs.cli", "false"),
                            ("changes.outputs.tooling", "false"),
                            ("changes.outputs.selective_read", "false"),
                            ("changes.outputs.release_pr", release_pr),
                            ("release-approval.result", approval),
                            ("release-cli.result", build),
                            ("release-cli.outputs.validated", validated),
                        ];
                        let mut rest = script;
                        let mut rendered = String::new();
                        while let Some((before, after)) = rest.split_once("${{ needs.") {
                            let (key, after) = after.split_once(" }}").unwrap();
                            rendered.push_str(before);
                            let value = values.iter().find(|(name, _)| *name == key);
                            rendered.push_str(match value {
                                Some((_, value)) => value,
                                None if key.ends_with(".result") => "success",
                                None if key.ends_with(".outputs.validated") => "true",
                                None if key.ends_with(".outputs.source-commit") => {
                                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                }
                                None if key.ends_with(".outputs.version") => "1.2.3",
                                None if key == "changes.outputs.pr_head_sha"
                                    || key.ends_with(".outputs.pr-head-sha") =>
                                {
                                    "pr-head"
                                }
                                None if key.ends_with(".outputs.artifact-id")
                                    || key.ends_with(".outputs.artifact-name")
                                    || key.ends_with(".outputs.artifact-url") =>
                                {
                                    "present"
                                }
                                None => panic!("missing CI test input: {key}"),
                            });
                            rest = after;
                        }
                        rendered.push_str(rest);
                        let output = Command::new("bash")
                            .args(["-euo", "pipefail", "-c", &rendered])
                            .output()?;
                        let expected = release_pr != "true"
                            || approval == "success" && build == "success" && validated == "true";
                        assert_eq!(
                            output.status.success(),
                            expected,
                            "code={code}, release_pr={release_pr}, approval={approval}, build={build}, validated={validated}: {output:?}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn cli_handoff_requires_source_checks_builds_and_runtime_tests() -> Result<(), Box<dyn Error>> {
    let workflow = workflow();
    let validate = &workflow["jobs"]["validate"];
    let stages = ["prepare", "checks", "build", "runtime"];
    let variables = [
        "PREPARE_RESULT",
        "CHECKS_RESULT",
        "BUILD_RESULT",
        "RUNTIME_RESULT",
    ];
    assert_eq!(validate["needs"], json!(stages));
    assert_eq!(validate["if"], "always()");
    for (stage, variable) in stages.iter().zip(variables) {
        assert_eq!(
            validate["env"][variable],
            format!("${{{{ needs.{stage}.result }}}}")
        );
    }
    let steps = validate["steps"].as_array().unwrap();
    let script = format!(
        "{}\n{}",
        steps.first().unwrap()["run"].as_str().unwrap(),
        steps.last().unwrap()["run"].as_str().unwrap()
    );
    let directory = tempfile::tempdir()?;
    fs::create_dir(directory.path().join("cli-artifacts"))?;
    fs::write(
        directory.path().join("cli-artifacts/cli-artifacts.json"),
        "manifest fixture",
    )?;
    let handoff = directory.path().join("output");
    let summary = directory.path().join("summary");
    for (stage, variable) in stages.iter().zip(variables) {
        for result in ["success", "failure", "skipped", "cancelled", ""] {
            fs::write(&handoff, "")?;
            let output = Command::new("bash")
                .args(["-euo", "pipefail", "-c", &script])
                .envs(variables.map(|name| (name, "success")))
                .env(variable, result)
                .env("GITHUB_OUTPUT", &handoff)
                .env("RUNNER_TEMP", directory.path())
                .env("GITHUB_STEP_SUMMARY", &summary)
                .env(
                    "CLI_SOURCE_COMMIT",
                    "0000000000000000000000000000000000000000",
                )
                .env("CLI_VERSION", "0.0.0")
                .env("CLI_RUST_TOOLCHAIN", "1.94.1")
                .env("CLI_ARTIFACT_NAME", "test-artifact")
                .env("ARTIFACT_ID", "123")
                .env("ARTIFACT_URL", "https://example.invalid/artifact")
                .output()?;
            let expected = result == "success";
            assert_eq!(
                output.status.success(),
                expected,
                "{stage}={result}: {output:?}"
            );
            assert_eq!(
                fs::read_to_string(&handoff)?.contains("validated=true"),
                expected
            );
        }
    }
    Ok(())
}
