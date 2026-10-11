#![cfg(unix)]

use std::{error::Error, fs, process::Command};

use serde_json::{Value, json};

fn workflow() -> Value {
    serde_yaml_ng::from_str(include_str!("../../.github/workflows/cli-build.yml")).unwrap()
}

#[test]
fn cli_validation_requires_an_explicit_run_and_source_commit() {
    let workflow = workflow();
    let events: Vec<_> = workflow["on"].as_object().unwrap().keys().collect();
    assert_eq!(events, ["workflow_call", "workflow_dispatch"]);
    for event in events {
        let source = &workflow["on"][event]["inputs"]["source-ref"];
        assert_eq!(source["required"], true);
        assert!(source["default"].is_null());
    }

    let ci: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml")).unwrap();
    let jobs = ci["jobs"].as_object().unwrap();
    assert!(!jobs.contains_key("cli"));
    assert!(!jobs.contains_key("cli-binaries"));
    for job in jobs.values() {
        assert_ne!(job["uses"], "./.github/workflows/cli-build.yml");
    }
    let needs = ci["jobs"]["ci"]["needs"].as_array().unwrap();
    assert!(!needs.contains(&json!("cli")));
    assert!(!needs.contains(&json!("cli-binaries")));
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
                .env("GITHUB_STEP_SUMMARY", &summary)
                .env(
                    "CLI_SOURCE_COMMIT",
                    "0000000000000000000000000000000000000000",
                )
                .env("CLI_VERSION", "0.0.0")
                .env("CLI_RUST_TOOLCHAIN", "1.94.1")
                .env("CLI_ARTIFACT_NAME", "test-artifact")
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
