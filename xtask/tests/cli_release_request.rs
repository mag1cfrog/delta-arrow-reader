//! Release event validation before publication outputs are created.

#![cfg(target_os = "linux")]

use std::{fs, process::Command};

use serde_json::{Value, json};

#[allow(dead_code)]
mod support;
use support::Result;
#[allow(dead_code)]
#[path = "support/release_selection.rs"]
mod release_selection_fixture;
use release_selection_fixture::{ARTIFACTS, Fixture, REPOSITORY};

fn event(automatic: bool) -> Value {
    if automatic {
        json!({"repository":{"full_name":REPOSITORY}, "action":"published",
            "release":{"id":7,"tag_name":"v1.2.3","draft":false,"prerelease":false}})
    } else {
        json!({"repository":{"full_name":REPOSITORY}, "inputs":{
            "mode":"rehearse","tag":"v1.2.3","validation-run-id":"701","record-artifact-id":"901"}})
    }
}

fn command(fixture: &Fixture, event: &Value, automatic: bool) -> Result<Command> {
    let root = fixture.directory.path();
    fs::write(root.join("event.json"), serde_json::to_vec(event)?)?;
    let reference = if automatic {
        "refs/tags/v1.2.3"
    } else {
        "refs/heads/main"
    };
    let mut command = fixture.xtask()?;
    command
        .args(["cli", "check-release-request"])
        .env("GITHUB_ACTIONS", "true")
        .env("GITHUB_REPOSITORY", REPOSITORY)
        .env("GITHUB_EVENT_PATH", root.join("event.json"))
        .env("GITHUB_OUTPUT", root.join("outputs"))
        .env(
            "GITHUB_EVENT_NAME",
            if automatic {
                "release"
            } else {
                "workflow_dispatch"
            },
        )
        .env("GITHUB_REF", reference)
        .env(
            "GITHUB_SHA",
            if automatic {
                "b".repeat(40)
            } else {
                "d".repeat(40)
            },
        )
        .env(
            "GITHUB_WORKFLOW_REF",
            format!("{REPOSITORY}/.github/workflows/cli-release.yml@{reference}"),
        );
    Ok(command)
}

#[test]
fn automatic_release_and_explicit_recovery_select_the_same_original_record() -> Result<()> {
    for (automatic, mode, publish) in [
        (true, "publish", true),
        (false, "rehearse", false),
        (false, "publish", true),
    ] {
        let fixture = Fixture::new()?;
        let mut event = event(automatic);
        if !automatic {
            event["inputs"]["mode"] = json!(mode);
        }
        let output = command(&fixture, &event, automatic)?.output()?;
        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            fs::read_to_string(fixture.directory.path().join("outputs"))?,
            format!(
                "tag=v1.2.3\npublish={publish}\nrelease-commit={}\nvalidation-run-id=701\nrecord-artifact-id=901\n",
                "b".repeat(40)
            )
        );
        let calls = fs::read_to_string(fixture.directory.path().join("calls"))?;
        assert!(calls.lines().all(|line| line.contains("--method GET")));
    }
    Ok(())
}

#[test]
fn malformed_or_untrusted_events_never_produce_publication_outputs() -> Result<()> {
    for (automatic, field, value) in [
        (true, "action", json!("edited")),
        (true, "draft", json!(true)),
        (true, "draft", json!(null)),
        (true, "prerelease", json!(true)),
        (true, "tag_name", json!("benchmarks-2026")),
        (true, "tag_name", json!("v1.2.3-rc.1")),
        (true, "id", json!(8)),
        (false, "tag", json!("v1.2.3\npublish=true")),
        (false, "mode", json!("unexpected")),
        (false, "validation-run-id", json!("")),
        (false, "validation-run-id", json!("0")),
        (false, "validation-run-id", json!("-1")),
        (false, "record-artifact-id", json!("+100")),
        (false, "record-artifact-id", json!("18446744073709551616")),
        (false, "record-artifact-id", json!(100)),
    ] {
        let fixture = Fixture::new()?;
        let mut event = event(automatic);
        if field == "action" {
            event[field] = value;
        } else {
            event[if automatic { "release" } else { "inputs" }][field] = value;
        }
        let output = command(&fixture, &event, automatic)?.output()?;
        assert!(!output.status.success(), "{field}: {output:?}");
        assert!(!fixture.directory.path().join("outputs").exists());
    }
    for (automatic, key, value) in [
        (true, "GITHUB_EVENT_NAME", "pull_request"),
        (true, "GITHUB_EVENT_NAME", "pull_request_target"),
        (true, "GITHUB_ACTIONS", "false"),
        (true, "GITHUB_REPOSITORY", "fork/delta-arrow-reader"),
        (true, "GITHUB_REF", "refs/heads/main"),
        (false, "GITHUB_REF", "refs/heads/feature"),
        (
            true,
            "GITHUB_WORKFLOW_REF",
            "mag1cfrog/delta-arrow-reader/.github/workflows/other.yml@refs/tags/v1.2.3",
        ),
        (false, "GITHUB_SHA", "invalid"),
    ] {
        let fixture = Fixture::new()?;
        let output = command(&fixture, &event(automatic), automatic)?
            .env(key, value)
            .output()?;
        assert!(!output.status.success(), "{key}: {output:?}");
        assert!(!fixture.directory.path().join("outputs").exists());
    }
    Ok(())
}

#[test]
fn changed_release_commits_and_invalid_artifacts_cannot_reach_publishing_jobs() -> Result<()> {
    for kind in ["commit", "payload", "expired", "record"] {
        let mut fixture = Fixture::new()?;
        let mut event = event(kind != "record");
        if kind == "expired" {
            fixture.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"][1]["expired"] =
                json!(true);
        }
        if kind == "payload" {
            event["repository"]["full_name"] = json!("fork/repository");
        }
        if kind == "record" {
            event["inputs"]["record-artifact-id"] = json!("999");
        }
        let mut command = command(&fixture, &event, kind != "record")?;
        if kind == "commit" {
            command.env("GITHUB_SHA", "e".repeat(40));
        }
        let output = command.output()?;
        assert!(!output.status.success(), "{kind}: {output:?}");
        assert!(!fixture.directory.path().join("outputs").exists());
    }
    Ok(())
}
