//! CLI build workflow inputs, approvals, and toolchain checks.

#![allow(clippy::panic, clippy::unwrap_used)]
#![cfg(unix)]

use std::{error::Error, fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

use serde_json::{Value, json};

fn workflow() -> Value {
    serde_yaml_ng::from_str(include_str!("../../.github/workflows/cli-build.yml")).unwrap()
}

#[test]
fn prepare_uses_the_installed_patch_toolchain_and_checks_its_release() -> Result<(), Box<dyn Error>>
{
    let temporary = tempfile::tempdir()?;
    for (name, script) in [
        (
            "git",
            "#!/bin/sh\ncase \"$*\" in\n'rev-parse HEAD') printf '%s\\n' \"$SOURCE_COMMIT\";;\n'status --porcelain --untracked-files=all') ;;\n*) exit 1;;\nesac\n",
        ),
        (
            "rustup",
            "#!/bin/sh\n[ \"$*\" = \"run $INSTALLED_TOOLCHAIN rustc --version --verbose\" ] || { echo 'toolchain is not installed' >&2; exit 1; }\nprintf 'release: %s\\n' \"$COMPILER_RELEASE\"\n",
        ),
    ] {
        let path = temporary.path().join(name);
        fs::write(&path, script)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    }
    let manifest: toml::Value = toml::from_str(include_str!("../../Cargo.toml"))?;
    let declared = manifest["workspace"]["package"]["rust-version"]
        .as_str()
        .unwrap();
    let installed = if declared.split('.').count() == 2 {
        format!("{declared}.1")
    } else {
        declared.to_owned()
    };
    let (minor, patch) = installed.rsplit_once('.').unwrap();
    let other_patch = format!("{minor}.{}", patch.parse::<u32>()? + 1);
    let source = "a".repeat(40);
    for (requested, reported, success) in [
        (None, installed.as_str(), declared == installed),
        (Some(installed.as_str()), installed.as_str(), true),
        (Some("1.0.0"), installed.as_str(), false),
        (Some(installed.as_str()), "1.0.0", false),
        (Some(installed.as_str()), other_patch.as_str(), false),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        command.args(["cli", "prepare", "--source-commit", &source]);
        if let Some(toolchain) = requested {
            command.args(["--rust-toolchain", toolchain]);
        }
        let output = command
            .env("PATH", temporary.path())
            .env("SOURCE_COMMIT", &source)
            .env("INSTALLED_TOOLCHAIN", &installed)
            .env("COMPILER_RELEASE", reported)
            .output()?;
        assert_eq!(
            output.status.success(),
            success,
            "{requested:?}: {output:?}"
        );
        if success {
            let identity: Value = serde_json::from_slice(&output.stdout)?;
            assert_eq!(identity["rust-toolchain"], installed);
            assert_eq!(identity["source-commit"], source);
        } else {
            assert!(
                output.stdout.is_empty(),
                "must not report an invalid identity"
            );
        }
    }
    Ok(())
}

#[test]
fn debian_build_uses_the_restored_target_and_prebuilt_notice_tool() -> Result<(), Box<dyn Error>> {
    let workflow = workflow();
    let job = &workflow["jobs"]["build"];
    let steps = job["steps"].as_array().unwrap();
    let cache = steps
        .iter()
        .find(|step| step["uses"] == "Swatinem/rust-cache@v2")
        .unwrap();
    assert_eq!(cache["with"]["workspaces"], ". -> target/cli-debian");
    assert!(
        cache["with"]["shared-key"]
            .as_str()
            .unwrap()
            .contains("needs.prepare.outputs.image")
    );
    assert_eq!(cache["with"]["cache-bin"], false);
    let script = steps
        .iter()
        .find(|step| step["name"] == "Build and package in pinned Debian 12")
        .unwrap()["run"]
        .as_str()
        .unwrap();
    let temporary = tempfile::tempdir()?;
    let bin = temporary.path().join("bin");
    fs::create_dir(&bin)?;
    for (name, script) in [
        ("cargo-about", "#!/bin/sh\nexit 0\n"),
        (
            "docker",
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$DOCKER_ARGS\"\nexit \"$DOCKER_EXIT\"\n",
        ),
    ] {
        let path = bin.join(name);
        fs::write(&path, script)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    }
    let arguments = temporary.path().join("docker-args");
    for exit in [0, 7] {
        let output = Command::new("bash")
            .args(["-euo", "pipefail", "-c", script])
            .current_dir(temporary.path())
            .env(
                "PATH",
                std::env::join_paths([bin.as_path(), Path::new("/usr/bin"), Path::new("/bin")])?,
            )
            .env("RUNNER_TEMP", temporary.path())
            .env("GITHUB_WORKSPACE", temporary.path())
            .env("CARGO_HOME", temporary.path().join("cargo"))
            .env("RUSTUP_HOME", temporary.path().join("rustup"))
            .env("CLI_ARCHITECTURE", "amd64")
            .env("CLI_IMAGE", "debian-fixture")
            .env("DOCKER_ARGS", &arguments)
            .env("DOCKER_EXIT", exit.to_string())
            .output()?;
        assert_eq!(output.status.code(), Some(exit), "{output:?}");
        let arguments = fs::read_to_string(&arguments)?;
        for mount in [
            format!(
                "{}/target/cli-debian:/build/target",
                temporary.path().display()
            ),
            format!(
                "{}/cargo/registry:/cargo/registry",
                temporary.path().display()
            ),
            format!("{}/cargo/git:/cargo/git", temporary.path().display()),
            format!(
                "{}/cargo-about:/usr/local/bin/cargo-about:ro",
                bin.display()
            ),
        ] {
            assert!(arguments.lines().any(|line| line == mount), "{mount}");
        }
        assert!(arguments.contains("--package xtask --bin xtask"));
        assert!(!arguments.contains("cargo install"));
    }
    let arguments = fs::read_to_string(&arguments)?;
    let container_script = arguments
        .split_once("bash\n-euo\npipefail\n-c\n")
        .unwrap()
        .1;
    let script = format!(
        r#"
apt-get() {{ :; }}
git() {{ :; }}
install() {{ :; }}
strip() {{ :; }}
rustup() {{
  printf '%s\n' "$*"
  case "$*" in *"cargo $FAIL_CARGO "*) return 7 ;; esac
  return 0
}}
{container_script}
"#
    );
    for cache_only in ["true", "false"] {
        for fail in ["", "test", "run"] {
            let output = Command::new("bash")
                .args(["-euo", "pipefail", "-c", &script])
                .env("CACHE_ONLY", cache_only)
                .env("FAIL_CARGO", fail)
                .env("CLI_RUST_TOOLCHAIN", "1.94.1")
                .env("CLI_SOURCE_COMMIT", "a".repeat(40))
                .env("CLI_ARCHITECTURE", "amd64")
                .env("CLI_VERSION", "1.2.3")
                .output()?;
            let should_fail = fail == "run" || fail == "test" && cache_only == "false";
            assert_eq!(
                output.status.success(),
                !should_fail,
                "{cache_only}, {fail}: {output:?}"
            );
            let calls = String::from_utf8(output.stdout)?;
            assert_eq!(calls.contains("cargo test "), cache_only == "false");
            assert_eq!(
                calls.contains("cargo run "),
                !(fail == "test" && cache_only == "false")
            );
        }
    }
    Ok(())
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
                            ("changes.outputs.rust_docs", "false"),
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
    assert_eq!(validate["if"], "${{ always() && !inputs.cache-only }}");
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
