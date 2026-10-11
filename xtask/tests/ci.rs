#![cfg(unix)]

use std::{
    env,
    error::Error,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

fn test_dir(name: &str) -> PathBuf {
    let path = root().join(format!("xtask/target/test-{name}-{}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn workflow_script(source: &str, step: &str) -> String {
    let marker = format!("      - name: {step}\n");
    assert_eq!(source.matches(&marker).count(), 1);
    let (_, step) = source.split_once(&marker).unwrap();
    let (_, script) = step.split_once("        run: |\n").unwrap();
    let script = script
        .lines()
        .take_while(|line| line.is_empty() || line.starts_with("          "))
        .map(|line| line.strip_prefix("          ").unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!script.is_empty());
    script
}

#[test]
fn python_filter_covers_inputs_without_cli_benchmarks_or_docs() -> Result<()> {
    let filters: Value = serde_json::from_str(include_str!("../../.github/path-filters.json"))?;
    let patterns: Vec<_> = filters["python"]
        .as_array()
        .unwrap()
        .iter()
        .map(|pattern| pattern.as_str().unwrap())
        .collect();
    for (path, expected) in [
        ("src/reader/scan.rs", true),
        ("Cargo.lock", true),
        ("Cargo.toml", true),
        ("crates/delta-arrow-reader-cli/Cargo.toml", true),
        ("crates/delta-arrow-reader-python/src/lib.rs", true),
        ("crates/delta-arrow-reader-python/tests/test_table.py", true),
        ("tests/reader/https.py", true),
        (
            "tests/reader/fixtures/external_writer/corpus/table/data.parquet",
            true,
        ),
        ("pyproject.toml", true),
        ("delta_arrow_reader.pyi", true),
        ("README.md", true),
        ("LICENSE", true),
        (".cargo/config.toml", true),
        ("xtask/src/main.rs", true),
        ("xtask/Cargo.lock", true),
        (".github/workflows/cache-warm.yml", true),
        (".github/workflows/pypi-build.yml", true),
        (".github/workflows/pypi.yml", true),
        (".github/workflows/release-plz.yml", true),
        ("xtask/src/python_release.rs", true),
        ("xtask/src/python_wheels.rs", true),
        ("xtask/src/python_upload.rs", true),
        ("xtask/tests/python_upload.rs", true),
        ("xtask/tests/python_release.rs", true),
        ("xtask/tests/python_wheels.rs", true),
        ("xtask/tests/python_workflow.rs", true),
        ("xtask/tests/release_plz.rs", true),
        ("release-plz.toml", true),
        ("crates/delta-arrow-reader-cli/src/main.rs", false),
        ("crates/delta-arrow-reader-cli/tests/scan.py", false),
        ("benches/selective_read/src/main.rs", false),
        ("tests/reader/direct.rs", false),
        ("docs/content/contributing.md", false),
    ] {
        let output = Command::new("bash")
            .args([
                "-c",
                "for pattern; do [[ \"$MATCH_PATH\" == $pattern ]] && exit 0; done; exit 1",
                "--",
            ])
            .args(&patterns)
            .env("MATCH_PATH", path)
            .output()?;
        assert_eq!(output.status.success(), expected, "{path}");
    }
    Ok(())
}

fn check_ci_results(values: &[(&str, &str)]) -> Result<Output> {
    let script = workflow_script(
        include_str!("../../.github/workflows/ci.yml"),
        "Check results",
    );
    let mut rest = script.as_str();
    let mut rendered = String::new();
    while let Some((before, after)) = rest.split_once("${{ needs.") {
        let (key, after) = after.split_once(" }}").unwrap();
        rendered.push_str(before);
        rendered.push_str(
            values
                .iter()
                .find(|(name, _)| *name == key)
                .map_or("success", |(_, value)| *value),
        );
        rest = after;
    }
    rendered.push_str(rest);
    Ok(Command::new("bash")
        .args(["-e", "-c", &rendered])
        .output()?)
}

#[test]
fn summary_requires_python_only_when_requested() -> Result<()> {
    for code in ["true", "false"] {
        for python in ["true", "false"] {
            for package in ["success", "failure", "skipped", "cancelled"] {
                let values = [
                    ("changes.outputs.code", code),
                    ("changes.outputs.python", python),
                    ("changes.outputs.selective_read", "false"),
                    ("changes.outputs.release_pr", "false"),
                    ("python-package.result", package),
                ];
                let output = check_ci_results(&values)?;
                let expected = python != "true" || package == "success";
                assert_eq!(
                    output.status.success(),
                    expected,
                    "code={code}, python={python}, package={package}: {output:?}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn release_pr_requires_approved_and_validated_wheels() -> Result<()> {
    for code in ["true", "false"] {
        for release_pr in ["true", "false"] {
            for approval in ["success", "failure", "skipped", "cancelled"] {
                for wheels in ["success", "failure", "skipped", "cancelled"] {
                    for validated in ["true", "false", ""] {
                        let output = check_ci_results(&[
                            ("changes.outputs.code", code),
                            ("changes.outputs.python", "true"),
                            ("changes.outputs.selective_read", "false"),
                            ("changes.outputs.release_pr", release_pr),
                            ("release-approval.result", approval),
                            ("release-wheels.result", wheels),
                            ("release-wheels.outputs.validated", validated),
                            ("release-cli.outputs.validated", "true"),
                        ])?;
                        let expected = release_pr != "true"
                            || approval == "success" && wheels == "success" && validated == "true";
                        assert_eq!(
                            output.status.success(),
                            expected,
                            "code={code}, release_pr={release_pr}, approval={approval}, wheels={wheels}, validated={validated}: {output:?}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn release_wheel_builds_share_approval_with_cli() -> Result<()> {
    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    let jobs = &ci["jobs"];
    let approvals: Vec<_> = jobs
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, job)| !job["environment"].is_null())
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(approvals, ["release-approval"]);
    assert_eq!(jobs["release-approval"]["environment"], "release-checks");
    assert_eq!(jobs["release-approval"]["needs"], "changes");
    for name in ["release-approval", "release-wheels", "release-cli"] {
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
    let wheels = &jobs["release-wheels"];
    assert_eq!(wheels["uses"], "./.github/workflows/pypi-build.yml");
    for name in ["release-wheels", "release-cli"] {
        assert_eq!(jobs[name]["needs"], json!(["changes", "release-approval"]));
    }
    assert_eq!(
        wheels["with"],
        json!({
            "source-ref": "${{ github.sha }}",
            "expected-version": "${{ needs.changes.outputs.package_version }}",
        })
    );
    Ok(())
}

#[test]
fn wheel_cache_warming_requires_manual_dispatch() -> Result<()> {
    let warmer: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cache-warm.yml"))?;
    assert_eq!(
        warmer["jobs"]["wheels"]["if"],
        "github.event_name == 'workflow_dispatch'"
    );
    Ok(())
}

#[test]
fn benchmark_cache_warming_matches_pr_builds() -> Result<()> {
    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    let warmer: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cache-warm.yml"))?;
    let check = &ci["jobs"]["selective-read-fixtures"];
    let warm = &warmer["jobs"]["selective-read-fixtures"];

    assert_eq!(warm["runs-on"], check["runs-on"]);
    assert_eq!(warm["defaults"], check["defaults"]);
    for name in ["CARGO_TARGET_DIR", "RUSTFLAGS"] {
        assert_eq!(warm["env"][name], check["env"][name], "{name}");
    }
    let check_steps = check["steps"].as_array().unwrap();
    let warm_steps = warm["steps"].as_array().unwrap();
    for (action, inputs) in [
        ("dtolnay/rust-toolchain@stable", &["toolchain"][..]),
        (
            "Swatinem/rust-cache@v2",
            &["workspaces", "shared-key", "cache-targets"][..],
        ),
    ] {
        let check_step = check_steps
            .iter()
            .find(|step| step["uses"] == action)
            .unwrap();
        let warm_step = warm_steps
            .iter()
            .find(|step| step["uses"] == action)
            .unwrap();
        for input in inputs {
            assert_eq!(
                warm_step["with"][input], check_step["with"][input],
                "{action}: {input}"
            );
        }
    }
    let warm_builds = warm_steps
        .iter()
        .find(|step| step["name"] == "Warm benchmark caches")
        .unwrap()["run"]
        .as_str()
        .unwrap();
    for command in check_steps.iter().filter_map(|step| step["run"].as_str()) {
        if command.starts_with("cargo clippy ") || command.starts_with("cargo test ") {
            let expected = if command.starts_with("cargo test ") {
                format!("{command} --no-run")
            } else {
                command.to_owned()
            };
            assert!(
                warm_builds.lines().any(|line| line == expected),
                "{expected}"
            );
        }
    }
    assert_eq!(warm["env"]["SCCACHE_GHA_RW_MODE"], "READ_WRITE");
    assert_eq!(ci["env"]["SCCACHE_GHA_RW_MODE"], "READ_ONLY");
    assert_eq!(warm["needs"], "core");
    assert!(
        warmer["jobs"]["wheels"]["needs"]
            .as_array()
            .unwrap()
            .contains(&json!("selective-read-fixtures"))
    );
    Ok(())
}

#[test]
fn cache_warming_requires_the_triggering_main_commit() -> Result<()> {
    let script = workflow_script(
        include_str!("../../.github/workflows/pypi-build.yml"),
        "Check build inputs and read Rust version",
    );
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root())
        .output()?;
    assert!(output.status.success());
    let sha = String::from_utf8(output.stdout)?;
    let version = include_str!("../../Cargo.toml")
        .lines()
        .find_map(|line| {
            line.strip_prefix("version = \"")
                .and_then(|line| line.strip_suffix('"'))
        })
        .unwrap();
    let directory = test_dir("main-guard");
    for git_ref in [
        "refs/heads/main",
        "refs/heads/fix/example",
        "refs/pull/1/merge",
    ] {
        for event in [
            "push",
            "workflow_dispatch",
            "pull_request",
            "pull_request_target",
        ] {
            for same_commit in [true, false] {
                let output = Command::new("bash")
                    .args(["-e", "-c", &script])
                    .current_dir(root())
                    .env("CACHE_ONLY", "true")
                    .env("SOURCE_REF", sha.trim())
                    .env(
                        "GITHUB_SHA",
                        if same_commit {
                            sha.trim()
                        } else {
                            "0000000000000000000000000000000000000000"
                        },
                    )
                    .env("GITHUB_REF", git_ref)
                    .env("GITHUB_EVENT_NAME", event)
                    .env("EXPECTED_VERSION", version)
                    .env("GITHUB_OUTPUT", directory.join("output"))
                    .output()?;
                let expected = git_ref == "refs/heads/main"
                    && matches!(event, "push" | "workflow_dispatch")
                    && same_commit;
                assert_eq!(
                    output.status.success(),
                    expected,
                    "{git_ref}, {event}, same_commit={same_commit}: {output:?}"
                );
            }
        }
    }
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn linux_cache_check_isolates_build_outputs_without_masking_build_failures() -> Result<()> {
    let directory = test_dir("linux-exit");
    let cargo = directory.join("cargo");
    fs::write(
        &cargo,
        "#!/bin/sh\ntest -z \"$RUSTC_WRAPPER\" || exit 10\nprintf '%s\\n' \"$@\"\nexit \"$CACHE_CHECK_EXIT\"\n",
    )?;
    fs::set_permissions(&cargo, fs::Permissions::from_mode(0o755))?;
    let trap = include_str!("../../.github/workflows/pypi-build.yml")
        .lines()
        .find(|line| line.trim_start().starts_with("trap '"))
        .unwrap();
    for build_status in [0, 7] {
        for check_status in [0, 9] {
            let output = Command::new("/bin/bash")
                .args(["-euc", &format!("{trap}\nexit {build_status}")])
                .env("PATH", &directory)
                .env("GITHUB_WORKSPACE", root())
                .env("RUSTC_WRAPPER", "sccache")
                .env("CACHE_CHECK_EXIT", check_status.to_string())
                .output()?;
            assert_eq!(
                output.status.success(),
                build_status == 0 && check_status == 0,
                "build={build_status}, check={check_status}: {output:?}"
            );
            let stdout = String::from_utf8(output.stdout)?;
            let args: Vec<_> = stdout.lines().collect();
            let target = args
                .windows(2)
                .find(|args| args[0] == "--target-dir")
                .expect("container cache check must use a separate target directory")[1];
            let target = Path::new(target);
            assert!(target.is_absolute(), "container target must be absolute");
            assert!(
                !target.starts_with(root()),
                "container target must stay outside the mounted workspace"
            );
        }
    }
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn cache_retry_cleans_the_correct_target_and_restarts_native_sccache() -> Result<()> {
    let directory = test_dir("cache-retry");
    let log = directory.join("commands");
    for program in ["sudo", "sccache", "cargo", "sleep"] {
        let path = directory.join(program);
        fs::write(
            &path,
            "#!/bin/sh\nprogram=${0##*/}\n[ -z \"$RUSTC_WRAPPER\" ] || exit 10\nprintf '%s %s\\n' \"$program\" \"$*\" >> \"$COMMAND_LOG\"\n[ \"$program\" != \"$FAIL_COMMAND\" ]\n",
        )?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    }
    let wheel_script = workflow_script(
        include_str!("../../.github/workflows/pypi-build.yml"),
        "Prepare cache write retry",
    );
    let benchmark_script = workflow_script(
        include_str!("../../.github/workflows/cache-warm.yml"),
        "Prepare benchmark cache write retry",
    );
    for (script, runner, expected) in [
        (
            &wheel_script,
            "Linux",
            "sudo rm -rf .python-sdist-source/target\nsleep 60\n",
        ),
        (
            &wheel_script,
            "Windows",
            "sccache --stop-server\ncargo clean --target-dir target\nsleep 60\n",
        ),
        (
            &wheel_script,
            "macOS",
            "sccache --stop-server\ncargo clean --target-dir target\nsleep 60\n",
        ),
        (
            &benchmark_script,
            "Linux",
            "sccache --stop-server\ncargo clean --target-dir /tmp/benchmark-fixtures\nsleep 60\n",
        ),
    ] {
        for failure in ["", "sudo", "sccache", "cargo"] {
            fs::write(&log, "")?;
            let output = Command::new("/bin/bash")
                .args(["-euc", script])
                .env("PATH", &directory)
                .env("RUNNER_OS", runner)
                .env("RUSTC_WRAPPER", "")
                .env("CARGO_TARGET_DIR", "/tmp/benchmark-fixtures")
                .env("COMMAND_LOG", &log)
                .env("FAIL_COMMAND", failure)
                .output()?;
            let should_fail = !failure.is_empty() && expected.contains(failure);
            assert_eq!(
                output.status.success(),
                !should_fail,
                "{runner}: {output:?}"
            );
            let actual = fs::read_to_string(&log)?;
            if should_fail {
                assert!(!actual.contains("sleep"));
            } else {
                assert_eq!(actual, expected);
            }
        }
    }
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn core_cache_warming_builds_python_tests_only_on_linux_and_stops_on_failure() -> Result<()> {
    let directory = test_dir("core-warm");
    let cargo = directory.join("cargo");
    fs::write(
        &cargo,
        "#!/bin/sh\nprintf '%s\\n' \"$*\"\nexit \"$CARGO_EXIT\"\n",
    )?;
    fs::set_permissions(cargo, fs::Permissions::from_mode(0o755))?;
    let script = workflow_script(
        &include_str!("../../.github/workflows/cache-warm.yml")
            .replace("run: &warm-test-caches |", "run: |"),
        "Warm test caches",
    );
    for runner in ["Linux", "Windows", "macOS"] {
        for exit in ["0", "7"] {
            let output = Command::new("/bin/bash")
                .args(["-euc", &script])
                .env("PATH", &directory)
                .env("RUNNER_OS", runner)
                .env("CARGO_EXIT", exit)
                .output()?;
            assert_eq!(output.status.success(), exit == "0");
            let mut expected = "test --locked --all-features --no-run\n".to_owned();
            if exit == "0" {
                expected.push_str("test --locked --no-run\n");
                if runner == "Linux" {
                    expected.push_str("test --locked -p delta-arrow-reader-python --no-run\n");
                }
            }
            assert_eq!(String::from_utf8(output.stdout)?, expected);
        }
    }
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn prune_deletes_by_id_and_stops_on_command_or_schema_errors() -> Result<()> {
    let directory = test_dir("prune");
    let gh = directory.join("gh");
    fs::write(
        &gh,
        r#"#!/bin/sh
case "$*" in
  'api --paginate --slurp repos/owner/repo/actions/caches?key=v0-rust-core-ci-targets-Windows_NT-x64-&ref=refs/heads/main&per_page=100')
    [ "$FAIL_LIST" != true ] || exit 9
    printf '%s' "$CACHE_PAGES" ;;
  'api --method DELETE repos/owner/repo/actions/caches/'*)
    printf '%s\n' "$4" >> "$DELETE_LOG"
    [ "$FAIL_DELETE" != true ] || exit 7 ;;
  *) exit 8 ;;
esac
"#,
    )?;
    fs::set_permissions(&gh, fs::Permissions::from_mode(0o755))?;
    let cache = |id| {
        json!({"id": id, "key": format!("v0-rust-core-ci-targets-Windows_NT-x64-{id}"),
        "ref": "refs/heads/main", "last_accessed_at": format!("2026-10-{id:02}T00:00:00Z")})
    };
    let pages =
        json!([{"actions_caches": [cache(4), cache(3)]}, {"actions_caches": [cache(2), cache(1)]}]);
    let log = directory.join("deletions");
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command
        .arg("prune-ci-caches")
        .env("PATH", &directory)
        .env("GH_REPO", "owner/repo")
        .env("CACHE_PAGES", pages.to_string())
        .env("DELETE_LOG", &log)
        .env("FAIL_DELETE", "false")
        .env("FAIL_LIST", "false");
    assert!(command.output()?.status.success());
    assert_eq!(
        fs::read_to_string(&log)?,
        "repos/owner/repo/actions/caches/2\nrepos/owner/repo/actions/caches/1\n"
    );
    fs::write(&log, "")?;
    assert!(
        !command
            .env("FAIL_DELETE", "true")
            .output()?
            .status
            .success()
    );
    assert_eq!(
        fs::read_to_string(&log)?,
        "repos/owner/repo/actions/caches/2\n"
    );
    fs::write(&log, "")?;
    assert!(!command.env("FAIL_LIST", "true").output()?.status.success());
    assert!(fs::read_to_string(&log)?.is_empty());
    assert!(
        !command
            .env("FAIL_LIST", "false")
            .env(
                "CACHE_PAGES",
                r#"[{"actions_caches": [{"id": "wrong type"}]}]"#
            )
            .output()?
            .status
            .success()
    );
    assert!(fs::read_to_string(&log)?.is_empty());
    fs::remove_dir_all(directory)?;
    Ok(())
}
