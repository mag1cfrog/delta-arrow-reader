#![cfg(unix)]

use std::{collections::BTreeSet, fs, path::Path, process::Command};

use serde_json::{Value, json};

mod support;
use support::{PLATFORMS, Result, WheelFixture, wheel_files, write_wheel};

fn workflow() -> Value {
    serde_yaml_ng::from_str(include_str!("../../.github/workflows/pypi.yml")).unwrap()
}

fn git(directory: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args([
            "-c",
            "user.name=Wheel workflow test",
            "-c",
            "user.email=wheel-test@example.invalid",
        ])
        .args(args)
        .current_dir(directory)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()?;
    assert!(output.status.success(), "git {args:?}: {output:?}");
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
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
                "!cancelled() && github.repository == 'mag1cfrog/delta-arrow-reader' && {events} && \
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
        let mut expected_env = json!({
            "EXPECTED_VERSION": "${{ needs.prepare.outputs.expected-version }}",
        });
        if target == "pypi" {
            expected_env["ARTIFACT_ID"] = json!("${{ needs.prepare.outputs.artifact-id }}");
            expected_env["ARTIFACT_SHA256"] = json!("${{ needs.prepare.outputs.artifact-sha256 }}");
        }
        assert_eq!(upload["env"], expected_env);
        let steps = upload["steps"].as_array().unwrap();
        assert_eq!(
            steps[0]["with"],
            json!({
                "ref": "${{ needs.prepare.outputs.source-ref }}",
                "persist-credentials": false,
            })
        );
        let download = &steps[steps.len() - 3];
        if target == "testpypi" {
            assert_eq!(download["uses"], "actions/download-artifact@v6");
            assert_eq!(
                download["with"],
                json!({
                    "pattern": "delta-arrow-reader-*-wheel", "path": "wheel-artifacts",
                })
            );
        } else {
            assert_eq!(upload["permissions"]["actions"], "read");
            assert_eq!(download["env"]["GH_TOKEN"], "${{ github.token }}");
            let script = format!(
                "cargo() {{ printf '%s\\n' \"$@\"; }}\n{}",
                download["run"].as_str().unwrap()
            );
            let output = Command::new("bash")
                .args(["-e", "-o", "pipefail", "-c", &script])
                .env("EXPECTED_VERSION", "1.2.3")
                .env("ARTIFACT_ID", "901")
                .env("ARTIFACT_SHA256", "a".repeat(64))
                .output()?;
            assert!(output.status.success(), "{output:?}");
            assert_eq!(
                String::from_utf8(output.stdout)?
                    .lines()
                    .collect::<Vec<_>>(),
                [
                    "xtask",
                    "download-python-wheels",
                    "wheel-artifacts",
                    "--expected-version",
                    "1.2.3",
                    "--artifact-id",
                    "901",
                    "--sha256",
                    &"a".repeat(64),
                ]
            );
        }
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
            "PUBLISH_TO": "${{ needs.prepare.outputs.publish-to }}",
            "ARTIFACT_ID": "${{ needs.prepare.outputs.artifact-id }}",
            "ARTIFACT_SHA256": "${{ needs.prepare.outputs.artifact-sha256 }}",
        })
    );
    for prepare in ["success", "failure", "skipped", "cancelled"] {
        for build in ["success", "failure", "skipped", "cancelled"] {
            for validated in ["true", "false", ""] {
                for (target, artifact_id, sha256) in [
                    ("none", "", ""),
                    ("testpypi", "", ""),
                    ("pypi", "901", "digest"),
                    ("pypi", "", "digest"),
                    ("pypi", "901", ""),
                    ("unknown", "", ""),
                    ("", "", ""),
                ] {
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
                        .env("PUBLISH_TO", target)
                        .env("ARTIFACT_ID", artifact_id)
                        .env("ARTIFACT_SHA256", sha256)
                        .output()?;
                    let expected = prepare == "success"
                        && match target {
                            "pypi" => {
                                build == "skipped" && !artifact_id.is_empty() && !sha256.is_empty()
                            }
                            "none" | "testpypi" => build == "success" && validated == "true",
                            _ => false,
                        };
                    assert_eq!(
                        output.status.success(),
                        expected,
                        "{prepare}, {build}, {validated}, {target}, {artifact_id}, {sha256}: {output:?}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn pypi_preparation_reuses_pr_wheels_and_disables_rebuilding() {
    let workflow = workflow();
    assert_eq!(
        workflow["jobs"]["build"]["if"],
        "needs.prepare.outputs.publish-to != 'pypi'"
    );
    let prepare = &workflow["jobs"]["prepare"];
    assert_eq!(
        prepare["permissions"],
        json!({"contents": "read", "actions": "read", "pull-requests": "read"})
    );
    let steps = prepare["steps"].as_array().unwrap();
    assert_eq!(steps[steps.len() - 2]["id"], "request");
    let wheels = steps.last().unwrap();
    assert_eq!(wheels["id"], "wheels");
    assert_eq!(wheels["if"], "steps.request.outputs.publish-to == 'pypi'");
    assert!(wheels["continue-on-error"].is_null());
    assert_eq!(
        wheels["env"],
        json!({
            "GH_TOKEN": "${{ github.token }}", "EXPECTED_VERSION": "${{ steps.request.outputs.expected-version }}",
        })
    );
    assert_eq!(
        wheels["run"],
        "cargo xtask reuse-python-wheels wheel-artifacts --expected-version \"$EXPECTED_VERSION\""
    );
    for field in ["artifact-id", "artifact-sha256"] {
        assert_eq!(
            prepare["outputs"][field],
            format!("${{{{ steps.wheels.outputs.{field} }}}}")
        );
    }
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
fn linux_sdist_checks_preserve_the_workspace_and_reject_lockfile_drift() -> Result<()> {
    let builder: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/pypi-build.yml"))?;
    let steps = builder["jobs"]["build"]["steps"].as_array().unwrap();
    let script = |name| {
        steps.iter().find(|step| step["name"] == name).unwrap()["run"]
            .as_str()
            .unwrap()
    };
    let fixture = WheelFixture::new("sdist-lockfile")?;
    let directory = &fixture.directory;
    let original_lock = "version = 4\n\n[[package]]\nname = \"kept\"\nversion = \"0.1.0\"\n\n\
                         [[package]]\nname = \"tools\"\nversion = \"0.1.0\"\n";
    let workspace = "[workspace]\nmembers = [\"kept\", \"tools\"]\nresolver = \"3\"\n";
    for (path, contents) in [
        ("Cargo.toml", workspace),
        ("Cargo.lock", original_lock),
        (
            "kept/Cargo.toml",
            "[package]\nname = \"kept\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
        ),
        ("kept/src/lib.rs", ""),
        (
            "tools/Cargo.toml",
            "[package]\nname = \"tools\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
        ),
        ("tools/src/main.rs", "fn main() {}"),
    ] {
        let path = directory.join("source").join(path);
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(path, contents)?;
    }
    let runner_temp = directory.join("temp");
    fs::create_dir_all(runner_temp.join("python-sdist"))?;
    let output = Command::new("tar")
        .arg("-czf")
        .arg(runner_temp.join("python-sdist/source.tar.gz"))
        .arg("-C")
        .arg(directory)
        .arg("source")
        .output()?;
    assert!(output.status.success(), "{output:?}");
    // Exercise the workflow's comparisons against a local source archive.
    let mut prepare = Command::new("bash");
    prepare
        .args([
            "-e",
            "-o",
            "pipefail",
            "-c",
            &format!(
                "python() {{ :; }}\n{}",
                script("Prepare Linux source from sdist")
            ),
        ])
        .current_dir(directory)
        .env("RUNNER_TEMP", &runner_temp);
    fs::write(directory.join("Cargo.lock"), "different workspace lockfile")?;
    assert!(!prepare.output()?.status.success());
    fs::remove_dir_all(directory.join(".python-sdist-source"))?;
    fs::write(directory.join("Cargo.lock"), original_lock)?;
    fs::write(directory.join("Cargo.toml"), "different workspace members")?;
    assert!(!prepare.output()?.status.success());
    fs::remove_dir_all(directory.join(".python-sdist-source"))?;
    fs::write(directory.join("Cargo.toml"), workspace)?;
    let output = prepare.output()?;
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        fs::read_to_string(directory.join("Cargo.lock"))?,
        original_lock
    );
    let lockfile = directory.join(".python-sdist-source/Cargo.lock");
    assert_eq!(fs::read_to_string(&lockfile)?, original_lock);
    let metadata = Command::new("cargo")
        .args(["metadata", "--locked", "--offline", "--format-version", "1"])
        .current_dir(directory.join(".python-sdist-source"))
        .output()?;
    assert!(metadata.status.success(), "{metadata:?}");
    for changed in [false, true] {
        if changed {
            fs::write(&lockfile, "changed lockfile")?;
        }
        let output = Command::new("bash")
            .args([
                "-e",
                "-o",
                "pipefail",
                "-c",
                &format!(
                    "python() {{ :; }}\nauditwheel() {{ printf 'inspected\\n'; }}\n{}",
                    script("Inspect Linux native dependencies")
                ),
            ])
            .current_dir(directory)
            .env("RUNNER_TEMP", &runner_temp)
            .output()?;
        assert_eq!(output.status.success(), !changed, "{output:?}");
        assert_eq!(
            String::from_utf8(output.stdout)?.contains("inspected"),
            !changed
        );
    }
    Ok(())
}

#[test]
fn every_wheel_cli_is_audited_after_installation_and_before_upload() -> Result<()> {
    let builder: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/pypi-build.yml"))?;
    let build = &builder["jobs"]["build"];
    let steps = build["steps"].as_array().unwrap();
    let position = |name| steps.iter().position(|step| step["name"] == name).unwrap();
    let audit_position = position("Audit installed wheel CLI");
    assert!(position("Inspect wheel metadata") < audit_position);
    assert!(position("Test installed wheel on Python 3.10 and PyArrow 18.0.0") < audit_position);
    assert!(audit_position < position("Upload validated wheel"));
    let audit = &steps[audit_position];
    assert!(audit["if"].is_null());
    assert!(audit["continue-on-error"].is_null());
    let fixture = WheelFixture::new("installed-cli-audit")?;
    let runner_temp = fixture.directory.join("runner temp");
    for platform in build["strategy"]["matrix"]["include"].as_array().unwrap() {
        let windows = platform["os"] == "windows-2022";
        let cli = if windows {
            "Scripts/delta-arrow-reader.exe"
        } else {
            "bin/delta-arrow-reader"
        };
        let script = audit["run"].as_str().unwrap()
            .replace("${{ runner.os == 'Windows' && 'Scripts/delta-arrow-reader.exe' || 'bin/delta-arrow-reader' }}", cli)
            .replace("${{ matrix.platform-tag }}", platform["platform-tag"].as_str().unwrap());
        for exit_status in [0, 9] {
            let script =
                format!("cargo() {{ printf '%s\\n' \"$@\"; return {exit_status}; }}\n{script}");
            let output = Command::new("bash")
                .args(["-e", "-o", "pipefail", "-c", &script])
                .env("RUNNER_TEMP", &runner_temp)
                .output()?;
            assert_eq!(output.status.success(), exit_status == 0, "{output:?}");
            assert_eq!(
                String::from_utf8(output.stdout)?
                    .lines()
                    .collect::<Vec<_>>(),
                [
                    "xtask",
                    "audit-python-cli",
                    runner_temp.join("python310").join(cli).to_str().unwrap(),
                    "--platform-tag",
                    platform["platform-tag"].as_str().unwrap(),
                ]
            );
        }
    }
    Ok(())
}

#[test]
fn linux_runtime_acceptance_uses_the_baseline_and_propagates_failures() -> Result<()> {
    let builder: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/pypi-build.yml"))?;
    let build = &builder["jobs"]["build"];
    let steps = build["steps"].as_array().unwrap();
    let position = |name| steps.iter().position(|step| step["name"] == name).unwrap();
    let runtime_position = position("Test installed wheel on glibc 2.28");
    assert!(position("Test installed wheel on Python 3.14 and current PyArrow") < runtime_position);
    assert!(runtime_position < position("Upload validated wheel"));
    let runtime = &steps[runtime_position];
    assert_eq!(runtime["if"], "runner.os == 'Linux'");
    assert!(runtime["continue-on-error"].is_null());
    assert_eq!(
        steps[position("Build wheel")]["with"]["container"],
        "${{ runner.os == 'Linux' && env.MANYLINUX_IMAGE || '' }}"
    );
    let image = build["env"]["MANYLINUX_IMAGE"].as_str().unwrap();
    assert!(image.starts_with("quay.io/pypa/manylinux_2_28_x86_64@sha256:"));
    let fixture = WheelFixture::new("glibc-runtime")?;
    let workspace = fixture.directory.join("source checkout");
    let script = runtime["run"].as_str().unwrap();
    let mut container_script = String::new();
    for status in [0, 9] {
        let output = Command::new("bash")
            .args([
                "-e",
                "-o",
                "pipefail",
                "-c",
                &format!("docker() {{ printf '%s\\0' \"$@\"; return {status}; }}\n{script}"),
            ])
            .env("GITHUB_WORKSPACE", &workspace)
            .env("MANYLINUX_IMAGE", image)
            .output()?;
        assert_eq!(output.status.code(), Some(status), "{output:?}");
        let stdout = String::from_utf8(output.stdout)?;
        let arguments: Vec<_> = stdout.trim_end_matches('\0').split('\0').collect();
        assert_eq!(
            &arguments[..9],
            [
                "run",
                "--rm",
                "--workdir",
                "/tmp",
                "--env",
                "PATH=/usr/bin:/bin",
                "--env",
                "LD_LIBRARY_PATH=",
                "--volume",
            ]
        );
        assert_eq!(
            arguments[9],
            format!("{}/dist:/wheels:ro", workspace.display())
        );
        for (index, path) in [
            (10, "crates/delta-arrow-reader-python/tests"),
            (
                12,
                "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned",
            ),
        ] {
            assert_eq!(arguments[index], "--volume");
            assert_eq!(
                arguments[index + 1],
                format!("{}/{path}:/acceptance/{path}:ro", workspace.display())
            );
        }
        assert_eq!(
            &arguments[14..20],
            ["--entrypoint", "/bin/bash", image, "-euo", "pipefail", "-c"]
        );
        assert_eq!(arguments.len(), 21);
        container_script = arguments[20].to_owned();
    }
    // Keep the actual shell control flow while replacing the container's interpreters.
    let script = container_script
        .replace(
            "/opt/python/cp$version-cp$version/bin/python",
            "base_python",
        )
        .replace("/tmp/python$version/bin/python", "installed_python");
    for (glibc, failure) in [
        ("2.28", ""),
        ("2.39", ""),
        ("2.28", "pip"),
        ("2.28", "unittest"),
    ] {
        let output = Command::new("bash")
            .args([
                "-euo",
                "pipefail",
                "-c",
                &format!(
                    r#"
                    getconf() {{ printf 'glibc %s\n' "$GLIBC"; }}
                    base_python() {{ printf 'create %s\n' "$version"; }}
                    installed_python() {{
                        printf '%s %s\n' "$version" "$*"
                        if [ -n "$FAILURE" ] && [[ "$*" == *"$FAILURE"* ]]; then return 7; fi
                    }}
                    {script}
                "#
                ),
            ])
            .env("GLIBC", glibc)
            .env("FAILURE", failure)
            .output()?;
        assert_eq!(
            output.status.success(),
            glibc == "2.28" && failure.is_empty(),
            "{output:?}"
        );
        let stdout = String::from_utf8(output.stdout)?;
        if output.status.success() {
            assert_eq!(
                stdout.lines().collect::<Vec<_>>(),
                [
                    "create 310",
                    "310 -m pip install --only-binary=:all: /wheels/*.whl pyarrow==18.0.0",
                    "310 -I -m unittest discover -s /acceptance/crates/delta-arrow-reader-python/tests -k installed",
                    "create 314",
                    "314 -m pip install --only-binary=:all: /wheels/*.whl",
                    "314 -I -m unittest discover -s /acceptance/crates/delta-arrow-reader-python/tests -k installed",
                ]
            );
        } else if glibc != "2.28" {
            assert!(stdout.is_empty(), "{stdout}");
        } else {
            assert!(!stdout.contains("314"), "{stdout}");
        }
    }
    Ok(())
}

#[test]
fn tool_acceptance_checks_both_environments_before_upload() -> Result<()> {
    let builder: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/pypi-build.yml"))?;
    let build = &builder["jobs"]["build"];
    let steps = build["steps"].as_array().unwrap();
    let position = |name| steps.iter().position(|step| step["name"] == name).unwrap();
    let tool_position = position("Test wheel with uv tool and uvx");
    assert!(position("Test installed wheel on Python 3.14 and current PyArrow") < tool_position);
    assert!(tool_position < position("Upload validated wheel"));
    let tool = &steps[tool_position];
    assert!(tool["if"].is_null());
    assert!(tool["continue-on-error"].is_null());
    assert_eq!(tool["working-directory"], "${{ runner.temp }}");
    assert_eq!(
        tool["env"],
        json!({
            "UV_PYTHON": "${{ steps.python314.outputs.python-path }}",
            "UV_PYTHON_DOWNLOADS": "never",
            "UV_NO_BUILD": "true",
        })
    );
    let setup = steps.iter().find(|step| step["id"] == "python314").unwrap();
    assert_eq!(setup["uses"], "actions/setup-python@v6");
    assert_eq!(setup["with"]["python-version"], "3.14");
    let fixture = WheelFixture::new("tool-installation")?;
    let workspace = fixture.directory.join("source checkout");
    let runner_temp = fixture.directory.join("runner temp");
    fs::create_dir_all(workspace.join("dist"))?;
    fs::create_dir(&runner_temp)?;
    let wheel = workspace.join("dist/test wheel.whl");
    fs::write(&wheel, "fixture")?;
    let calls = fixture.directory.join("calls");
    let mocks = r#"
        record() {
            printf '%s|' "$@" >> "$CALLS"
            printf '\n' >> "$CALLS"
            [ "$FAIL_STAGE" != "$1" ] || return 9
        }
        python() {
            case "$*" in
                '-m pip install '*) record setup "$@" ;;
                '-m uv tool install '*)
                    record install "$@"
                    test "$UV_TOOL_DIR" = "$tools/environments"
                    test "$UV_TOOL_BIN_DIR" = "$tools/bin"
                    test "$UV_CACHE_DIR" = "$tools/cache" ;;
                *'python -I -m unittest '*) record uvx-tests "$@" ;;
                *'delta-arrow-reader --version')
                    record uvx-version "$@" || return 9
                    printf 'delta-arrow-reader %s\n' "$UVX_VERSION" ;;
                *) return 99 ;;
            esac
        }
        tool_python() { record tool-tests "$@"; }
        tool_cli() {
            record tool-version "$@" || return 9
            printf 'delta-arrow-reader %s\n' "$TOOL_VERSION"
        }
    "#;
    for platform in build["strategy"]["matrix"]["include"].as_array().unwrap() {
        let python = platform["venv-python"].as_str().unwrap();
        let suffix = if platform["os"] == "windows-2022" {
            ".exe"
        } else {
            ""
        };
        let script = tool["run"]
            .as_str()
            .unwrap()
            .replace("${{ matrix.venv-python }}", python)
            .replace("${{ runner.os == 'Windows' && '.exe' || '' }}", suffix)
            .replace(
                &format!("\"$UV_TOOL_DIR/delta-arrow-reader/{python}\""),
                "tool_python",
            )
            .replace(
                &format!("\"$UV_TOOL_BIN_DIR/delta-arrow-reader{suffix}\""),
                "tool_cli",
            );
        for failure in [
            "",
            "setup",
            "install",
            "tool-tests",
            "tool-version",
            "uvx-tests",
            "uvx-version",
            "wrong-tool-version",
            "wrong-uvx-version",
        ] {
            fs::write(&calls, "")?;
            let output = Command::new("bash")
                .args(["-e", "-o", "pipefail", "-c", &format!("{mocks}\n{script}")])
                .current_dir(&runner_temp)
                .env("GITHUB_WORKSPACE", &workspace)
                .env("RUNNER_TEMP", &runner_temp)
                .env("EXPECTED_VERSION", "1.2.3")
                .env("CALLS", &calls)
                .env("FAIL_STAGE", failure)
                .env(
                    "TOOL_VERSION",
                    if failure == "wrong-tool-version" {
                        "stale"
                    } else {
                        "1.2.3"
                    },
                )
                .env(
                    "UVX_VERSION",
                    if failure == "wrong-uvx-version" {
                        "stale"
                    } else {
                        "1.2.3"
                    },
                )
                .output()?;
            assert_eq!(
                output.status.success(),
                failure.is_empty(),
                "{platform}, {failure}: {output:?}"
            );
            if failure.is_empty() {
                let calls = fs::read_to_string(&calls)?;
                let wheel = wheel.display();
                let tests = workspace.join("crates/delta-arrow-reader-python/tests");
                let tests = tests.display();
                assert_eq!(
                    calls.lines().collect::<Vec<_>>(),
                    [
                        "setup|-m|pip|install|--only-binary=:all:|uv==0.10.0|".to_owned(),
                        format!("install|-m|uv|tool|install|--no-config|{wheel}|"),
                        format!("tool-tests|-I|-m|unittest|discover|-s|{tests}|-k|installed|"),
                        "tool-version|--version|".to_owned(),
                        format!(
                            "uvx-tests|-m|uv|tool|run|--no-config|--isolated|--from|{wheel}|python|-I|-m|unittest|discover|-s|{tests}|-k|installed|"
                        ),
                        format!(
                            "uvx-version|-m|uv|tool|run|--no-config|--isolated|--from|{wheel}|delta-arrow-reader|--version|"
                        ),
                    ]
                );
            }
        }
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
        "${{ steps.complete.outputs.validated }}"
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
    let complete = steps.iter().find(|step| step["id"] == "complete").unwrap();
    let script = format!(
        "{}\n{}\n{}",
        steps[0]["run"].as_str().unwrap(),
        verify["run"].as_str().unwrap(),
        complete["run"].as_str().unwrap()
    )
    .replace("cargo xtask", "\"$XTASK_BINARY\"");
    let fixture = WheelFixture::new("workflow-validation")?;
    git(&fixture.directory, &["init", "--initial-branch=main"])?;
    fs::write(fixture.directory.join("source.txt"), "release source\n")?;
    git(&fixture.directory, &["add", "source.txt"])?;
    git(
        &fixture.directory,
        &["commit", "-m", "test: release source"],
    )?;
    let source_tree = git(&fixture.directory, &["rev-parse", "HEAD^{tree}"])?;
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
                    format!("existing=kept\nsource-tree={source_tree}\nvalidated=true\n")
                } else {
                    "existing=kept\n".to_owned()
                }
            );
        }
    }
    Ok(())
}

#[test]
fn wheels_without_a_cli_cannot_complete_release_validation() -> Result<()> {
    let builder: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/pypi-build.yml"))?;
    let steps = builder["jobs"]["validate"]["steps"].as_array().unwrap();
    let verify = steps.iter().find(|step| step["id"] == "verify").unwrap();
    let complete = steps.iter().find(|step| step["id"] == "complete").unwrap();
    let script = format!(
        "{}\n{}\n{}",
        steps[0]["run"].as_str().unwrap(),
        verify["run"].as_str().unwrap(),
        complete["run"].as_str().unwrap()
    )
    .replace("cargo xtask", "\"$XTASK_BINARY\"");
    for (index, (platform, tag)) in PLATFORMS.into_iter().enumerate() {
        let fixture = WheelFixture::new(&format!("validation-missing-cli-{platform}"))?;
        let (_, wheels) = fixture.create_artifacts()?;
        let mut files = wheel_files(tag);
        files.retain(|(name, _)| !name.contains(".data/scripts/"));
        write_wheel(&wheels[index], &files, true)?;
        let output_file = fixture.directory.join("outputs");
        fs::write(&output_file, "existing=kept\n")?;
        let output = Command::new("bash")
            .args(["-e", "-o", "pipefail", "-c", &script])
            .current_dir(&fixture.directory)
            .env("XTASK_BINARY", env!("CARGO_BIN_EXE_xtask"))
            .env("EXPECTED_VERSION", "1.2.3")
            .env("BUILD_RESULT", "success")
            .env("GITHUB_OUTPUT", &output_file)
            .output()?;
        assert!(!output.status.success(), "{platform}: {output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(
                "missing required wheel file: delta_arrow_reader-1.2.3.data/scripts/delta-arrow-reader"
            ),
            "{output:?}"
        );
        assert_eq!(fs::read_to_string(output_file)?, "existing=kept\n");
    }
    Ok(())
}

#[test]
fn release_pr_wheels_are_saved_before_reporting_success() -> Result<()> {
    let builder: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/pypi-build.yml"))?;
    let platform_upload = builder["jobs"]["build"]["steps"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(
        platform_upload["with"]["overwrite"],
        "${{ github.event_name == 'pull_request' }}"
    );
    let steps = builder["jobs"]["validate"]["steps"].as_array().unwrap();
    let tail = &steps[steps.len() - 3..];
    assert_eq!(tail[0]["id"], "verify");
    assert_eq!(tail[1]["uses"], "actions/upload-artifact@v6");
    assert_eq!(tail[1]["if"], "github.event_name == 'pull_request'");
    assert!(tail[1]["continue-on-error"].is_null());
    assert_eq!(
        tail[1]["with"],
        json!({
            "name": "python-wheels-${{ steps.verify.outputs.source-tree }}-${{ github.run_id }}-${{ github.run_attempt }}",
            "path": "wheel-artifacts/",
            "if-no-files-found": "error",
            "compression-level": 0,
            "retention-days": 90,
        })
    );
    assert_eq!(tail[2]["id"], "complete");
    assert!(tail[2]["if"].is_null());
    Ok(())
}
