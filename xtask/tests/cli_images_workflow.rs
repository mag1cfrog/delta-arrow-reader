#![cfg(unix)]

use std::{collections::BTreeMap, error::Error, fs, process::Command};

use serde_json::{Value, json};

type TestResult = Result<(), Box<dyn Error>>;

fn images_workflow() -> Value {
    serde_yaml_ng::from_str(include_str!("../../.github/workflows/cli-images.yml")).unwrap()
}

fn ci_workflow() -> Value {
    serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml")).unwrap()
}

fn steps(job: &Value) -> &[Value] {
    job["steps"].as_array().unwrap()
}

fn named_step<'a>(job: &'a Value, name: &str) -> &'a Value {
    steps(job).iter().find(|step| step["name"] == name).unwrap()
}

#[test]
fn image_workflow_requires_explicit_identity_and_read_only_artifact_access() {
    let workflow = images_workflow();
    assert_eq!(
        workflow["on"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["workflow_call", "workflow_dispatch"]
    );
    assert_eq!(
        workflow["permissions"],
        json!({"contents": "read", "actions": "read"})
    );
    for event in ["workflow_call", "workflow_dispatch"] {
        for input in [
            "binary-artifact-id",
            "source-ref",
            "expected-version",
            "rust-toolchain",
        ] {
            assert_eq!(workflow["on"][event]["inputs"][input]["required"], true);
            assert_eq!(workflow["on"][event]["inputs"][input]["type"], "string");
            assert!(workflow["on"][event]["inputs"][input]["default"].is_null());
        }
    }
    assert_eq!(
        workflow["on"]["workflow_call"]["inputs"]["binary-run-id"]["default"],
        ""
    );
    assert_eq!(
        workflow["on"]["workflow_dispatch"]["inputs"]["binary-run-id"]["required"],
        true
    );
    for job in workflow["jobs"].as_object().unwrap().values() {
        assert!(job["environment"].is_null());
        assert!(job["permissions"].is_null());
        assert!(job["continue-on-error"].is_null());
        for step in steps(job) {
            assert!(step["continue-on-error"].is_null());
            assert!(step["if"].is_null());
        }
    }
    let text = workflow.to_string();
    for forbidden in [
        "packages",
        "id-token",
        "secrets.",
        "docker push",
        "skopeo copy --dest",
        "cli-build.yml",
        "cli build-archive",
        "cargo build",
        "delta-arrow-reader-cli --",
    ] {
        assert!(
            !text.contains(forbidden),
            "unexpected image workflow capability: {forbidden}"
        );
    }
}

#[test]
fn release_images_reuse_the_binary_approval_identity_and_immutable_artifact() {
    let ci = ci_workflow();
    let jobs = &ci["jobs"];
    let images = &jobs["release-cli-images"];
    assert_eq!(
        images["needs"],
        json!(["changes", "release-approval", "release-cli"])
    );
    assert_eq!(
        images["if"]
            .as_str()
            .unwrap()
            .split_whitespace()
            .collect::<String>(),
        "needs.changes.outputs.release_pr=='true'&&needs.release-approval.result=='success'&&needs.release-cli.result=='success'&&needs.release-cli.outputs.validated=='true'"
    );
    assert_eq!(images["uses"], "./.github/workflows/cli-images.yml");
    assert_eq!(images["permissions"], images_workflow()["permissions"]);
    assert_eq!(
        images["with"],
        json!({
            "binary-artifact-id": "${{ needs.release-cli.outputs.artifact-id }}",
            "source-ref": "${{ needs.release-cli.outputs.source-commit }}",
            "expected-version": "${{ needs.release-cli.outputs.version }}",
            "rust-toolchain": "${{ needs.release-cli.outputs.rust-toolchain }}",
        })
    );
    assert!(
        jobs["ci"]["needs"]
            .as_array()
            .unwrap()
            .contains(&json!("release-cli-images"))
    );
    assert_eq!(jobs["ci"]["if"], "always()");
    assert_eq!(ci["concurrency"]["cancel-in-progress"], true);
    let approvals: Vec<_> = jobs
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, job)| !job["environment"].is_null())
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(approvals, ["release-approval"]);
    for (name, job) in jobs.as_object().unwrap() {
        if job["uses"] == "./.github/workflows/cli-images.yml" {
            assert_eq!(name, "release-cli-images");
        }
    }
}

#[test]
fn artifact_downloads_never_fall_back_to_a_name_ref_or_partial_binary_set() {
    let workflow = images_workflow();
    let jobs = &workflow["jobs"];
    for name in ["prepare", "images", "validate"] {
        let downloads: Vec<_> = steps(&jobs[name])
            .iter()
            .filter(|step| {
                step["uses"] == "actions/download-artifact@v6"
                    && step["with"]["path"] == "${{ runner.temp }}/cli-binaries"
            })
            .collect();
        assert_eq!(downloads.len(), 1);
        assert_eq!(
            downloads[0]["with"],
            json!({
                "artifact-ids": "${{ inputs.binary-artifact-id }}",
                "run-id": "${{ inputs.binary-run-id || github.run_id }}",
                "github-token": "${{ github.token }}",
                "path": "${{ runner.temp }}/cli-binaries",
            })
        );
        assert!(downloads[0]["if"].is_null());
    }
    let prepare = named_step(
        &jobs["prepare"],
        "Validate source, toolchain, and complete binary set",
    )["run"]
        .as_str()
        .unwrap();
    for required in [
        "cli prepare --source-commit \"$CLI_SOURCE_COMMIT\"",
        "--expected-version \"$CLI_VERSION\"",
        "cli artifacts verify",
        ".\"rust-toolchain\" == $rust",
        "--rust-toolchain \"$CLI_RUST_TOOLCHAIN\"",
    ] {
        assert!(
            prepare.contains(required),
            "missing preparation check: {required}"
        );
    }
    assert!(
        !prepare.contains("--architecture"),
        "input verification must require both platforms"
    );
    for job in jobs.as_object().unwrap().values() {
        let checkout = steps(job)
            .iter()
            .find(|step| step["uses"] == "actions/checkout@v5")
            .unwrap();
        assert_eq!(checkout["with"]["persist-credentials"], false);
        assert!(
            [
                "${{ inputs.source-ref }}",
                "${{ needs.prepare.outputs.source-commit }}"
            ]
            .contains(&checkout["with"]["ref"].as_str().unwrap())
        );
    }
}

#[test]
fn image_checks_use_both_native_runners_before_export_and_upload() {
    let workflow = images_workflow();
    let images = &workflow["jobs"]["images"];
    assert_eq!(images["needs"], "prepare");
    assert_eq!(images["runs-on"], "${{ matrix.runner }}");
    assert_eq!(images["strategy"]["fail-fast"], false);
    assert!(images["strategy"]["max-parallel"].is_null());
    assert_eq!(
        images["strategy"]["matrix"]["include"],
        json!([
            {"architecture":"amd64", "runner":"ubuntu-24.04"},
            {"architecture":"arm64", "runner":"ubuntu-24.04-arm"},
        ])
    );
    let names: Vec<_> = steps(images)
        .iter()
        .filter_map(|step| step["name"].as_str())
        .collect();
    assert_eq!(
        names,
        [
            "Install OCI verification tools",
            "Build and test the native CLI image",
            "Build and test the Go consumer",
            "Export and reimport the tested image",
            "Upload tested platform image"
        ]
    );
    let native = named_step(images, names[1])["run"].as_str().unwrap();
    assert!(native.contains("amd64:x86_64|arm64:aarch64)"));
    assert!(native.contains("cli test-image \"$cli_image_id\""));
    let go = named_step(images, names[2])["run"].as_str().unwrap();
    for required in [
        "docker tag \"$CLI_IMAGE_ID\"",
        "DAR_IMAGE=$CLI_IMAGE",
        "examples/go-cli",
        "--read-only --network=none",
        "-dar /usr/local/bin/dar",
        ". == {\"rows\":360,\"id_sum\":64980}",
        "cmp \"$RUNNER_TEMP/cli-files.sha256\"",
    ] {
        assert!(go.contains(required), "missing Go check: {required}");
    }
    assert!(
        include_str!("../../examples/go-cli/Dockerfile")
            .contains("RUN go test -count=1 -timeout 60s ./...")
    );
    let export = named_step(images, names[3])["run"].as_str().unwrap();
    assert!(export.contains("cli images export"));
    assert!(export.contains("--image \"$CLI_IMAGE_ID\""));
    assert!(export.contains("--binary-artifacts \"$RUNNER_TEMP/cli-binaries\""));
}

#[test]
fn invalid_artifact_references_and_identities_fail_before_checkout() -> TestResult {
    let workflow = images_workflow();
    let first = &steps(&workflow["jobs"]["prepare"])[0];
    let script = first["run"].as_str().unwrap();
    let inputs = [
        ("CLI_BINARY_ARTIFACT_ID", "123"),
        ("CLI_BINARY_RUN_ID", "456"),
        (
            "CLI_SOURCE_COMMIT",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        ("CLI_VERSION", "1.2.3"),
        ("CLI_RUST_TOOLCHAIN", "1.94.1"),
    ];
    for (name, valid) in inputs {
        for value in [valid, "", "0", "1,2", "main", "123junk", "1\n2", "latest"] {
            let output = Command::new("bash")
                .args(["-euo", "pipefail", "-c", script])
                .envs(inputs)
                .env(name, value)
                .output()?;
            assert_eq!(
                output.status.success(),
                value == valid,
                "{name}={value:?}: {output:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn image_handoff_requires_all_jobs_and_upload_outputs() -> TestResult {
    let workflow = images_workflow();
    let validate = &workflow["jobs"]["validate"];
    assert_eq!(validate["needs"], json!(["prepare", "images"]));
    assert_eq!(validate["if"], "always()");
    let steps = steps(validate);
    let upload = &steps[steps.len() - 2];
    let complete = &steps[steps.len() - 1];
    assert_eq!(upload["uses"], "actions/upload-artifact@v6");
    assert_eq!(upload["with"]["if-no-files-found"], "error");
    assert_eq!(upload["with"]["path"], "${{ runner.temp }}/cli-images/*");
    assert!(upload["with"]["overwrite"].is_null());
    let binary: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cli-build.yml"))?;
    let binary_upload = named_step(&binary["jobs"]["validate"], "Upload complete artifact set");
    assert_eq!(
        upload["with"]["retention-days"],
        binary_upload["with"]["retention-days"]
    );
    assert_eq!(
        validate["env"]["CLI_ARTIFACT_NAME"],
        "dar-cli-images-${{ needs.prepare.outputs.source-commit }}-${{ github.run_id }}-${{ github.run_attempt }}"
    );
    let script = format!(
        "{}\n{}",
        steps[0]["run"].as_str().unwrap(),
        complete["run"].as_str().unwrap()
    );
    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("cli-images"))?;
    fs::write(
        root.path().join("cli-images/cli-images.json"),
        "image manifest fixture",
    )?;
    let handoff = root.path().join("output");
    for prepare in ["success", "failure", "skipped", "cancelled", ""] {
        for images in ["success", "failure", "skipped", "cancelled", ""] {
            for (id, url) in [
                ("123", "https://example.invalid/artifact"),
                ("", "url"),
                ("invalid", "url"),
                ("123", ""),
            ] {
                fs::write(&handoff, "")?;
                let output = Command::new("bash")
                    .args(["-euo", "pipefail", "-c", &script])
                    .env("PREPARE_RESULT", prepare)
                    .env("IMAGES_RESULT", images)
                    .env("ARTIFACT_ID", id)
                    .env("ARTIFACT_URL", url)
                    .env("CLI_ARTIFACT_NAME", "image-fixture")
                    .env("CLI_SOURCE_COMMIT", "a".repeat(40))
                    .env("CLI_VERSION", "1.2.3")
                    .env("CLI_RUST_TOOLCHAIN", "1.94.1")
                    .env("GITHUB_OUTPUT", &handoff)
                    .env("RUNNER_TEMP", root.path())
                    .env("GITHUB_STEP_SUMMARY", root.path().join("summary"))
                    .output()?;
                let expected =
                    prepare == "success" && images == "success" && id == "123" && !url.is_empty();
                assert_eq!(
                    output.status.success(),
                    expected,
                    "{prepare}/{images}/{id}/{url}: {output:?}"
                );
                assert_eq!(
                    fs::read_to_string(&handoff)?.contains("validated=true"),
                    expected
                );
            }
        }
    }
    for field in [
        "validated",
        "source-commit",
        "version",
        "rust-toolchain",
        "artifact-id",
        "artifact-name",
        "artifact-url",
    ] {
        let output = &workflow["on"]["workflow_call"]["outputs"][field]["value"];
        let job = if ["source-commit", "version", "rust-toolchain"].contains(&field) {
            "prepare"
        } else {
            "validate"
        };
        assert_eq!(output, &format!("${{{{ jobs.{job}.outputs.{field} }}}}"));
    }
    Ok(())
}

#[test]
fn required_ci_rejects_missing_or_failed_image_handoffs_only_on_release_prs() -> TestResult {
    let ci = ci_workflow();
    let script = named_step(&ci["jobs"]["ci"], "Check results")["run"]
        .as_str()
        .unwrap();
    for code in ["true", "false"] {
        for release in ["true", "false"] {
            let valid = BTreeMap::from([
                ("changes.outputs.code", code),
                ("changes.outputs.python", "false"),
                ("changes.outputs.cli", "false"),
                ("changes.outputs.tooling", "false"),
                ("changes.outputs.selective_read", "false"),
                ("changes.outputs.release_pr", release),
                ("changes.outputs.pr_head_sha", "pr-head"),
                ("release-cli.outputs.validated", "true"),
                ("release-wheels.outputs.validated", "true"),
                (
                    "release-cli.outputs.source-commit",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                ("release-cli.outputs.version", "1.2.3"),
                ("release-cli-images.result", "success"),
                ("release-cli-images.outputs.validated", "true"),
                ("release-cli-images.outputs.artifact-id", "123"),
                ("release-cli-images.outputs.artifact-name", "image-fixture"),
                (
                    "release-cli-images.outputs.artifact-url",
                    "https://example.invalid/artifact",
                ),
                (
                    "release-cli-images.outputs.source-commit",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                ("release-cli-images.outputs.version", "1.2.3"),
                ("release-cli-record.outputs.validated", "true"),
                ("release-cli-record.outputs.artifact-id", "124"),
                ("release-cli-record.outputs.artifact-name", "record-fixture"),
                (
                    "release-cli-record.outputs.artifact-url",
                    "https://example.invalid/record",
                ),
                (
                    "release-cli-record.outputs.source-commit",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                ("release-cli-record.outputs.pr-head-sha", "pr-head"),
                ("release-cli-record.outputs.version", "1.2.3"),
            ]);
            let mut cases = vec![("release-cli-images.result", "success")];
            for result in ["failure", "skipped", "cancelled", ""] {
                cases.push(("release-cli-images.result", result));
            }
            cases.push(("release-cli-images.outputs.validated", "false"));
            for key in valid
                .keys()
                .filter(|key| key.starts_with("release-cli-images.outputs."))
            {
                cases.push((key, ""));
            }
            cases.push((
                "release-cli-images.outputs.source-commit",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ));
            cases.push(("release-cli-images.outputs.version", "1.2.4"));
            for (key, value) in cases {
                let mut values = valid.clone();
                values.insert(key, value);
                let mut rest = script;
                let mut rendered = String::new();
                while let Some((before, after)) = rest.split_once("${{ needs.") {
                    let (name, after) = after.split_once(" }}").unwrap();
                    rendered.push_str(before);
                    rendered.push_str(values.get(name).copied().unwrap_or_else(|| {
                        assert!(name.ends_with(".result"), "missing test value: {name}");
                        "success"
                    }));
                    rest = after;
                }
                rendered.push_str(rest);
                let output = Command::new("bash")
                    .args(["-euo", "pipefail", "-c", &rendered])
                    .output()?;
                let expected =
                    release != "true" || (key == "release-cli-images.result" && value == "success");
                assert_eq!(
                    output.status.success(),
                    expected,
                    "code={code}, release={release}, {key}={value:?}: {output:?}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn ordinary_ci_only_adds_small_image_workflow_checks() {
    let filters: Value =
        serde_json::from_str(include_str!("../../.github/path-filters.json")).unwrap();
    for path in [
        ".github/workflows/cli-images.yml",
        "crates/delta-arrow-reader-cli/Dockerfile",
        "examples/go-cli/**",
    ] {
        assert!(
            filters["tooling"]
                .as_array()
                .unwrap()
                .contains(&json!(path))
        );
        assert!(!filters["code"].as_array().unwrap().contains(&json!(path)));
        assert!(!filters["python"].as_array().unwrap().contains(&json!(path)));
    }
    let ci = ci_workflow();
    let checks = &ci["jobs"]["tooling-checks"];
    assert!(
        named_step(checks, "Check CI workflows")["with"]["args"]
            .as_str()
            .unwrap()
            .contains(".github/workflows/cli-images.yml")
    );
    let script = named_step(checks, "Test CI workflows, Python, and cache tasks")["run"]
        .as_str()
        .unwrap()
        .replace("\\\n", " ");
    for target in ["--test cli_images_workflow", "--test images"] {
        assert!(script.contains(target), "missing image check: {target}");
    }
    for target in [
        "cli_release_plan",
        "cli_release_assets",
        "cli_release_images",
    ] {
        let commands: Vec<_> = script
            .lines()
            .filter(|line| line.contains(&format!("--test {target}")))
            .collect();
        assert_eq!(commands.len(), 1);
        assert!(!commands[0].contains("ignored"));
    }
    let images = images_workflow();
    assert_eq!(
        named_step(
            &images["jobs"]["validate"],
            "Test release planning with real OCI tools"
        )["run"],
        "cargo test --locked --package xtask --test cli_release_plan -- --ignored"
    );
    let install = named_step(
        &images["jobs"]["validate"],
        "Install registry publication tool",
    )["run"]
        .as_str()
        .unwrap();
    assert!(install.contains("regclient/regclient/releases/download/v0.9.2/regctl-linux-amd64"));
    assert!(install.contains("4da11cfe7009661a498ffafc11f510049864ffb4eb44961743618b3975db3c55"));
    assert!(install.contains("sha256sum --check"));
    assert_eq!(
        named_step(
            &images["jobs"]["validate"],
            "Test publication with a disposable local registry"
        )["run"],
        "cargo test --locked --package xtask --test cli_release_images -- --ignored"
    );
    for (name, job) in ci["jobs"].as_object().unwrap() {
        if name != "release-cli-images" {
            assert_ne!(job["uses"], "./.github/workflows/cli-images.yml");
        }
        if let Some(steps) = job["steps"].as_array() {
            for step in steps {
                let run = step["run"].as_str().unwrap_or_default();
                assert!(
                    !run.contains("cli test-image") && !run.contains("docker build"),
                    "ordinary CI runs container acceptance in {name}"
                );
            }
        }
    }
}
