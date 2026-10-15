#![cfg(unix)]

use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    process::{Command, Output},
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

mod support;
use support::{PLATFORMS, Result, WheelFixture, wheel_files, write_wheel, write_zip};

const REPOSITORY: &str = "mag1cfrog/delta-arrow-reader";

struct ReuseFixture {
    wheels: WheelFixture,
    files: Vec<(String, Vec<u8>)>,
    pulls: Value,
    runs: Value,
    artifacts: Value,
    merge: Value,
    digest: String,
}

impl ReuseFixture {
    fn new(name: &str) -> Result<Self> {
        let wheels = WheelFixture::new(name)?;
        let (directory, paths) = wheels.create_artifacts()?;
        let files = paths
            .iter()
            .map(|path| {
                Ok((
                    path.strip_prefix(&directory)?.to_str().unwrap().to_owned(),
                    fs::read(path)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut fixture = Self {
            wheels,
            files,
            pulls: Value::Null,
            runs: Value::Null,
            artifacts: Value::Null,
            merge: Value::Null,
            digest: String::new(),
        };
        fixture.git(&["init", "--initial-branch=main"])?;
        fs::write(
            fixture.wheels.directory.join("Cargo.toml"),
            "[workspace.package]\nversion = '1.2.3'\n",
        )?;
        fixture.git(&["add", "Cargo.toml"])?;
        fixture.git(&["commit", "-m", "test: release PR source"])?;
        let head = fixture.git(&["rev-parse", "HEAD"])?;
        fixture.git(&["commit", "--allow-empty", "-m", "test: merged release PR"])?;
        let commit = fixture.git(&["rev-parse", "HEAD"])?;
        assert_ne!(head, commit);
        fixture.git(&["tag", "v1.2.3"])?;
        let tree = fixture.git(&["rev-parse", "HEAD^{tree}"])?;
        fixture.merge = json!({"sha": commit, "tree": {"sha": tree}});
        fixture.pulls = json!([[], [{
            "number": 42, "merged_at": "2026-10-11T00:00:00Z", "merge_commit_sha": commit,
            "user": {"login": "release-plz-bot-mag1cfrog[bot]"},
            "head": {"ref": "release-plz-test", "sha": head, "repo": {"full_name": REPOSITORY}},
            "base": {"ref": "main", "sha": "unused", "repo": {"full_name": REPOSITORY}},
        }]]);
        fixture.runs = json!([{"workflow_runs": []}, {"workflow_runs": [{
            "id": 701, "run_attempt": 1, "event": "pull_request", "path": ".github/workflows/ci.yml",
            "status": "completed", "conclusion": "success", "head_sha": head, "head_branch": "release-plz-test",
            "repository": {"full_name": REPOSITORY}, "head_repository": {"full_name": REPOSITORY},
            "pull_requests": [],
        }]}]);
        fixture.artifacts = json!([{"artifacts": []}, {"artifacts": [{
            "id": 901, "name": format!("python-wheels-{tree}-701-1"), "expired": false, "digest": "",
        }]}]);
        fixture.write_bundle(true)?;
        let bin = fixture.wheels.directory.join("bin");
        fs::create_dir(&bin)?;
        let gh = bin.join("gh");
        fs::write(
            &gh,
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$TEST_API/api-log"
[ "$1" = api ] && [ "$2" = --hostname ] && [ "$3" = github.com ] || exit 97
for endpoint do :; done
case "$endpoint" in
  repos/mag1cfrog/delta-arrow-reader/commits/*/pulls?per_page=100) response=pulls ;;
  repos/mag1cfrog/delta-arrow-reader/git/commits/*) response=merge ;;
  repos/mag1cfrog/delta-arrow-reader/actions/workflows/ci.yml/runs?event=pull_request\&head_sha=*\&per_page=100) response=runs ;;
  repos/mag1cfrog/delta-arrow-reader/actions/runs/701/artifacts?per_page=100) response=artifacts ;;
  repos/mag1cfrog/delta-arrow-reader/actions/artifacts/901/zip) response=bundle ;;
  *) echo "Unexpected endpoint: $endpoint" >&2; exit 98 ;;
esac
[ "$FAIL_RESPONSE" != "$response" ] || exit 99
/bin/cat "$TEST_API/$response"
"#,
        )?;
        fs::set_permissions(gh, fs::Permissions::from_mode(0o755))?;
        fs::write(fixture.wheels.directory.join("output"), "existing=kept\n")?;
        Ok(fixture)
    }

    fn git(&self, args: &[&str]) -> Result<String> {
        let output = Command::new("git")
            .args([
                "-c",
                "user.name=Wheel reuse test",
                "-c",
                "user.email=wheel-test@example.invalid",
            ])
            .args(args)
            .current_dir(&self.wheels.directory)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()?;
        assert!(output.status.success(), "git {args:?}: {output:?}");
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }

    fn write_bundle(&mut self, compressed: bool) -> Result<()> {
        write_zip(
            &self.wheels.directory.join("bundle"),
            &self.files,
            compressed,
        )?;
        self.refresh_digest()
    }

    fn refresh_digest(&mut self) -> Result<()> {
        self.digest = format!(
            "{:x}",
            Sha256::digest(fs::read(self.wheels.directory.join("bundle"))?)
        );
        self.artifacts[1]["artifacts"][0]["digest"] = json!(format!("sha256:{}", self.digest));
        Ok(())
    }

    fn command(&self, task: &str) -> Result<Command> {
        for (name, response) in [
            ("pulls", &self.pulls),
            ("runs", &self.runs),
            ("artifacts", &self.artifacts),
            ("merge", &self.merge),
        ] {
            fs::write(
                self.wheels.directory.join(name),
                serde_json::to_vec(response)?,
            )?;
        }
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        command
            .arg(task)
            .args(["reused-wheels", "--expected-version", "1.2.3"]);
        if task == "download-python-wheels" {
            command.args(["--artifact-id", "901", "--sha256", &self.digest]);
        }
        command
            .current_dir(&self.wheels.directory)
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.wheels.directory.join("bin").display(),
                    env::var("PATH")?
                ),
            )
            .env("TEST_API", &self.wheels.directory)
            .env("GITHUB_REPOSITORY", REPOSITORY)
            .env("GITHUB_OUTPUT", self.wheels.directory.join("output"))
            .env("FAIL_RESPONSE", "");
        Ok(command)
    }

    fn assert_files(&self) -> Result<()> {
        for (name, contents) in &self.files {
            assert_eq!(
                fs::read(self.wheels.directory.join("reused-wheels").join(name))?,
                *contents
            );
        }
        Ok(())
    }

    fn assert_rejected(&self, output: Output, message: &str) -> Result<()> {
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{output:?}"
        );
        assert_eq!(
            fs::read_to_string(self.wheels.directory.join("output"))?,
            "existing=kept\n"
        );
        Ok(())
    }
}

#[test]
fn reuses_exact_wheels_across_merge_shas_and_deleted_pr_branches() -> Result<()> {
    let fixture = ReuseFixture::new("reuse-merged")?;
    let output = fixture.command("reuse-python-wheels")?.output()?;
    assert!(output.status.success(), "{output:?}");
    fixture.assert_files()?;
    assert_eq!(
        fs::read_to_string(fixture.wheels.directory.join("output"))?,
        format!(
            "existing=kept\nartifact-id=901\nartifact-sha256={}\n",
            fixture.digest
        )
    );
    let calls = fs::read_to_string(fixture.wheels.directory.join("api-log"))?;
    assert_eq!(calls.lines().count(), 4);
    assert!(
        calls
            .lines()
            .take(3)
            .all(|line| line.contains("--paginate --slurp"))
    );
    assert!(calls.contains(&format!(
        "commits/{}/pulls",
        fixture.git(&["rev-parse", "HEAD"])?
    )));
    Ok(())
}

#[test]
fn tagged_pr_heads_require_an_identical_merge_tree() -> Result<()> {
    for case in [
        "valid",
        "changed-tree",
        "wrong-commit",
        "api-error",
        "missing-merge",
    ] {
        let mut fixture = ReuseFixture::new(&format!("reuse-tagged-head-{case}"))?;
        let head = fixture.pulls[1][0]["head"]["sha"]
            .as_str()
            .unwrap()
            .to_owned();
        fixture.git(&["checkout", "--detach", &head])?;
        match case {
            "changed-tree" => fixture.merge["tree"]["sha"] = json!("e".repeat(40)),
            "wrong-commit" => fixture.merge["sha"] = json!("e".repeat(40)),
            "missing-merge" => fixture.pulls[1][0]["merge_commit_sha"] = json!(null),
            _ => {}
        }
        let mut command = fixture.command("reuse-python-wheels")?;
        if case == "api-error" {
            command.env("FAIL_RESPONSE", "merge");
        }
        let output = command.output()?;
        if case == "valid" {
            assert!(output.status.success(), "{output:?}");
            fixture.assert_files()?;
        } else {
            fixture.assert_rejected(output, "")?;
            assert!(!fixture.wheels.directory.join("reused-wheels").exists());
        }
    }
    Ok(())
}

#[test]
fn requires_a_merged_upstream_release_pr_for_the_exact_commit() -> Result<()> {
    let mut fixture = ReuseFixture::new("reuse-pr-identity")?;
    let valid = fixture.pulls.clone();
    for (pointer, value) in [
        ("/merged_at", json!(null)),
        ("/merge_commit_sha", json!("other")),
        ("/user/login", json!("mag1cfrog")),
        ("/head/ref", json!("feature")),
        ("/head/repo/full_name", json!("fork/repo")),
        ("/head/repo", json!(null)),
        ("/base/ref", json!("develop")),
        ("/base/repo/full_name", json!("other/repo")),
    ] {
        fixture.pulls = valid.clone();
        *fixture.pulls[1][0].pointer_mut(pointer).unwrap() = value;
        fixture.assert_rejected(
            fixture.command("reuse-python-wheels")?.output()?,
            "exactly one merged release-plz PR",
        )?;
    }
    fixture.pulls = json!([[valid[1][0], valid[1][0]]]);
    fixture.assert_rejected(
        fixture.command("reuse-python-wheels")?.output()?,
        "exactly one merged release-plz PR",
    )?;
    fixture.assert_rejected(
        fixture
            .command("reuse-python-wheels")?
            .env("GITHUB_REPOSITORY", "fork/repo")
            .output()?,
        "upstream repository",
    )?;
    assert!(!fixture.wheels.directory.join("reused-wheels").exists());
    Ok(())
}

#[test]
fn latest_ci_must_succeed_with_the_correct_workflow_and_pr_head() -> Result<()> {
    let mut fixture = ReuseFixture::new("reuse-ci-identity")?;
    let valid = fixture.runs[1]["workflow_runs"][0].clone();
    for (pointer, value) in [
        ("/event", json!("workflow_dispatch")),
        ("/path", json!(".github/workflows/pypi.yml")),
        ("/status", json!("in_progress")),
        ("/conclusion", json!("failure")),
        ("/conclusion", json!("cancelled")),
        ("/conclusion", json!(null)),
        ("/head_sha", json!("other")),
        ("/head_branch", json!("feature")),
        ("/repository/full_name", json!("fork/repo")),
        ("/head_repository/full_name", json!("fork/repo")),
        ("/run_attempt", json!(0)),
    ] {
        let mut invalid = valid.clone();
        invalid["id"] = json!(702);
        *invalid.pointer_mut(pointer).unwrap() = value;
        fixture.runs = json!([{"workflow_runs": [invalid]}, {"workflow_runs": [valid]}]);
        fixture.assert_rejected(
            fixture.command("reuse-python-wheels")?.output()?,
            "latest release PR CI run",
        )?;
    }
    assert!(!fixture.wheels.directory.join("reused-wheels").exists());
    Ok(())
}

#[test]
fn handles_reruns_without_falling_back_to_an_older_bundle() -> Result<()> {
    for rebuilt in [false, true] {
        let mut fixture = ReuseFixture::new(&format!("reuse-attempt-{rebuilt}"))?;
        fixture.runs[1]["workflow_runs"][0]["run_attempt"] = json!(2);
        if rebuilt {
            let mut old = fixture.artifacts[1]["artifacts"][0].clone();
            old["id"] = json!(900);
            fixture.artifacts[1]["artifacts"][0]["name"] = json!(format!(
                "python-wheels-{}-701-2",
                fixture.git(&["rev-parse", "HEAD^{tree}"])?
            ));
            fixture.artifacts[1]["artifacts"]
                .as_array_mut()
                .unwrap()
                .push(old);
        }
        let output = fixture.command("reuse-python-wheels")?.output()?;
        assert!(output.status.success(), "{output:?}");
        fixture.assert_files()?;
    }
    let mut fixture = ReuseFixture::new("reuse-expired-latest")?;
    let mut older = fixture.artifacts[1]["artifacts"][0].clone();
    older["id"] = json!(900);
    fixture.artifacts[1]["artifacts"][0]["expired"] = json!(true);
    fixture.artifacts[1]["artifacts"][0]["name"] = json!(format!(
        "python-wheels-{}-701-2",
        fixture.git(&["rev-parse", "HEAD^{tree}"])?
    ));
    fixture.artifacts[1]["artifacts"]
        .as_array_mut()
        .unwrap()
        .push(older);
    fixture.runs[1]["workflow_runs"][0]["run_attempt"] = json!(2);
    fixture.assert_rejected(fixture.command("reuse-python-wheels")?.output()?, "expired")?;
    Ok(())
}

#[test]
fn rejects_wrong_source_missing_expired_and_ambiguous_bundles() -> Result<()> {
    let mut fixture = ReuseFixture::new("reuse-bundle-identity")?;
    let valid = fixture.artifacts.clone();
    let prefix = format!(
        "python-wheels-{}-701-",
        fixture.git(&["rev-parse", "HEAD^{tree}"])?
    );
    for (pointer, value, error) in [
        (
            "/name",
            json!("python-wheels-wrong-tree-701-1"),
            "source tree",
        ),
        (
            "/name",
            json!(format!("{prefix}0")),
            "invalid CI run attempt",
        ),
        (
            "/name",
            json!(format!("{prefix}2")),
            "invalid CI run attempt",
        ),
        ("/expired", json!(true), "expired"),
        ("/digest", json!(""), "no SHA256 digest"),
        (
            "/digest",
            json!(format!("sha256:{}", "0".repeat(64))),
            "SHA256 mismatch",
        ),
    ] {
        fixture.artifacts = valid.clone();
        *fixture.artifacts[1]["artifacts"][0]
            .pointer_mut(pointer)
            .unwrap() = value;
        fixture.assert_rejected(fixture.command("reuse-python-wheels")?.output()?, error)?;
    }
    fixture.artifacts =
        json!([{"artifacts": [valid[1]["artifacts"][0], valid[1]["artifacts"][0]]}]);
    fixture.assert_rejected(
        fixture.command("reuse-python-wheels")?.output()?,
        "multiple wheel bundles",
    )?;
    fixture.artifacts = json!([{"artifacts": []}]);
    fixture.assert_rejected(
        fixture.command("reuse-python-wheels")?.output()?,
        "source tree",
    )?;
    assert!(!fixture.wheels.directory.join("reused-wheels").exists());
    Ok(())
}

#[test]
fn a_source_change_after_pr_validation_blocks_reuse_even_with_the_same_version() -> Result<()> {
    let mut fixture = ReuseFixture::new("reuse-source-changed")?;
    fs::write(
        fixture.wheels.directory.join("reader.rs"),
        "changed source\n",
    )?;
    fixture.git(&["add", "reader.rs"])?;
    fixture.git(&["commit", "-m", "test: main changed before merge"])?;
    fixture.pulls[1][0]["merge_commit_sha"] = json!(fixture.git(&["rev-parse", "HEAD"])?);
    fixture.assert_rejected(
        fixture.command("reuse-python-wheels")?.output()?,
        "source tree",
    )?;
    assert!(!fixture.wheels.directory.join("reused-wheels").exists());
    Ok(())
}

#[test]
fn github_failures_and_malformed_responses_never_enable_uploads() -> Result<()> {
    let fixture = ReuseFixture::new("reuse-api-errors")?;
    for response in ["pulls", "runs", "artifacts", "bundle"] {
        fixture.assert_rejected(
            fixture
                .command("reuse-python-wheels")?
                .env("FAIL_RESPONSE", response)
                .output()?,
            "gh failed",
        )?;
    }
    for response in ["pulls", "runs", "artifacts"] {
        let mut command = fixture.command("reuse-python-wheels")?;
        fs::write(fixture.wheels.directory.join(response), "not JSON")?;
        fixture.assert_rejected(command.output()?, "")?;
    }
    assert!(!fixture.wheels.directory.join("reused-wheels").exists());
    Ok(())
}

#[test]
fn rejects_incomplete_duplicate_unsafe_and_mismatched_zip_entries() -> Result<()> {
    let mut fixture = ReuseFixture::new("reuse-zip-entries")?;
    let valid = fixture.files.clone();
    for name in [
        "../escaped.whl",
        "/tmp/escaped.whl",
        "other/wheel.whl",
        &valid[1].0,
    ] {
        fixture.files = valid.clone();
        fixture.files[0].0 = name.to_owned();
        fixture.write_bundle(true)?;
        fixture.assert_rejected(fixture.command("reuse-python-wheels")?.output()?, "")?;
        assert!(!fixture.wheels.directory.join("reused-wheels").exists());
    }
    fixture.files = valid.clone();
    fixture.files.pop();
    fixture.write_bundle(true)?;
    fixture.assert_rejected(
        fixture.command("reuse-python-wheels")?.output()?,
        "exactly three",
    )?;
    fixture.files = valid;
    fixture.write_bundle(false)?;
    let mut bytes = fs::read(fixture.wheels.directory.join("bundle"))?;
    bytes[30] ^= 1;
    fs::write(fixture.wheels.directory.join("bundle"), bytes)?;
    fixture.refresh_digest()?;
    fixture.assert_rejected(
        fixture.command("reuse-python-wheels")?.output()?,
        "local header",
    )?;
    assert!(!fixture.wheels.directory.join("reused-wheels").exists());
    Ok(())
}

#[test]
fn validates_wheel_contents_before_emitting_artifact_identity() -> Result<()> {
    let mut fixture = ReuseFixture::new("reuse-bad-wheel")?;
    fixture.files[0].1 = b"not a wheel".to_vec();
    fixture.write_bundle(true)?;
    fixture.assert_rejected(fixture.command("reuse-python-wheels")?.output()?, "")?;
    Ok(())
}

#[test]
fn a_matching_bundle_digest_cannot_authorize_a_wheel_without_its_cli() -> Result<()> {
    for (index, (platform, tag)) in PLATFORMS.into_iter().enumerate() {
        for task in ["reuse-python-wheels", "download-python-wheels"] {
            let mut fixture = ReuseFixture::new(&format!("missing-cli-{platform}-{task}"))?;
            let path = fixture
                .wheels
                .directory
                .join("wheel-artifacts")
                .join(&fixture.files[index].0);
            let mut files = wheel_files(tag);
            files.retain(|(name, _)| !name.contains(".data/scripts/"));
            write_wheel(&path, &files, true)?;
            fixture.files[index].1 = fs::read(path)?;
            // Match the new bundle's SHA256 so only the missing payload can reject it.
            fixture.write_bundle(true)?;
            fixture.assert_rejected(
                fixture.command(task)?.output()?,
                "missing required wheel file: delta_arrow_reader-1.2.3.data/scripts/delta-arrow-reader",
            )?;
        }
    }
    Ok(())
}

#[test]
fn upload_download_uses_the_pinned_id_and_preserves_existing_files() -> Result<()> {
    let mut fixture = ReuseFixture::new("reuse-pinned-download")?;
    fixture.write_bundle(false)?;
    let output = fixture.command("download-python-wheels")?.output()?;
    assert!(output.status.success(), "{output:?}");
    fixture.assert_files()?;
    fixture.assert_rejected(
        fixture.command("download-python-wheels")?.output()?,
        "must not already exist",
    )?;
    fixture.assert_files()?;
    let calls = fs::read_to_string(fixture.wheels.directory.join("api-log"))?;
    assert_eq!(
        calls.trim(),
        "api --hostname github.com repos/mag1cfrog/delta-arrow-reader/actions/artifacts/901/zip"
    );
    Ok(())
}
