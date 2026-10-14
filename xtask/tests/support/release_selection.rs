use crate::support::{Result, write_zip};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env, fs,
    os::unix::fs::PermissionsExt,
    process::{Command, Output},
};

pub const REPOSITORY: &str = "mag1cfrog/delta-arrow-reader";
pub const RUNS: &str = "actions/workflows/ci.yml/runs?event=pull_request&head_sha=cccccccccccccccccccccccccccccccccccccccc&status=success&per_page=100";
pub const ARTIFACTS: &str = "actions/runs/701/artifacts?per_page=100";
pub const JOBS: &str = "actions/runs/701/jobs?filter=all&per_page=100";
pub const APPROVALS: &str = "actions/runs/701/approvals";
pub const RELEASE: &str = "releases/tags/v1.2.3";
pub const TAG: &str = "git/ref/tags/v1.2.3";
pub const PULLS: &str = "commits/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/pulls?per_page=100";
pub const MANIFEST: &str = "contents/Cargo.toml?ref=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub const CLI_MANIFEST: &str = "contents/crates/delta-arrow-reader-cli/Cargo.toml?ref=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub const COMPARE: &str = "compare/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb...main";

pub struct Fixture {
    pub directory: tempfile::TempDir,
    pub responses: BTreeMap<String, Value>,
    pub raw: BTreeMap<String, Vec<u8>>,
    pub record: Value,
}

impl Fixture {
    pub fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let repo = json!({"id": 10, "full_name": REPOSITORY});
        let run = json!({
            "id": 701, "run_attempt": 1, "workflow_id": 70, "path": ".github/workflows/ci.yml",
            "event": "pull_request", "status": "completed", "conclusion": "success", "head_sha": "c".repeat(40),
            "head_branch": "release-plz-test", "repository": repo, "head_repository": repo, "pull_requests": [],
        });
        let artifact = |id, prefix: &str| {
            json!({
                "id": id, "name": format!("{prefix}-{}-701-1", "a".repeat(40)), "expired": false,
                "size_in_bytes": 1, "digest": format!("sha256:{}", "f".repeat(64)),
                "workflow_run": {"id": 701, "repository_id": 10, "head_repository_id": 10,
                    "head_sha": "c".repeat(40), "head_branch": "release-plz-test"},
            })
        };
        let names = [
            "Detect Changes",
            "Approve release checks",
            "Release CLI binaries / Validate complete CLI artifact set",
            "Release CLI images and Go consumer / Validate complete CLI image set",
            "Record validated CLI artifacts",
            "CI",
        ];
        let jobs: Vec<_> = names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                json!({
                    "id": i + 1, "name": name, "run_id": 701, "run_attempt": 1,
                    "head_sha": "c".repeat(40), "status": "completed", "conclusion": "success",
                })
            })
            .collect();
        let mut fixture = Self { directory, responses: BTreeMap::from([
            ("".into(), repo.clone()),
            (RELEASE.into(), json!({"id": 7, "tag_name": "v1.2.3", "draft": false, "prerelease": false, "published_at": "2026-10-11T00:00:00Z"})),
            (TAG.into(), json!({"object": {"type": "commit", "sha": "b".repeat(40)}})),
            (format!("git/commits/{}", "b".repeat(40)), json!({"sha": "b".repeat(40), "tree": {"sha": "d".repeat(40)}})),
            (COMPARE.into(), json!({"status": "ahead", "base_commit": {"sha": "b".repeat(40)}, "merge_base_commit": {"sha": "b".repeat(40)}})),
            (PULLS.into(), json!([[], [{"number": 42, "merged_at": "2026-10-11T00:00:00Z", "merge_commit_sha": "b".repeat(40),
                "user": {"login": "release-plz-bot-mag1cfrog[bot]"},
                "head": {"ref": "release-plz-test", "sha": "c".repeat(40), "repo": repo},
                "base": {"ref": "main", "sha": "e".repeat(40), "repo": repo}}]])),
            ("actions/workflows/ci.yml".into(), json!({"id": 70, "path": ".github/workflows/ci.yml"})),
            (RUNS.into(), json!([{"total_count": 1, "workflow_runs": []}, {"total_count": 1, "workflow_runs": [run]}])),
            ("actions/runs/701".into(), run),
            (ARTIFACTS.into(), json!([{"artifacts": []}, {"artifacts": [artifact(901, "dar-cli-validation"), artifact(101, "dar-cli"), artifact(102, "dar-cli-images")]}])),
            (JOBS.into(), json!([{"jobs": []}, {"jobs": jobs}])),
            ("environments/release-checks".into(), json!({"id": 90, "name": "release-checks"})),
            (APPROVALS.into(), json!([{"state": "approved", "user": {"login": "maintainer"}, "environments": [{"id": 90, "name": "release-checks"}]}])),
        ]), raw: BTreeMap::from([
            (MANIFEST.into(), b"[workspace.package]\nrust-version = '1.94.1'\nversion = '1.2.3'\n[workspace.metadata.cli-release]\ncargo-about-version = '0.8.2'\n".to_vec()),
            (CLI_MANIFEST.into(), b"[package]\nname = 'delta-arrow-reader-cli'\n".to_vec()),
        ]), record: json!({
            "schema_version": 1, "repository": REPOSITORY, "pull_request": 42, "pull_request_head_sha": "c".repeat(40),
            "source_commit": "a".repeat(40), "source_tree": "d".repeat(40), "version": "1.2.3", "run_id": 701, "run_attempt": 1,
            "binary_artifact": {"id": 101, "manifest_sha256": "1".repeat(64)},
            "image_artifact": {"id": 102, "manifest_sha256": "2".repeat(64)},
        }) };
        let bin = fixture.directory.path().join("bin");
        fs::create_dir(&bin)?;
        let gh = bin.join("gh");
        fs::write(
            &gh,
            r#"#!/bin/sh
set -eu
[ "$1" = api ] && [ "$2" = --hostname ] && [ "$3" = github.com ] && [ "$4" = --method ] && [ "$5" = GET ] || exit 97
endpoint=
for arg do
  case "$arg" in
    repos/mag1cfrog/delta-arrow-reader*) endpoint="$arg" ;;
  esac
done
[ -n "$endpoint" ] || exit 98
printf '%s\n' "$*" >> "$CLI_TEST_API/calls"
[ "$endpoint" != "${CLI_TEST_FAIL_ENDPOINT:-}" ] || exit 99
case "$endpoint" in
  *per_page=100) case "$*" in *'--paginate --slurp'*) ;; *) exit 96 ;; esac ;;
esac
response="$CLI_TEST_API/responses/$endpoint.body"
if [ -f "$response.next" ]; then
  if [ -f "$response.seen" ]; then response="$response.next"; else touch "$response.seen"; fi
fi
/bin/cat "$response"
"#,
        )?;
        fs::set_permissions(gh, fs::Permissions::from_mode(0o755))?;
        fixture.pack_record(true)?;
        Ok(fixture)
    }

    pub fn pack_record(&mut self, compressed: bool) -> Result<()> {
        self.pack_files(
            &[(
                "cli-validation.json".into(),
                serde_json::to_vec(&self.record)?,
            )],
            compressed,
        )
    }

    pub fn pack_files(&mut self, files: &[(String, Vec<u8>)], compressed: bool) -> Result<()> {
        let path = self.directory.path().join("record.zip");
        write_zip(&path, files, compressed)?;
        let bytes = fs::read(&path)?;
        self.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"][0]["size_in_bytes"] =
            json!(bytes.len());
        self.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"][0]["digest"] =
            json!(format!("sha256:{:x}", Sha256::digest(&bytes)));
        self.raw.insert("actions/artifacts/901/zip".into(), bytes);
        Ok(())
    }

    pub fn command(&self, explicit: bool) -> Result<Command> {
        self.command_for("select-release-artifacts", "selection.json", explicit)
    }

    pub fn command_for(&self, task: &str, output: &str, explicit: bool) -> Result<Command> {
        let mut command = self.xtask()?;
        command.args(["cli", task, output, "--tag", "v1.2.3"]);
        if explicit {
            command.args(["--run-id", "701", "--record-artifact-id", "901"]);
        }
        Ok(command)
    }

    pub fn xtask(&self) -> Result<Command> {
        let path = |endpoint: &str| {
            self.directory.path().join("responses").join(format!(
                "{}.body",
                format!("repos/{REPOSITORY}/{endpoint}").trim_end_matches('/')
            ))
        };
        for (endpoint, value) in &self.responses {
            let destination = path(endpoint);
            fs::create_dir_all(destination.parent().unwrap())?;
            fs::write(destination, serde_json::to_vec(value)?)?;
        }
        for (endpoint, bytes) in &self.raw {
            let destination = path(endpoint);
            fs::create_dir_all(destination.parent().unwrap())?;
            fs::write(destination, bytes)?;
        }
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        command
            .current_dir(self.directory.path())
            .env("CLI_TEST_API", self.directory.path())
            .env("GITHUB_REPOSITORY", REPOSITORY)
            .env_remove("CLI_TEST_FAIL_ENDPOINT")
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.directory.path().join("bin").display(),
                    env::var("PATH")?
                ),
            );
        Ok(command)
    }

    pub fn rejected(&self, output: Output, message: &str) -> Result<()> {
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{message}: {output:?}"
        );
        assert!(!self.directory.path().join("selection.json").exists());
        Ok(())
    }

    pub fn selected(&self, output: Output) -> Result<Value> {
        assert!(output.status.success(), "{output:?}");
        Ok(serde_json::from_slice(&fs::read(
            self.directory.path().join("selection.json"),
        )?)?)
    }
}
