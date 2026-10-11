#![cfg(unix)]

use std::{
    collections::BTreeMap,
    env, fs,
    os::unix::fs::PermissionsExt,
    process::{Command, Output},
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[allow(dead_code)]
mod support;
use support::{Result, write_zip};

const REPOSITORY: &str = "mag1cfrog/delta-arrow-reader";
const RUNS: &str = "actions/workflows/ci.yml/runs?event=pull_request&head_sha=cccccccccccccccccccccccccccccccccccccccc&status=success&per_page=100";
const ARTIFACTS: &str = "actions/runs/701/artifacts?per_page=100";
const JOBS: &str = "actions/runs/701/jobs?filter=all&per_page=100";
const APPROVALS: &str = "actions/runs/701/approvals";
const RELEASE: &str = "releases/tags/v1.2.3";
const TAG: &str = "git/ref/tags/v1.2.3";
const PULLS: &str = "commits/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/pulls?per_page=100";
const MANIFEST: &str = "contents/Cargo.toml?ref=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const CLI_MANIFEST: &str = "contents/crates/delta-arrow-reader-cli/Cargo.toml?ref=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const COMPARE: &str = "compare/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb...main";

struct Fixture {
    directory: tempfile::TempDir,
    responses: BTreeMap<String, Value>,
    raw: BTreeMap<String, Vec<u8>>,
    record: Value,
}

impl Fixture {
    fn new() -> Result<Self> {
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
            (MANIFEST.into(), b"[package]\nrust-version = '1.94.1'\n[workspace.package]\nversion = '1.2.3'\n[workspace.metadata.cli-release]\ncargo-about-version = '0.8.2'\n".to_vec()),
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

    fn pack_record(&mut self, compressed: bool) -> Result<()> {
        self.pack_files(
            &[(
                "cli-validation.json".into(),
                serde_json::to_vec(&self.record)?,
            )],
            compressed,
        )
    }

    fn pack_files(&mut self, files: &[(String, Vec<u8>)], compressed: bool) -> Result<()> {
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

    fn command(&self, explicit: bool) -> Result<Command> {
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
        command.args([
            "cli",
            "select-release-artifacts",
            "selection.json",
            "--tag",
            "v1.2.3",
        ]);
        if explicit {
            command.args(["--run-id", "701", "--record-artifact-id", "901"]);
        }
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

    fn rejected(&self, output: Output, message: &str) -> Result<()> {
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{message}: {output:?}"
        );
        assert!(!self.directory.path().join("selection.json").exists());
        Ok(())
    }

    fn selected(&self, output: Output) -> Result<Value> {
        assert!(output.status.success(), "{output:?}");
        Ok(serde_json::from_slice(&fs::read(
            self.directory.path().join("selection.json"),
        )?)?)
    }
}

#[test]
fn selects_artifact_metadata_across_different_merge_shas_without_the_temporary_commit() -> Result<()>
{
    for compressed in [false, true] {
        let mut fixture = Fixture::new()?;
        fixture.pack_record(compressed)?;
        let selected = fixture.selected(fixture.command(false)?.output()?)?;
        assert_eq!(selected["release"]["commit"], "b".repeat(40));
        assert_eq!(selected["validation_record"], fixture.record);
        assert_ne!(
            selected["release"]["commit"],
            selected["validation_record"]["source_commit"]
        );
        assert_eq!(
            selected["release"]["tree"],
            selected["validation_record"]["source_tree"]
        );
        assert_eq!(selected["binary_artifact"]["id"], 101);
        assert_eq!(selected["image_artifact"]["id"], 102);
        assert_eq!(selected["record_artifact"]["id"], 901);
        let calls = fs::read_to_string(fixture.directory.path().join("calls"))?;
        assert!(!calls.contains(&format!("git/commits/{}", "a".repeat(40))));
        assert!(!calls.contains("actions/artifacts/101/zip"));
        assert!(!calls.contains("actions/artifacts/102/zip"));
        assert!(calls.lines().all(|line| line.contains("--method GET")));
    }
    Ok(())
}

#[test]
fn rejects_unpublished_or_unrelated_releases_and_untrusted_prs() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let valid = fixture.responses.clone();
    for (endpoint, pointer, value, message) in [
        (RELEASE, "/draft", json!(true), "published stable release"),
        (
            RELEASE,
            "/prerelease",
            json!(true),
            "published stable release",
        ),
        (
            RELEASE,
            "/published_at",
            json!(null),
            "published stable release",
        ),
        (
            RELEASE,
            "/tag_name",
            json!("v1.2.4"),
            "published stable release",
        ),
        (
            TAG,
            "/object/type",
            json!("tree"),
            "does not resolve to a commit",
        ),
        (TAG, "/object/sha", json!("short"), "invalid object SHA"),
        (COMPARE, "/status", json!("diverged"), "not reachable"),
        (
            COMPARE,
            "/merge_base_commit/sha",
            json!("f".repeat(40)),
            "not reachable",
        ),
        (PULLS, "/1/0/merged_at", json!(null), "no merged upstream"),
        (
            PULLS,
            "/1/0/merge_commit_sha",
            json!("f".repeat(40)),
            "no merged upstream",
        ),
        (
            PULLS,
            "/1/0/user/login",
            json!("someone"),
            "no merged upstream",
        ),
        (
            PULLS,
            "/1/0/head/ref",
            json!("feature"),
            "no merged upstream",
        ),
        (PULLS, "/1/0/head/sha", json!("short"), "no merged upstream"),
        (PULLS, "/1/0/head/repo/id", json!(11), "no merged upstream"),
        (
            PULLS,
            "/1/0/head/repo/full_name",
            json!("fork/repo"),
            "no merged upstream",
        ),
        (PULLS, "/1/0/head/repo", json!(null), "no merged upstream"),
        (
            PULLS,
            "/1/0/base/ref",
            json!("develop"),
            "no merged upstream",
        ),
        (PULLS, "/1/0/base/repo/id", json!(11), "no merged upstream"),
    ] {
        fixture.responses = valid.clone();
        *fixture
            .responses
            .get_mut(endpoint)
            .unwrap()
            .pointer_mut(pointer)
            .unwrap() = value;
        fixture.rejected(fixture.command(true)?.output()?, message)?;
    }
    fixture.responses = valid;
    fixture.raw.insert(MANIFEST.into(), b"[package]\nrust-version='1.94.1'\n[workspace.package]\nversion='1.2.4'\n[workspace.metadata.cli-release]\ncargo-about-version='0.8.2'\n".to_vec());
    fixture.rejected(fixture.command(true)?.output()?, "workspace version differ")?;
    Ok(())
}

#[test]
fn rejects_wrong_run_workflow_revision_and_repository() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let valid = fixture.responses["actions/runs/701"].clone();
    for (pointer, value) in [
        ("/id", json!(702)),
        ("/workflow_id", json!(71)),
        ("/path", json!(".github/workflows/other.yml")),
        ("/event", json!("workflow_dispatch")),
        ("/status", json!("in_progress")),
        ("/conclusion", json!("failure")),
        ("/head_sha", json!("e".repeat(40))),
        ("/head_branch", json!("release-plz-other")),
        ("/repository/id", json!(11)),
        ("/head_repository/id", json!(11)),
        ("/head_repository/full_name", json!("fork/repo")),
    ] {
        let mut run = valid.clone();
        *run.pointer_mut(pointer).unwrap() = value;
        fixture.responses.insert("actions/runs/701".into(), run);
        fixture.rejected(fixture.command(true)?.output()?, "run")?;
    }
    Ok(())
}

#[test]
fn rejects_records_for_different_trees_versions_prs_or_artifacts() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let valid = fixture.record.clone();
    for (pointer, value) in [
        ("/schema_version", json!(2)),
        ("/repository", json!("fork/repo")),
        ("/pull_request", json!(43)),
        ("/pull_request_head_sha", json!("e".repeat(40))),
        ("/source_commit", json!("short")),
        ("/source_tree", json!("e".repeat(40))),
        ("/version", json!("1.2.4")),
        ("/run_id", json!(702)),
        ("/run_attempt", json!(2)),
        ("/binary_artifact/id", json!(102)),
        ("/image_artifact/id", json!(901)),
        ("/binary_artifact/manifest_sha256", json!("")),
        ("/image_artifact/manifest_sha256", json!("A".repeat(64))),
    ] {
        fixture.record = valid.clone();
        *fixture.record.pointer_mut(pointer).unwrap() = value;
        fixture.pack_record(true)?;
        fixture.rejected(
            fixture.command(true)?.output()?,
            "validation record differs",
        )?;
    }
    fixture.record = valid;
    fixture.record["unknown_field"] = json!("untrusted");
    fixture.pack_record(true)?;
    fixture.rejected(fixture.command(true)?.output()?, "unknown field")?;
    Ok(())
}

#[test]
fn requires_successful_jobs_and_an_environment_approval_for_the_same_revision() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let valid = fixture.responses[JOBS].clone();
    for index in 0..6 {
        for conclusion in ["skipped", "cancelled", "failure"] {
            fixture.responses.insert(JOBS.into(), valid.clone());
            fixture.responses.get_mut(JOBS).unwrap()[1]["jobs"][index]["conclusion"] =
                json!(conclusion);
            fixture.rejected(fixture.command(true)?.output()?, "unsuccessful")?;
        }
        fixture.responses.insert(JOBS.into(), valid.clone());
        fixture.responses.get_mut(JOBS).unwrap()[1]["jobs"]
            .as_array_mut()
            .unwrap()
            .remove(index);
        fixture.rejected(fixture.command(true)?.output()?, "missing job")?;
    }
    fixture.responses.insert(JOBS.into(), valid.clone());
    fixture.responses.get_mut(JOBS).unwrap()[1]["jobs"][4]["head_sha"] = json!("e".repeat(40));
    fixture.rejected(fixture.command(true)?.output()?, "another revision")?;
    fixture.responses.insert(JOBS.into(), valid);
    let reviews = fixture.responses[APPROVALS].clone();
    for value in [
        json!([]),
        json!([{"state":"rejected", "user":{"login":"maintainer"}, "environments":[{"id":90,"name":"release-checks"}]}]),
        json!([{"state":"approved", "user":{"login":"maintainer"}, "environments":[{"id":91,"name":"release-checks"}]}]),
    ] {
        fixture.responses.insert(APPROVALS.into(), value);
        fixture.rejected(fixture.command(true)?.output()?, "no release-checks")?;
    }
    fixture.responses.insert(APPROVALS.into(), reviews);
    Ok(())
}

#[test]
fn rejects_missing_expired_foreign_or_mixed_artifacts() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let valid = fixture.responses[ARTIFACTS].clone();
    for index in 0..3 {
        for (pointer, value) in [
            ("/expired", json!(true)),
            ("/digest", json!(null)),
            ("/size_in_bytes", json!(0)),
            ("/workflow_run/id", json!(702)),
            ("/workflow_run/repository_id", json!(11)),
            ("/workflow_run/head_repository_id", json!(11)),
            ("/workflow_run/head_sha", json!("e".repeat(40))),
            ("/workflow_run/head_branch", json!("other")),
            ("/name", json!("wrong-source-or-attempt")),
        ] {
            fixture.responses.insert(ARTIFACTS.into(), valid.clone());
            *fixture.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"][index]
                .pointer_mut(pointer)
                .unwrap() = value;
            fixture.rejected(fixture.command(true)?.output()?, "")?;
        }
        fixture.responses.insert(ARTIFACTS.into(), valid.clone());
        fixture.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"]
            .as_array_mut()
            .unwrap()
            .remove(index);
        fixture.rejected(fixture.command(true)?.output()?, "missing")?;
    }
    Ok(())
}

#[test]
fn verifies_the_record_zip_digest_inventory_and_size() -> Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.raw.get_mut("actions/artifacts/901/zip").unwrap()[0] ^= 1;
    fixture.rejected(fixture.command(true)?.output()?, "SHA256 differs")?;
    for files in [
        vec![(
            "../cli-validation.json".into(),
            serde_json::to_vec(&fixture.record)?,
        )],
        vec![
            (
                "cli-validation.json".into(),
                serde_json::to_vec(&fixture.record)?,
            ),
            ("extra".into(), vec![]),
        ],
        vec![("cli-validation.json".into(), vec![b' '; 65 * 1024])],
    ] {
        fixture.pack_files(&files, true)?;
        fixture.rejected(fixture.command(true)?.output()?, "")?;
    }
    Ok(())
}

#[test]
fn supports_partial_reruns_without_mixing_producer_attempts() -> Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.responses.get_mut("actions/runs/701").unwrap()["run_attempt"] = json!(2);
    fixture.responses.get_mut(JOBS).unwrap()[1]["jobs"][4]["run_attempt"] = json!(2);
    fixture.responses.get_mut(JOBS).unwrap()[1]["jobs"][5]["run_attempt"] = json!(2);
    fixture.record["run_attempt"] = json!(2);
    fixture.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"][0]["name"] =
        json!(format!("dar-cli-validation-{}-701-2", "a".repeat(40)));
    fixture.pack_record(true)?;
    let selected = fixture.selected(fixture.command(true)?.output()?)?;
    assert_eq!(selected["validation_record"]["run_attempt"], 2);
    assert!(
        selected["binary_artifact"]["name"]
            .as_str()
            .unwrap()
            .ends_with("-701-1")
    );
    fs::remove_file(fixture.directory.path().join("selection.json"))?;
    fixture.responses.get_mut(JOBS).unwrap()[1]["jobs"][2]["run_attempt"] = json!(2);
    fixture.rejected(fixture.command(true)?.output()?, "producer attempt")?;
    Ok(())
}

#[test]
fn refuses_ambiguous_records_until_both_recovery_ids_are_explicit() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let mut second_run = fixture.responses["actions/runs/701"].clone();
    second_run["id"] = json!(702);
    fixture.responses.get_mut(RUNS).unwrap()[1]["workflow_runs"]
        .as_array_mut()
        .unwrap()
        .push(second_run);
    let mut record = fixture.record.clone();
    record["run_id"] = json!(702);
    record["binary_artifact"]["id"] = json!(201);
    record["image_artifact"]["id"] = json!(202);
    let path = fixture.directory.path().join("second.zip");
    write_zip(
        &path,
        &[("cli-validation.json".into(), serde_json::to_vec(&record)?)],
        true,
    )?;
    let bytes = fs::read(path)?;
    let mut artifacts = fixture.responses[ARTIFACTS].clone();
    for (index, id) in [902, 201, 202].into_iter().enumerate() {
        let artifact = &mut artifacts[1]["artifacts"][index];
        artifact["id"] = json!(id);
        artifact["workflow_run"]["id"] = json!(702);
        artifact["name"] = json!(artifact["name"].as_str().unwrap().replace("-701-", "-702-"));
    }
    artifacts[1]["artifacts"][0]["size_in_bytes"] = json!(bytes.len());
    artifacts[1]["artifacts"][0]["digest"] = json!(format!("sha256:{:x}", Sha256::digest(&bytes)));
    fixture
        .responses
        .insert(ARTIFACTS.replace("701", "702"), artifacts);
    fixture
        .raw
        .insert("actions/artifacts/902/zip".into(), bytes);
    let mut jobs = fixture.responses[JOBS].clone();
    for job in jobs[1]["jobs"].as_array_mut().unwrap() {
        job["run_id"] = json!(702);
    }
    fixture.responses.insert(JOBS.replace("701", "702"), jobs);
    fixture.responses.insert(
        APPROVALS.replace("701", "702"),
        fixture.responses[APPROVALS].clone(),
    );
    let output = fixture.command(false)?.output()?;
    assert!(String::from_utf8_lossy(&output.stderr).contains("run 702, record artifact 902"));
    fixture.rejected(output, "multiple eligible")?;
    let mut command = fixture.command(false)?;
    command.env(
        "CLI_TEST_FAIL_ENDPOINT",
        format!("repos/{REPOSITORY}/actions/runs/702/approvals"),
    );
    fixture.rejected(command.output()?, "gh failed")?;
    fixture.selected(fixture.command(true)?.output()?)?;
    Ok(())
}

#[test]
fn resolves_annotated_tags_and_rechecks_release_identity_before_output() -> Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.responses.insert(
        TAG.into(),
        json!({"object": {"type":"tag", "sha":"f".repeat(40)}}),
    );
    fixture.responses.insert(
        format!("git/tags/{}", "f".repeat(40)),
        json!({"object":{"type":"commit", "sha":"b".repeat(40)}}),
    );
    fixture.selected(fixture.command(true)?.output()?)?;

    for (endpoint, changed) in [
        (
            TAG,
            json!({"object":{"type":"commit", "sha":"e".repeat(40)}}),
        ),
        (
            RELEASE,
            json!({"id":8, "tag_name":"v1.2.3", "draft":false, "prerelease":false, "published_at":"2026-10-11T00:00:00Z"}),
        ),
    ] {
        let fixture = Fixture::new()?;
        let mut command = fixture.command(true)?;
        fs::write(
            fixture
                .directory
                .path()
                .join(format!("responses/repos/{REPOSITORY}/{endpoint}.body.next")),
            serde_json::to_vec(&changed)?,
        )?;
        fixture.rejected(command.output()?, "changed during")?;
    }
    Ok(())
}

#[test]
fn rejects_invalid_tag_arguments_and_incomplete_recovery_selection() -> Result<()> {
    let fixture = Fixture::new()?;
    for tag in [
        "v1.2.3-rc.1",
        "v01.2.3",
        "v1.2",
        "benchmark-result",
        "v1.2.3/../main",
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        command
            .args([
                "cli",
                "select-release-artifacts",
                "selection.json",
                "--tag",
                tag,
            ])
            .current_dir(fixture.directory.path());
        fixture.rejected(command.output()?, "stable vX.Y.Z")?;
    }
    for option in ["--run-id", "--record-artifact-id"] {
        let mut command = fixture.command(false)?;
        command.args([option, "701"]);
        fixture.rejected(command.output()?, "required")?;
    }
    let mut command = fixture.command(true)?;
    command.env("GITHUB_REPOSITORY", "fork/repo");
    fixture.rejected(command.output()?, "upstream repository")?;
    Ok(())
}

#[test]
fn does_not_fall_back_after_missing_candidates_or_truncated_run_search() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let valid = fixture.responses[RUNS].clone();
    fixture
        .responses
        .insert(RUNS.into(), json!([{"total_count":0, "workflow_runs":[]} ]));
    fixture.rejected(fixture.command(false)?.output()?, "no eligible")?;
    fixture.responses.insert(RUNS.into(), valid);
    fixture.responses.get_mut(RUNS).unwrap()[0]["total_count"] = json!(1000);
    fixture.rejected(
        fixture.command(false)?.output()?,
        "search reached GitHub's limit",
    )?;
    Ok(())
}

#[test]
fn required_job_names_follow_the_current_workflows() -> Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let read = |name| -> Result<serde_yaml_ng::Value> {
        Ok(serde_yaml_ng::from_str(&fs::read_to_string(
            root.join(format!(".github/workflows/{name}")),
        )?)?)
    };
    let ci = read("ci.yml")?;
    let binary = read("cli-build.yml")?;
    let image = read("cli-images.yml")?;
    let names = [
        ci["jobs"]["changes"]["name"].as_str().unwrap().to_owned(),
        ci["jobs"]["release-approval"]["name"]
            .as_str()
            .unwrap()
            .to_owned(),
        format!(
            "{} / {}",
            ci["jobs"]["release-cli"]["name"].as_str().unwrap(),
            binary["jobs"]["validate"]["name"].as_str().unwrap()
        ),
        format!(
            "{} / {}",
            ci["jobs"]["release-cli-images"]["name"].as_str().unwrap(),
            image["jobs"]["validate"]["name"].as_str().unwrap()
        ),
        ci["jobs"]["release-cli-record"]["name"]
            .as_str()
            .unwrap()
            .to_owned(),
        ci["jobs"]["ci"]["name"].as_str().unwrap().to_owned(),
    ];
    let mut fixture = Fixture::new()?;
    for (job, name) in fixture.responses.get_mut(JOBS).unwrap()[1]["jobs"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .zip(names)
    {
        job["name"] = json!(name);
    }
    fixture.selected(fixture.command(true)?.output()?)?;
    Ok(())
}

#[test]
fn propagates_transport_failures_and_never_replaces_existing_output() -> Result<()> {
    let fixture = Fixture::new()?;
    let mut command = fixture.command(true)?;
    command.env(
        "CLI_TEST_FAIL_ENDPOINT",
        format!("repos/{REPOSITORY}/{APPROVALS}"),
    );
    fixture.rejected(command.output()?, "gh failed")?;
    fs::write(fixture.directory.path().join("selection.json"), "keep me")?;
    let output = fixture.command(true)?.output()?;
    assert!(!output.status.success());
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("selection.json"))?,
        "keep me"
    );
    Ok(())
}
