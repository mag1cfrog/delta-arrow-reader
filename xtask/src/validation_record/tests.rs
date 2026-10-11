use serde_json::json;

use super::*;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

struct Fixture {
    directory: tempfile::TempDir,
    repository: PathBuf,
    source: String,
    head: String,
    tree: String,
    event: Value,
    jobs: Value,
}

impl Fixture {
    fn new() -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let repository = directory.path().join("repository");
        fs::create_dir(&repository)?;
        let git = |args: &[&str]| -> Result<String> {
            command_output(
                Command::new("git")
                    .args(args)
                    .current_dir(&repository)
                    .env("GIT_CONFIG_GLOBAL", "/dev/null")
                    .env("GIT_CONFIG_NOSYSTEM", "1"),
            )
            .map(|text| text.trim().to_owned())
        };
        git(&["init", "--initial-branch=main"])?;
        git(&["config", "user.name", "CLI validation test"])?;
        git(&["config", "user.email", "cli-validation@example.invalid"])?;
        fs::write(
            repository.join("Cargo.toml"),
            r#"
[package]
rust-version = "1.94.1"
[workspace.package]
version = "1.2.3"
[workspace.metadata.cli-release]
cargo-about-version = "0.8.2"
"#,
        )?;
        git(&["add", "Cargo.toml"])?;
        git(&["commit", "-m", "test: create base"])?;
        let base = git(&["rev-parse", "HEAD"])?;
        git(&["checkout", "-b", "release-plz-fixture"])?;
        fs::write(repository.join("release.txt"), "release fixture\n")?;
        git(&["add", "release.txt"])?;
        git(&["commit", "-m", "chore: release v1.2.3"])?;
        let head = git(&["rev-parse", "HEAD"])?;
        git(&["checkout", "main"])?;
        git(&[
            "merge",
            "--no-ff",
            "release-plz-fixture",
            "-m",
            "test: merge candidate",
        ])?;
        let source = git(&["rev-parse", "HEAD"])?;
        let tree = git(&["rev-parse", "HEAD^{tree}"])?;
        let event = json!({
            "number": 123, "repository": {"full_name": REPOSITORY},
            "pull_request": {
                "number": 123, "user": {"login": "release-plz-bot-mag1cfrog[bot]"},
                "head": {"sha": head, "ref": "release-plz-fixture", "repo": {"full_name": REPOSITORY}},
                "base": {"sha": base, "ref": "main", "repo": {"full_name": REPOSITORY}},
            }
        });
        let mut jobs = json!({
            "changes": {"result": "success", "outputs": {
                "release_pr": "true", "package_version": "1.2.3", "pr_head_sha": head,
            }},
            "release-approval": {"result": "success", "outputs": {}},
        });
        for (job, id, digest) in [
            ("release-cli", "101", "a"),
            ("release-cli-images", "102", "b"),
        ] {
            jobs[job] = json!({"result": "success", "outputs": {
                "validated": "true", "source-commit": source, "version": "1.2.3",
                "rust-toolchain": "1.94.1", "artifact-id": id, "manifest-sha256": digest.repeat(64),
            }});
        }
        Ok(Self {
            directory,
            repository,
            source,
            head,
            tree,
            event,
            jobs,
        })
    }

    fn arguments(&self) -> std::result::Result<RecordValidationArgs, Box<dyn std::error::Error>> {
        let event = self.directory.path().join("event.json");
        let job_results = self.directory.path().join("jobs.json");
        fs::write(&event, serde_json::to_vec(&self.event)?)?;
        fs::write(&job_results, serde_json::to_vec(&self.jobs)?)?;
        Ok(RecordValidationArgs {
            output: self.directory.path().join("cli-validation.json"),
            event,
            job_results,
            source_commit: self.source.clone(),
            run_id: NonZeroU64::new(701).unwrap(),
            run_attempt: NonZeroU64::new(3).unwrap(),
        })
    }

    fn assert_rejected(&self) -> TestResult {
        let args = self.arguments()?;
        let output = args.output.clone();
        assert!(args.write(&self.repository).is_err());
        assert!(!output.exists());
        Ok(())
    }
}

#[test]
fn record_binds_the_tested_tree_and_pr_head_to_both_artifacts() -> TestResult {
    let fixture = Fixture::new()?;
    let args = fixture.arguments()?;
    let path = args.output.clone();
    args.write(&fixture.repository)?;
    let contents = fs::read(&path)?;
    let record: Value = serde_json::from_slice(&contents)?;
    assert_eq!(
        record,
        json!({
            "schema_version": 1, "repository": REPOSITORY, "pull_request": 123,
            "pull_request_head_sha": fixture.head, "source_commit": fixture.source,
            "source_tree": fixture.tree, "version": "1.2.3", "run_id": 701, "run_attempt": 3,
            "binary_artifact": {"id": 101, "manifest_sha256": "a".repeat(64)},
            "image_artifact": {"id": 102, "manifest_sha256": "b".repeat(64)},
        })
    );
    assert!(fixture.arguments()?.write(&fixture.repository).is_err());
    assert_eq!(fs::read(&path)?, contents);
    Ok(())
}

#[test]
fn untrusted_events_and_different_pr_revisions_never_write_a_record() -> TestResult {
    let mut fixture = Fixture::new()?;
    let original = fixture.event.clone();
    for (pointer, invalid) in [
        ("/repository/full_name", json!("fork/delta-arrow-reader")),
        (
            "/pull_request/head/repo/full_name",
            json!("fork/delta-arrow-reader"),
        ),
        (
            "/pull_request/base/repo/full_name",
            json!("fork/delta-arrow-reader"),
        ),
        ("/pull_request/head/ref", json!("feature/example")),
        ("/pull_request/base/ref", json!("develop")),
        ("/pull_request/user/login", json!("someone-else")),
        ("/pull_request/head/sha", json!("c".repeat(40))),
        ("/pull_request/base/sha", json!("d".repeat(40))),
        ("/pull_request/number", json!(0)),
        ("/number", json!(124)),
    ] {
        fixture.event = original.clone();
        *fixture.event.pointer_mut(pointer).unwrap() = invalid;
        fixture.assert_rejected()?;
    }
    fixture.event = original;
    let mut args = fixture.arguments()?;
    args.source_commit = fixture.head.clone();
    assert!(args.write(&fixture.repository).is_err());
    fs::write(fixture.repository.join("untracked"), "dirty source")?;
    fixture.assert_rejected()?;
    Ok(())
}

#[test]
fn missing_approval_or_validation_and_mixed_artifact_identities_are_rejected() -> TestResult {
    let mut fixture = Fixture::new()?;
    let original = fixture.jobs.clone();
    for job in [
        "changes",
        "release-approval",
        "release-cli",
        "release-cli-images",
    ] {
        for status in ["failure", "cancelled", "skipped", ""] {
            fixture.jobs = original.clone();
            fixture.jobs[job]["result"] = status.into();
            fixture.assert_rejected()?;
        }
        fixture.jobs = original.clone();
        fixture.jobs.as_object_mut().unwrap().remove(job);
        fixture.assert_rejected()?;
    }
    for job in ["release-cli", "release-cli-images"] {
        for (field, invalid) in [
            ("validated", json!("false")),
            ("validated", json!(true)),
            ("source-commit", json!("c".repeat(40))),
            ("version", json!("1.2.4")),
            ("rust-toolchain", json!("1.95.0")),
            ("artifact-id", json!("0")),
            ("artifact-id", json!("1,2")),
            ("artifact-id", json!("")),
            ("manifest-sha256", json!("")),
            ("manifest-sha256", json!("A".repeat(64))),
        ] {
            fixture.jobs = original.clone();
            fixture.jobs[job]["outputs"][field] = invalid;
            fixture.assert_rejected()?;
        }
        for field in original[job]["outputs"].as_object().unwrap().keys() {
            fixture.jobs = original.clone();
            fixture.jobs[job]["outputs"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            fixture.assert_rejected()?;
        }
    }
    for (field, invalid) in [
        ("release_pr", "false"),
        ("package_version", "1.2.4"),
        ("pr_head_sha", "incorrect"),
    ] {
        fixture.jobs = original.clone();
        fixture.jobs["changes"]["outputs"][field] = invalid.into();
        fixture.assert_rejected()?;
    }
    fixture.jobs = original;
    fixture.jobs["release-cli-images"]["outputs"]["artifact-id"] = json!("101");
    fixture.assert_rejected()?;
    Ok(())
}
