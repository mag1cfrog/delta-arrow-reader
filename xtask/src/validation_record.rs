use std::{
    fs,
    num::NonZeroU64,
    path::{Path, PathBuf},
    process::Command,
};

use clap::Args;
use serde::Serialize;
use serde_json::Value;
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::write_new_file,
    build::{BuildIdentity, check_source, is_full_commit_sha},
    command::command_output,
    error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
    repository::{WorkspaceManifest, read_toml, repository_root},
};

const REPOSITORY: &str = "mag1cfrog/delta-arrow-reader";

#[derive(Args)]
pub(crate) struct RecordValidationArgs {
    /// New cli-validation.json file to write after validating every input.
    output: PathBuf,
    /// GitHub pull_request event payload for this CI run.
    #[arg(long)]
    event: PathBuf,
    /// JSON serialization of the handoff job's GitHub Actions needs context.
    #[arg(long)]
    job_results: PathBuf,
    /// Full tested merge candidate SHA from github.sha.
    #[arg(long)]
    source_commit: String,
    #[arg(long)]
    run_id: NonZeroU64,
    #[arg(long)]
    run_attempt: NonZeroU64,
}

#[derive(Debug, Serialize)]
struct ValidationRecord {
    schema_version: u32,
    repository: &'static str,
    pull_request: u64,
    pull_request_head_sha: String,
    source_commit: String,
    source_tree: String,
    version: String,
    run_id: NonZeroU64,
    run_attempt: NonZeroU64,
    binary_artifact: ArtifactReference,
    image_artifact: ArtifactReference,
}

#[derive(Debug, Serialize)]
struct ArtifactReference {
    id: NonZeroU64,
    manifest_sha256: String,
}

impl RecordValidationArgs {
    pub(crate) fn run(self) -> Result<()> {
        self.write(&repository_root())
    }

    fn write(self, repository: &Path) -> Result<()> {
        let record = self.resolve(repository)?;
        let mut bytes = serde_json::to_vec_pretty(&record).context(JsonSnafu {
            subject: "CLI validation record",
        })?;
        bytes.push(b'\n');
        write_new_file(&self.output, &bytes)?;
        println!(
            "Recorded CLI validation for release PR #{}",
            record.pull_request
        );
        Ok(())
    }

    fn resolve(&self, repository: &Path) -> Result<ValidationRecord> {
        check_source(repository, &self.source_commit)?;
        let read_json = |path: &Path| -> Result<Value> {
            serde_json::from_slice(&fs::read(path).context(FileSnafu { path })?).context(
                JsonSnafu {
                    subject: "release PR validation inputs",
                },
            )
        };
        let event = read_json(&self.event)?;
        let pr = &event["pull_request"];
        let number = pr["number"].as_u64().filter(|number| *number > 0);
        let head = pr["head"]["sha"].as_str().unwrap_or_default();
        let base = pr["base"]["sha"].as_str().unwrap_or_default();
        ensure!(
            event["repository"]["full_name"] == REPOSITORY
                && pr["head"]["repo"]["full_name"] == REPOSITORY
                && pr["base"]["repo"]["full_name"] == REPOSITORY
                && pr["base"]["ref"] == "main"
                && pr["user"]["login"] == "release-plz-bot-mag1cfrog[bot]"
                && pr["head"]["ref"]
                    .as_str()
                    .is_some_and(|name| name.starts_with("release-plz-"))
                && number.is_some()
                && event["number"].as_u64() == number
                && is_full_commit_sha(head)
                && is_full_commit_sha(base),
            InvalidInputSnafu {
                message: "validation requires the upstream release-plz PR event"
            }
        );
        let parents = command_output(
            Command::new("git")
                .args(["show", "-s", "--format=%P", "HEAD"])
                .current_dir(repository),
        )?;
        ensure!(
            parents.split_whitespace().collect::<Vec<_>>() == [base, head],
            InvalidInputSnafu {
                message: "tested source must merge the event's base and PR head revisions"
            }
        );
        let source_tree = command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD^{tree}"])
                .current_dir(repository),
        )?
        .trim()
        .to_owned();
        ensure!(
            is_full_commit_sha(&source_tree),
            InvalidInputSnafu {
                message: "invalid tested source tree"
            }
        );
        let manifest: WorkspaceManifest = read_toml(&repository.join("Cargo.toml"))?;
        let jobs = read_json(&self.job_results)?;
        for name in [
            "changes",
            "release-approval",
            "release-cli",
            "release-cli-images",
        ] {
            ensure!(
                jobs[name]["result"] == "success",
                InvalidInputSnafu {
                    message: format!("CLI validation prerequisite {name} did not succeed"),
                }
            );
        }
        ensure!(
            jobs["changes"]["outputs"]["release_pr"] == "true"
                && jobs["changes"]["outputs"]["pr_head_sha"] == head
                && jobs["changes"]["outputs"]["package_version"]
                    == manifest.workspace.package.version,
            InvalidInputSnafu {
                message: "release PR classification, head, or workspace version differs"
            }
        );
        let identity = BuildIdentity {
            source_commit: self.source_commit.clone(),
            version: manifest.workspace.package.version,
            rust_toolchain: jobs["release-cli"]["outputs"]["rust-toolchain"]
                .as_str()
                .unwrap_or_default()
                .into(),
        };
        identity.validate()?;
        let binary_artifact = artifact_reference(&jobs["release-cli"]["outputs"], &identity)?;
        let image_artifact = artifact_reference(&jobs["release-cli-images"]["outputs"], &identity)?;
        ensure!(
            binary_artifact.id != image_artifact.id,
            InvalidInputSnafu {
                message: "binary and image artifact IDs must be distinct",
            }
        );
        Ok(ValidationRecord {
            schema_version: 1,
            repository: REPOSITORY,
            pull_request: number.unwrap(),
            pull_request_head_sha: head.into(),
            source_commit: identity.source_commit,
            source_tree,
            version: identity.version,
            run_id: self.run_id,
            run_attempt: self.run_attempt,
            binary_artifact,
            image_artifact,
        })
    }
}

fn artifact_reference(outputs: &Value, identity: &BuildIdentity) -> Result<ArtifactReference> {
    let id = outputs["artifact-id"]
        .as_str()
        .and_then(|id| id.parse::<NonZeroU64>().ok());
    let digest = outputs["manifest-sha256"].as_str().unwrap_or_default();
    ensure!(
        outputs["validated"] == "true"
            && outputs["source-commit"] == identity.source_commit
            && outputs["version"] == identity.version
            && outputs["rust-toolchain"] == identity.rust_toolchain
            && id.is_some()
            && digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        InvalidInputSnafu {
            message: "validated artifact ID, manifest SHA256, or build identity is missing or differs"
        }
    );
    Ok(ArtifactReference {
        id: id.unwrap(),
        manifest_sha256: digest.into(),
    })
}

#[cfg(test)]
mod tests;
