pub(crate) mod github;
mod provenance;

use std::{env, num::NonZeroU64, path::PathBuf};

use clap::Args;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::write_new_file,
    build::{is_full_commit_sha, is_stable_version},
    command::command_output,
    error::{Error, FileSnafu, InvalidInputSnafu, JsonSnafu, Result, TomlSnafu},
    repository::WorkspaceManifest,
    validation_record::REPOSITORY,
};
use github::{
    ArtifactPage, GitReference, PullRequest, Release, Repository, RunPage, Workflow, WorkflowRun,
};
use provenance::ValidationArtifacts;

#[derive(Args)]
pub(crate) struct SelectReleaseArtifactsArgs {
    /// New JSON report of the release identity, validation record, and artifact metadata.
    output: PathBuf,
    #[command(flatten)]
    selection: ReleaseSelectionArgs,
}

#[derive(Args)]
pub(crate) struct ReleaseSelectionArgs {
    /// Existing published stable release tag, such as v1.2.3.
    #[arg(long)]
    pub(crate) tag: String,
    /// Select this validation run explicitly; requires --record-artifact-id.
    #[arg(long, requires = "record_artifact_id")]
    pub(crate) run_id: Option<NonZeroU64>,
    /// Select this immutable record artifact explicitly; requires --run-id.
    #[arg(long, requires = "run_id")]
    pub(crate) record_artifact_id: Option<NonZeroU64>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseIdentity {
    pub(crate) id: NonZeroU64,
    pub(crate) tag: String,
    pub(crate) commit: String,
    pub(crate) tree: String,
    pub(crate) version: String,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct SelectedArtifacts {
    schema_version: u32,
    repository: String,
    pub(crate) release: ReleaseIdentity,
    #[serde(flatten)]
    pub(crate) artifacts: ValidationArtifacts,
}

impl SelectReleaseArtifactsArgs {
    pub(crate) fn run(self) -> Result<()> {
        ensure!(
            !self
                .output
                .try_exists()
                .context(FileSnafu { path: &self.output })?,
            InvalidInputSnafu {
                message: "release selection output already exists"
            }
        );
        let selected = self.selection.select()?;
        selected.release.ensure_unchanged()?;
        let mut bytes = serde_json::to_vec_pretty(&selected).context(JsonSnafu {
            subject: "CLI release selection",
        })?;
        bytes.push(b'\n');
        write_new_file(&self.output, &bytes)?;
        println!(
            "Selected CLI validation: release {}, PR #{}, run {}, record artifact {}",
            selected.release.tag,
            selected.artifacts.validation_record.pull_request,
            selected.artifacts.validation_record.run_id,
            selected.artifacts.record_artifact.id
        );
        Ok(())
    }
}

impl ReleaseSelectionArgs {
    pub(crate) fn select(&self) -> Result<SelectedArtifacts> {
        ensure!(
            env::var_os("GITHUB_REPOSITORY").is_none_or(|name| name == REPOSITORY),
            InvalidInputSnafu {
                message: "CLI release selection requires the upstream repository"
            }
        );
        let release = resolve_release(&self.tag)?;
        let repository: Repository = github::get("")?;
        ensure!(
            repository.full_name == REPOSITORY,
            InvalidInputSnafu {
                message: "unexpected GitHub repository"
            }
        );
        let pulls = github::pages::<Vec<PullRequest>>(&format!(
            "commits/{}/pulls?per_page=100",
            release.commit
        ))?;
        let mut pulls = pulls
            .into_iter()
            .flatten()
            .filter(|pr| trusted_release_pr(pr, &release, &repository));
        let pr = pulls.next().ok_or_else(|| {
            InvalidInputSnafu {
                message: "release commit has no merged upstream release-plz PR",
            }
            .build()
        })?;
        ensure!(
            pulls.next().is_none(),
            InvalidInputSnafu {
                message: "release commit has multiple merged release-plz PRs"
            }
        );
        let workflow: Workflow = github::get("actions/workflows/ci.yml")?;
        ensure!(
            workflow.path == ".github/workflows/ci.yml",
            InvalidInputSnafu {
                message: "unexpected validation workflow"
            }
        );

        let runs = if let Some(id) = self.run_id {
            let run: WorkflowRun = github::get(&format!("actions/runs/{id}"))?;
            ensure!(
                run.id == id,
                InvalidInputSnafu {
                    message: "GitHub returned a different validation run"
                }
            );
            vec![run]
        } else {
            let pages = github::pages::<RunPage>(&format!(
                "actions/workflows/ci.yml/runs?event=pull_request&head_sha={}&status=success&per_page=100",
                pr.head.sha
            ))?;
            ensure!(
                pages.iter().all(|page| page.total_count < 1000),
                InvalidInputSnafu {
                    message: "validation run search reached GitHub's limit; specify --run-id and --record-artifact-id",
                }
            );
            pages
                .into_iter()
                .flat_map(|page| page.workflow_runs)
                .collect()
        };
        let mut candidates = Vec::new();
        for run in runs {
            provenance::check_run(&run, &pr, &repository, &workflow)?;
            let artifacts: Vec<_> = github::pages::<ArtifactPage>(&format!(
                "actions/runs/{}/artifacts?per_page=100",
                run.id
            ))?
            .into_iter()
            .flat_map(|page| page.artifacts)
            .collect();
            let records: Vec<_> = artifacts
                .iter()
                .filter(|artifact| {
                    self.record_artifact_id.map_or_else(
                        || artifact.name.starts_with("dar-cli-validation-"),
                        |id| artifact.id == id,
                    )
                })
                .collect();
            if self.record_artifact_id.is_some() {
                ensure!(
                    records.len() == 1,
                    InvalidInputSnafu {
                        message: "selected record artifact is missing from the validation run"
                    }
                );
            }
            for artifact in records {
                let result = provenance::verify_record(
                    artifact,
                    &artifacts,
                    &run,
                    &pr,
                    &repository,
                    &release,
                );
                match result {
                    Ok(candidate) => candidates.push(candidate),
                    Err(error) => {
                        eprintln!(
                            "Rejected CLI validation run {}, artifact {}: {error}",
                            run.id, artifact.id
                        );
                        // An unavailable API response cannot prove that another candidate is ineligible.
                        if self.record_artifact_id.is_some()
                            || !matches!(error, Error::IneligibleReleaseValidation { .. })
                        {
                            return Err(error);
                        }
                    }
                }
            }
        }
        ensure!(
            !candidates.is_empty(),
            InvalidInputSnafu {
                message: "no eligible CLI validation record for the merged release PR; missing or expired artifacts cannot be rebuilt by selection",
            }
        );
        ensure!(
            candidates.len() == 1,
            InvalidInputSnafu {
                message: format!(
                    "multiple eligible CLI validation records: {}; specify --run-id and --record-artifact-id",
                    candidates
                        .iter()
                        .map(|candidate| format!(
                            "run {}, record artifact {}",
                            candidate.validation_record.run_id, candidate.record_artifact.id
                        ))
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
            }
        );
        Ok(SelectedArtifacts {
            schema_version: 1,
            repository: REPOSITORY.into(),
            release,
            artifacts: candidates.pop().unwrap(),
        })
    }
}

impl ReleaseIdentity {
    pub(crate) fn require_publication_context(&self) -> Result<()> {
        if env::var("GITHUB_ACTIONS").as_deref() != Ok("true") {
            return Ok(());
        }
        let reference = match env::var("GITHUB_EVENT_NAME").as_deref() {
            Ok("release") => {
                ensure!(
                    env::var("GITHUB_SHA").as_deref() == Ok(self.commit.as_str()),
                    InvalidInputSnafu {
                        message: "release event commit differs from the verified release"
                    }
                );
                format!("refs/tags/{}", self.tag)
            }
            Ok("workflow_dispatch") => "refs/heads/main".into(),
            _ => return InvalidInputSnafu {
                message: "CLI publication in Actions requires a release event or recovery dispatch",
            }
            .fail(),
        };
        ensure!(
            env::var("GITHUB_REPOSITORY").as_deref() == Ok(REPOSITORY)
                && env::var("GITHUB_REF").as_deref() == Ok(reference.as_str())
                && env::var("GITHUB_WORKFLOW_REF").as_deref()
                    == Ok(
                        format!("{REPOSITORY}/.github/workflows/cli-release.yml@{reference}")
                            .as_str()
                    ),
            InvalidInputSnafu {
                message: "CLI publication requires the trusted upstream release workflow and ref"
            }
        );
        Ok(())
    }

    pub(crate) fn ensure_unchanged(&self) -> Result<()> {
        let release = stable_release(&self.tag)?;
        ensure!(
            release.id == self.id && tag_commit(&self.tag)? == self.commit,
            InvalidInputSnafu {
                message: "release or tag changed during artifact selection"
            }
        );
        Ok(())
    }
}

impl SelectedArtifacts {
    pub(crate) fn revalidate(&self) -> Result<()> {
        let current = ReleaseSelectionArgs {
            tag: self.release.tag.clone(),
            run_id: Some(self.artifacts.validation_record.run_id),
            record_artifact_id: Some(self.artifacts.record_artifact.id),
        }
        .select()?;
        ensure!(
            serde_json::to_value(self).context(JsonSnafu {
                subject: "saved release selection"
            })? == serde_json::to_value(current).context(JsonSnafu {
                subject: "current release selection"
            })?,
            InvalidInputSnafu {
                message: "release plan provenance differs from current GitHub validation"
            }
        );
        Ok(())
    }
}

fn stable_release(tag: &str) -> Result<Release> {
    ensure!(
        tag.strip_prefix('v').is_some_and(is_stable_version),
        InvalidInputSnafu {
            message: "CLI releases require a stable vX.Y.Z tag"
        }
    );
    let release: Release = github::get(&format!("releases/tags/{tag}"))?;
    ensure!(
        release.tag_name == tag
            && !release.draft
            && !release.prerelease
            && release
                .published_at
                .as_ref()
                .is_some_and(|date| !date.is_empty()),
        InvalidInputSnafu {
            message: "CLI publication requires the matching published stable release"
        }
    );
    Ok(release)
}

fn tag_commit(tag: &str) -> Result<String> {
    let mut reference: GitReference = github::get(&format!("git/ref/tags/{tag}"))?;
    for _ in 0..16 {
        ensure!(
            is_full_commit_sha(&reference.object.sha),
            InvalidInputSnafu {
                message: "release tag has an invalid object SHA"
            }
        );
        match reference.object.r#type.as_str() {
            "commit" => return Ok(reference.object.sha),
            "tag" => reference = github::get(&format!("git/tags/{}", reference.object.sha))?,
            _ => {
                return InvalidInputSnafu {
                    message: "release tag does not resolve to a commit",
                }
                .fail();
            }
        }
    }
    InvalidInputSnafu {
        message: "release tag nesting exceeds 16 objects",
    }
    .fail()
}

fn resolve_release(tag: &str) -> Result<ReleaseIdentity> {
    #[derive(Deserialize)]
    struct Commit {
        sha: String,
        tree: ObjectSha,
    }
    #[derive(Deserialize)]
    struct ObjectSha {
        sha: String,
    }
    #[derive(Deserialize)]
    struct Comparison {
        status: String,
        base_commit: ObjectSha,
        merge_base_commit: ObjectSha,
    }

    let release = stable_release(tag)?;
    let commit = tag_commit(tag)?;
    let object: Commit = github::get(&format!("git/commits/{commit}"))?;
    ensure!(
        object.sha == commit && is_full_commit_sha(&object.tree.sha),
        InvalidInputSnafu {
            message: "release commit or source tree differs from GitHub metadata"
        }
    );
    let comparison: Comparison = github::get(&format!("compare/{commit}...main"))?;
    ensure!(
        matches!(comparison.status.as_str(), "ahead" | "identical")
            && comparison.base_commit.sha == commit
            && comparison.merge_base_commit.sha == commit,
        InvalidInputSnafu {
            message: "release commit is not reachable from upstream main"
        }
    );
    let manifest: WorkspaceManifest = read_manifest("Cargo.toml", &commit)?;
    let version = manifest.workspace.package.version;
    ensure!(
        tag == format!("v{version}"),
        InvalidInputSnafu {
            message: "release tag and workspace version differ"
        }
    );
    let cli: toml::Value = read_manifest("crates/delta-arrow-reader-cli/Cargo.toml", &commit)?;
    ensure!(
        cli.get("package")
            .and_then(|package| package.get("name"))
            .and_then(toml::Value::as_str)
            == Some("delta-arrow-reader-cli"),
        InvalidInputSnafu {
            message: "release commit does not contain the CLI package"
        }
    );
    Ok(ReleaseIdentity {
        id: release.id,
        tag: tag.into(),
        commit,
        tree: object.tree.sha,
        version,
    })
}

fn read_manifest<T: serde::de::DeserializeOwned>(path: &str, commit: &str) -> Result<T> {
    let text = command_output(
        github::request(&format!("contents/{path}?ref={commit}"))
            .args(["-H", "Accept: application/vnd.github.raw+json"]),
    )?;
    toml::from_str(&text).context(TomlSnafu { path })
}

fn trusted_release_pr(
    pr: &PullRequest,
    release: &ReleaseIdentity,
    repository: &Repository,
) -> bool {
    pr.merged_at.as_ref().is_some_and(|date| !date.is_empty())
        && pr.merge_commit_sha.as_deref() == Some(&release.commit)
        && pr.user.login == "release-plz-bot-mag1cfrog[bot]"
        && pr.head.r#ref.starts_with("release-plz-")
        && is_full_commit_sha(&pr.head.sha)
        && pr.base.r#ref == "main"
        && [&pr.head, &pr.base].into_iter().all(|branch| {
            branch
                .repo
                .as_ref()
                .is_some_and(|repo| repo.id == repository.id && repo.full_name == REPOSITORY)
        })
}
