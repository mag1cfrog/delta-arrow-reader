use std::num::NonZeroU64;

use serde::Serialize;
use snafu::ensure;

use crate::{
    build::is_full_commit_sha,
    error::{IneligibleReleaseValidationSnafu, Result},
    validation_record::{REPOSITORY, ValidationRecord},
};

use super::{
    ReleaseIdentity,
    github::{
        self, Artifact, Environment, Job, JobPage, PullRequest, Repository, Review, Workflow,
        WorkflowRun,
    },
};

const APPROVAL_JOB: &str = "Approve release checks";
const BINARY_JOB: &str = "Release CLI binaries / Validate complete CLI artifact set";
const IMAGE_JOB: &str = "Release CLI images and Go consumer / Validate complete CLI image set";
const RECORD_JOB: &str = "Record validated CLI artifacts";

#[derive(Serialize)]
pub(crate) struct ValidationArtifacts {
    pub validation_record: ValidationRecord,
    pub record_artifact: Artifact,
    pub binary_artifact: Artifact,
    pub image_artifact: Artifact,
}

pub(super) fn check_run(
    run: &WorkflowRun,
    pr: &PullRequest,
    repository: &Repository,
    workflow: &Workflow,
) -> Result<()> {
    ensure!(
        run.workflow_id == workflow.id
            && run.path == workflow.path
            && run.event == "pull_request"
            && run.status == "completed"
            && run.conclusion.as_deref() == Some("success")
            && run.head_sha == pr.head.sha
            && run.head_branch == pr.head.r#ref
            && [&run.repository, &run.head_repository]
                .into_iter()
                .all(|repo| repo.id == repository.id && repo.full_name == REPOSITORY),
        IneligibleReleaseValidationSnafu {
            message: format!(
                "run {} is not successful upstream CI for the merged PR head",
                run.id
            )
        }
    );
    Ok(())
}

pub(super) fn verify_record(
    artifact: &Artifact,
    artifacts: &[Artifact],
    run: &WorkflowRun,
    pr: &PullRequest,
    repository: &Repository,
    release: &ReleaseIdentity,
) -> Result<ValidationArtifacts> {
    check_artifact(artifact, run, repository)?;
    let record = github::download_record(artifact)?;
    ensure!(
        record.schema_version == 1
            && record.repository == REPOSITORY
            && record.pull_request == pr.number.get()
            && record.pull_request_head_sha == pr.head.sha
            && record.run_id == run.id
            && record.run_attempt <= run.run_attempt
            && is_full_commit_sha(&record.source_commit)
            && record.source_tree == release.tree
            && record.version == release.version
            && github::is_sha256(&record.binary_artifact.manifest_sha256)
            && github::is_sha256(&record.image_artifact.manifest_sha256)
            && record.binary_artifact.id != record.image_artifact.id
            && record.binary_artifact.id != artifact.id
            && record.image_artifact.id != artifact.id
            && artifact.name
                == format!(
                    "dar-cli-validation-{}-{}-{}",
                    record.source_commit, run.id, record.run_attempt
                ),
        IneligibleReleaseValidationSnafu {
            message: "validation record differs from release tree/version, PR revision, run, or artifact identity"
        }
    );

    // A partial rerun carries forward successful jobs from earlier attempts of this same PR revision.
    let jobs: Vec<_> = github::pages::<JobPage>(&format!(
        "actions/runs/{}/jobs?filter=all&per_page=100",
        run.id
    ))?
    .into_iter()
    .flat_map(|page| page.jobs)
    .collect();
    successful_job(&jobs, "Detect Changes", run)?;
    let approval = successful_job(&jobs, APPROVAL_JOB, run)?;
    let binaries = successful_job(&jobs, BINARY_JOB, run)?;
    let images = successful_job(&jobs, IMAGE_JOB, run)?;
    let recorder = successful_job(&jobs, RECORD_JOB, run)?;
    successful_job(&jobs, "CI", run)?;
    ensure!(
        recorder.run_attempt == record.run_attempt
            && binaries.run_attempt <= record.run_attempt
            && images.run_attempt <= record.run_attempt
            && approval.run_attempt <= binaries.run_attempt
            && approval.run_attempt <= images.run_attempt,
        IneligibleReleaseValidationSnafu {
            message: "record or producer artifacts do not match the successful approved job attempts"
        }
    );
    let environment: Environment = github::get("environments/release-checks")?;
    let reviews: Vec<Review> = github::get(&format!("actions/runs/{}/approvals", run.id))?;
    // Review history is run-scoped; the successful environment-gated job binds approval to this revision.
    ensure!(
        environment.name == "release-checks"
            && reviews.iter().any(|review| review.state == "approved"
                && !review.user.login.is_empty()
                && review
                    .environments
                    .iter()
                    .any(|env| env.name == environment.name && env.id == environment.id)),
        IneligibleReleaseValidationSnafu {
            message: "validation run has no release-checks environment approval"
        }
    );
    let binary_artifact = complete_artifact(
        artifacts,
        record.binary_artifact.id,
        "dar-cli",
        binaries.run_attempt,
        &record,
        run,
        repository,
    )?;
    let image_artifact = complete_artifact(
        artifacts,
        record.image_artifact.id,
        "dar-cli-images",
        images.run_attempt,
        &record,
        run,
        repository,
    )?;
    Ok(ValidationArtifacts {
        validation_record: record,
        record_artifact: artifact.clone(),
        binary_artifact,
        image_artifact,
    })
}

fn successful_job<'a>(jobs: &'a [Job], name: &str, run: &WorkflowRun) -> Result<&'a Job> {
    let mut matching: Vec<_> = jobs
        .iter()
        .filter(|job| job.name == name && job.run_attempt <= run.run_attempt)
        .collect();
    matching.sort_by_key(|job| job.run_attempt);
    let job = matching.pop().ok_or_else(|| {
        IneligibleReleaseValidationSnafu {
            message: format!("validation run {} is missing job {name:?}", run.id),
        }
        .build()
    })?;
    ensure!(
        matching
            .last()
            .is_none_or(|other| other.run_attempt != job.run_attempt)
            && job.run_id == run.id
            && job.head_sha == run.head_sha
            && job.status == "completed"
            && job.conclusion.as_deref() == Some("success"),
        IneligibleReleaseValidationSnafu {
            message: format!(
                "validation job {name:?} ({}) is ambiguous, unsuccessful, or belongs to another revision",
                job.id
            )
        }
    );
    Ok(job)
}

fn complete_artifact(
    artifacts: &[Artifact],
    id: NonZeroU64,
    prefix: &str,
    attempt: NonZeroU64,
    record: &ValidationRecord,
    run: &WorkflowRun,
    repository: &Repository,
) -> Result<Artifact> {
    let mut matching = artifacts.iter().filter(|artifact| artifact.id == id);
    let artifact = matching.next().ok_or_else(|| {
        IneligibleReleaseValidationSnafu {
            message: format!("complete CLI artifact {id} is missing from run {}", run.id),
        }
        .build()
    })?;
    ensure!(
        matching.next().is_none()
            && artifact.name == format!("{prefix}-{}-{}-{attempt}", record.source_commit, run.id),
        IneligibleReleaseValidationSnafu {
            message: format!(
                "complete CLI artifact {id} has a different source, run, or producer attempt"
            )
        }
    );
    check_artifact(artifact, run, repository)?;
    Ok(artifact.clone())
}

fn check_artifact(artifact: &Artifact, run: &WorkflowRun, repository: &Repository) -> Result<()> {
    ensure!(
        !artifact.expired
            && artifact.size_in_bytes > 0
            && artifact
                .digest
                .strip_prefix("sha256:")
                .is_some_and(github::is_sha256)
            && artifact.workflow_run.id == run.id
            && artifact.workflow_run.repository_id == repository.id
            && artifact.workflow_run.head_repository_id == repository.id
            && artifact.workflow_run.head_sha == run.head_sha
            && artifact.workflow_run.head_branch == run.head_branch,
        IneligibleReleaseValidationSnafu {
            message: format!(
                "CLI artifact {} is expired, has no SHA256, or belongs to another run/repository/revision",
                artifact.id
            )
        }
    );
    Ok(())
}
