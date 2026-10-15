use std::{
    collections::BTreeSet,
    env, fs,
    io::{self, Read, Write},
    path::Path,
};

use flate2::read::DeflateDecoder;
use rawzip::{CompressionMethod, ZipArchive};
use serde::{Deserialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    Result,
    python_wheels::{ARTIFACT_PLATFORM_TAGS, check_wheel_artifacts},
    release_selection::release_commit_matches_pr,
    run,
};

const REPOSITORY: &str = "mag1cfrog/delta-arrow-reader";

#[derive(Deserialize)]
struct Repository {
    full_name: String,
}

#[derive(Deserialize)]
struct PullRequestBranch {
    r#ref: String,
    sha: String,
    repo: Option<Repository>,
}

#[derive(Deserialize)]
struct Author {
    login: String,
}

#[derive(Deserialize)]
struct PullRequest {
    number: u64,
    merged_at: Option<String>,
    merge_commit_sha: Option<String>,
    user: Author,
    head: PullRequestBranch,
    base: PullRequestBranch,
}

#[derive(Deserialize)]
struct RunPage {
    workflow_runs: Vec<WorkflowRun>,
}

#[derive(Deserialize)]
struct WorkflowRun {
    id: u64,
    run_attempt: u64,
    event: String,
    path: String,
    status: String,
    conclusion: Option<String>,
    head_sha: String,
    head_branch: String,
    repository: Repository,
    head_repository: Repository,
}

#[derive(Deserialize)]
struct ArtifactPage {
    artifacts: Vec<Artifact>,
}

#[derive(Deserialize)]
struct Artifact {
    id: u64,
    name: String,
    expired: bool,
    digest: String,
}

fn api_pages<T: DeserializeOwned>(endpoint: &str) -> Result<Vec<T>> {
    Ok(serde_json::from_slice(&run(
        "gh",
        &[
            "api",
            "--hostname",
            "github.com",
            "--paginate",
            "--slurp",
            &format!("repos/{REPOSITORY}/{endpoint}"),
        ],
    )?)?)
}

pub fn reuse_release_wheels(destination: &Path, version: &str) -> Result<()> {
    if env::var("GITHUB_REPOSITORY")? != REPOSITORY {
        return Err("release wheels must come from the upstream repository".into());
    }
    let commit = String::from_utf8(run("git", &["rev-parse", "HEAD"])?)?;
    let commit = commit.trim();
    let tree = String::from_utf8(run("git", &["rev-parse", "HEAD^{tree}"])?)?;
    let tree = tree.trim();
    let pulls = api_pages::<Vec<PullRequest>>(&format!("commits/{commit}/pulls?per_page=100"))?;
    let mut release_prs = Vec::new();
    for pr in pulls.into_iter().flatten().filter(|pr| {
        pr.merged_at.is_some()
            && pr.user.login == "release-plz-bot-mag1cfrog[bot]"
            && pr.head.r#ref.starts_with("release-plz-")
            && pr.base.r#ref == "main"
            && pr
                .head
                .repo
                .as_ref()
                .is_some_and(|repo| repo.full_name == REPOSITORY)
            && pr
                .base
                .repo
                .as_ref()
                .is_some_and(|repo| repo.full_name == REPOSITORY)
    }) {
        if release_commit_matches_pr(commit, tree, &pr.head.sha, pr.merge_commit_sha.as_deref())? {
            release_prs.push(pr);
        }
    }
    let [pr] = release_prs.as_slice() else {
        return Err("release commit must belong to exactly one merged release-plz PR".into());
    };

    // GitHub can clear a run's pull_requests list after the head branch is deleted.
    let ci_run = api_pages::<RunPage>(&format!(
        "actions/workflows/ci.yml/runs?event=pull_request&head_sha={}&per_page=100",
        pr.head.sha
    ))?
    .into_iter()
    .flat_map(|page| page.workflow_runs)
    .max_by_key(|ci_run| ci_run.id)
    .ok_or("release PR has no CI run")?;
    if ci_run.id == 0
        || ci_run.run_attempt == 0
        || ci_run.event != "pull_request"
        || ci_run.path != ".github/workflows/ci.yml"
        || ci_run.status != "completed"
        || ci_run.conclusion.as_deref() != Some("success")
        || ci_run.head_sha != pr.head.sha
        || ci_run.head_branch != pr.head.r#ref
        || ci_run.repository.full_name != REPOSITORY
        || ci_run.head_repository.full_name != REPOSITORY
    {
        return Err("latest release PR CI run must have succeeded for the merged PR head".into());
    }

    let prefix = format!("python-wheels-{tree}-{}-", ci_run.id);
    let mut bundles = Vec::new();
    for artifact in api_pages::<ArtifactPage>(&format!(
        "actions/runs/{}/artifacts?per_page=100",
        ci_run.id
    ))?
    .into_iter()
    .flat_map(|page| page.artifacts)
    {
        if let Some(attempt) = artifact.name.strip_prefix(&prefix) {
            let attempt: u64 = attempt.parse()?;
            if attempt == 0 || attempt > ci_run.run_attempt {
                return Err("wheel artifact has an invalid CI run attempt".into());
            }
            bundles.push((attempt, artifact));
        }
    }
    bundles.sort_by_key(|(attempt, _)| *attempt);
    let (attempt, artifact) = bundles
        .pop()
        .ok_or("no release PR wheel bundle matches the release source tree")?;
    if bundles
        .last()
        .is_some_and(|(previous, _)| *previous == attempt)
    {
        return Err("multiple wheel bundles claim the same CI run attempt".into());
    }
    if artifact.expired {
        return Err("release PR wheel bundle has expired; publication stopped".into());
    }
    let sha256 = artifact
        .digest
        .strip_prefix("sha256:")
        .ok_or("wheel artifact has no SHA256 digest")?;
    download_wheel_artifact(destination, version, artifact.id, sha256)?;
    let mut output = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(env::var("GITHUB_OUTPUT")?)?;
    writeln!(
        output,
        "artifact-id={}\nartifact-sha256={sha256}",
        artifact.id
    )?;
    println!(
        "Reused wheels from release PR #{}, CI run {}, attempt {attempt}, artifact {}",
        pr.number, ci_run.id, artifact.id
    );
    Ok(())
}

pub fn download_wheel_artifact(
    destination: &Path,
    version: &str,
    artifact_id: u64,
    sha256: &str,
) -> Result<()> {
    if artifact_id == 0
        || sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err("a positive artifact ID and lowercase SHA256 are required".into());
    }
    if destination.try_exists()? {
        return Err("wheel destination must not already exist".into());
    }
    let bytes = run(
        "gh",
        &[
            "api",
            "--hostname",
            "github.com",
            &format!("repos/{REPOSITORY}/actions/artifacts/{artifact_id}/zip"),
        ],
    )?;
    if format!("{:x}", Sha256::digest(&bytes)) != sha256 {
        return Err("wheel artifact SHA256 mismatch".into());
    }
    let archive = ZipArchive::from_slice(bytes)?;
    let mut expected: BTreeSet<_> = ARTIFACT_PLATFORM_TAGS
        .iter()
        .map(|(artifact, tag)| {
            format!("{artifact}/delta_arrow_reader-{version}-cp310-abi3-{tag}.whl")
        })
        .collect();
    if archive.entries_hint() != expected.len() as u64 {
        return Err("wheel bundle must contain exactly three wheels".into());
    }
    let mut files = Vec::new();
    let mut entries = archive.entries();
    while let Some(entry) = entries.next_entry()? {
        let member = entry.file_path();
        let name = member.try_normalize()?;
        if name.as_str().as_bytes() != member.as_bytes() || !expected.remove(name.as_str()) {
            return Err("unexpected or duplicate wheel bundle entry".into());
        }
        let local = archive.get_entry(entry.wayfinder())?;
        let header = local.local_header();
        if header.file_path().as_bytes() != member.as_bytes()
            || header.compression_method() != entry.compression_method()
            || header.flags().is_encrypted()
            || entry.flags().is_encrypted()
        {
            return Err("wheel bundle local header does not match its directory entry".into());
        }
        files.push((name.as_str().to_owned(), entry.compression_method(), local));
    }
    if !expected.is_empty() {
        return Err("wheel bundle is incomplete".into());
    }
    fs::create_dir(destination)?;
    for (name, method, local) in files {
        let reader: Box<dyn Read + '_> = match method {
            CompressionMethod::STORE => Box::new(local.data()),
            CompressionMethod::DEFLATE => Box::new(DeflateDecoder::new(local.data())),
            _ => return Err("unsupported wheel bundle compression".into()),
        };
        let path = destination.join(name);
        fs::create_dir_all(path.parent().ok_or("wheel path has no parent")?)?;
        let mut file = fs::File::create_new(path)?;
        io::copy(&mut local.verifying_reader(reader), &mut file)?;
    }
    check_wheel_artifacts(destination, version)?;
    Ok(())
}
