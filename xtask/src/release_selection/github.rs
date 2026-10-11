use std::{
    fs,
    io::Read,
    num::NonZeroU64,
    process::{Command, Stdio},
};

use flate2::read::DeflateDecoder;
use rawzip::{CompressionMethod, ZipArchive};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use snafu::{ResultExt, ensure};

use crate::{
    command::command_output,
    error::{
        ArtifactZipSnafu, CommandFailedSnafu, CommandStartSnafu, FileSnafu, InvalidInputSnafu,
        IoSnafu, JsonSnafu, Result,
    },
    validation_record::{REPOSITORY, ValidationRecord},
};

pub(super) fn request(endpoint: &str) -> Command {
    let mut command = Command::new("gh");
    command.args([
        "api",
        "--hostname",
        "github.com",
        "--method",
        "GET",
        "-H",
        "X-GitHub-Api-Version: 2022-11-28",
    ]);
    command.arg(format!("repos/{REPOSITORY}/{endpoint}").trim_end_matches('/'));
    command
}

pub(super) fn get<T: DeserializeOwned>(endpoint: &str) -> Result<T> {
    decode(&command_output(&mut request(endpoint))?)
}

pub(super) fn pages<T: DeserializeOwned>(endpoint: &str) -> Result<Vec<T>> {
    decode(&command_output(
        request(endpoint).args(["--paginate", "--slurp"]),
    )?)
}

fn decode<T: DeserializeOwned>(text: &str) -> Result<T> {
    serde_json::from_str(text).context(JsonSnafu {
        subject: "GitHub API response",
    })
}

#[derive(Deserialize)]
pub(super) struct Repository {
    pub id: NonZeroU64,
    pub full_name: String,
}

#[derive(Deserialize)]
pub(super) struct Release {
    pub id: NonZeroU64,
    pub tag_name: String,
    pub draft: bool,
    pub prerelease: bool,
    pub published_at: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct GitObject {
    pub sha: String,
    pub r#type: String,
}

#[derive(Deserialize)]
pub(super) struct GitReference {
    pub object: GitObject,
}

#[derive(Deserialize)]
pub(super) struct PullRequest {
    pub number: NonZeroU64,
    pub merged_at: Option<String>,
    pub merge_commit_sha: Option<String>,
    pub user: User,
    pub head: PullRequestBranch,
    pub base: PullRequestBranch,
}

#[derive(Deserialize)]
pub(super) struct User {
    pub login: String,
}

#[derive(Deserialize)]
pub(super) struct PullRequestBranch {
    pub r#ref: String,
    pub sha: String,
    pub repo: Option<Repository>,
}

#[derive(Deserialize)]
pub(super) struct Workflow {
    pub id: NonZeroU64,
    pub path: String,
}

#[derive(Deserialize)]
pub(super) struct RunPage {
    pub total_count: u64,
    pub workflow_runs: Vec<WorkflowRun>,
}

#[derive(Deserialize)]
pub(super) struct WorkflowRun {
    pub id: NonZeroU64,
    pub run_attempt: NonZeroU64,
    pub workflow_id: NonZeroU64,
    pub event: String,
    pub path: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub head_sha: String,
    pub head_branch: String,
    pub repository: Repository,
    pub head_repository: Repository,
}

#[derive(Deserialize)]
pub(super) struct ArtifactPage {
    pub artifacts: Vec<Artifact>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Artifact {
    pub id: NonZeroU64,
    pub name: String,
    pub size_in_bytes: u64,
    pub expired: bool,
    pub digest: String,
    pub workflow_run: ArtifactRun,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct ArtifactRun {
    pub id: NonZeroU64,
    pub repository_id: NonZeroU64,
    pub head_repository_id: NonZeroU64,
    pub head_sha: String,
    pub head_branch: String,
}

#[derive(Deserialize)]
pub(super) struct JobPage {
    pub jobs: Vec<Job>,
}

#[derive(Deserialize)]
pub(super) struct Job {
    pub id: NonZeroU64,
    pub name: String,
    pub run_id: NonZeroU64,
    pub run_attempt: NonZeroU64,
    pub head_sha: String,
    pub status: String,
    pub conclusion: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct Review {
    pub state: String,
    pub environments: Vec<Environment>,
    pub user: User,
}

#[derive(Deserialize)]
pub(super) struct Environment {
    pub id: NonZeroU64,
    pub name: String,
}

pub(super) fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn download_record(artifact: &Artifact) -> Result<ValidationRecord> {
    const MAX_RECORD_BYTES: u64 = 64 * 1024;
    ensure!(
        artifact.size_in_bytes <= MAX_RECORD_BYTES,
        InvalidInputSnafu {
            message: "CLI validation record artifact exceeds 64 KiB",
        }
    );
    let file = tempfile::NamedTempFile::new().context(IoSnafu {
        operation: "create record download file",
    })?;
    let status = request(&format!("actions/artifacts/{}/zip", artifact.id))
        .stdout(file.reopen().context(FileSnafu { path: file.path() })?)
        .stderr(Stdio::inherit())
        .status()
        .context(CommandStartSnafu { program: "gh" })?;
    ensure!(
        status.success(),
        CommandFailedSnafu {
            program: "gh",
            status
        }
    );
    ensure!(
        file.as_file()
            .metadata()
            .context(FileSnafu { path: file.path() })?
            .len()
            == artifact.size_in_bytes,
        InvalidInputSnafu {
            message: "validation record download size differs from GitHub metadata"
        }
    );
    let bytes = fs::read(file.path()).context(FileSnafu { path: file.path() })?;
    ensure!(
        format!("sha256:{:x}", Sha256::digest(&bytes)) == artifact.digest,
        InvalidInputSnafu {
            message: "validation record download SHA256 differs from GitHub metadata"
        }
    );
    let archive = ZipArchive::from_slice(bytes).context(ArtifactZipSnafu)?;
    ensure!(
        archive.entries_hint() == 1,
        InvalidInputSnafu {
            message: "record ZIP must contain only cli-validation.json"
        }
    );
    let mut entries = archive.entries();
    let entry = entries
        .next_entry()
        .context(ArtifactZipSnafu)?
        .ok_or_else(|| {
            InvalidInputSnafu {
                message: "record ZIP is empty",
            }
            .build()
        })?;
    ensure!(
        entry.file_path().as_bytes() == b"cli-validation.json" && !entry.flags().is_encrypted(),
        InvalidInputSnafu {
            message: "record ZIP has an unexpected or encrypted entry"
        }
    );
    let local = archive
        .get_entry(entry.wayfinder())
        .context(ArtifactZipSnafu)?;
    let header = local.local_header();
    ensure!(
        header.file_path().as_bytes() == entry.file_path().as_bytes()
            && header.compression_method() == entry.compression_method()
            && !header.flags().is_encrypted()
            && entries.next_entry().context(ArtifactZipSnafu)?.is_none(),
        InvalidInputSnafu {
            message: "record ZIP has inconsistent or extra entries"
        }
    );
    let reader: Box<dyn Read + '_> = match entry.compression_method() {
        CompressionMethod::STORE => Box::new(local.data()),
        CompressionMethod::DEFLATE => Box::new(DeflateDecoder::new(local.data())),
        _ => {
            return InvalidInputSnafu {
                message: "unsupported record ZIP compression",
            }
            .fail();
        }
    };
    let mut json = Vec::new();
    local
        .verifying_reader(reader)
        .take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut json)
        .context(IoSnafu {
            operation: "read validation record from ZIP",
        })?;
    ensure!(
        json.len() as u64 <= MAX_RECORD_BYTES,
        InvalidInputSnafu {
            message: "CLI validation record exceeds 64 KiB"
        }
    );
    serde_json::from_slice(&json).context(JsonSnafu {
        subject: "CLI validation record",
    })
}
