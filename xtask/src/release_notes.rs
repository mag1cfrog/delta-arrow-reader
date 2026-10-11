use std::{fs, num::NonZeroU64, path::PathBuf, process::Command};

use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use crate::{
    command::command_output,
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, JsonSnafu, Result},
    release_assets, release_images,
    release_plan::ReleasePlan,
    release_selection::github,
    validation_record::REPOSITORY,
};

const START: &str = "<!-- dar-cli:start -->";
const END: &str = "<!-- dar-cli:end -->";
const STATE: &str = "<!-- dar-cli-state ";

#[derive(Args)]
pub(crate) struct UpdateReleaseNotesArgs {
    /// Completed plan-release directory, including the original binary and image files.
    directory: PathBuf,
    /// SHA256 of release-plan.json from the trusted read-only verification step.
    #[arg(long)]
    plan_sha256: String,
    /// Mark pending before the first upload; mark complete only after both delivery checks pass.
    #[arg(long, value_enum)]
    status: PublicationStatus,
    /// Validate the section and provenance without changing the release body.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum PublicationStatus {
    Pending,
    Complete,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PublicationState {
    status: PublicationStatus,
    release_commit: String,
    validation_run_id: NonZeroU64,
    record_artifact_id: NonZeroU64,
}

#[derive(Deserialize)]
struct ReleaseBody {
    body: Option<String>,
}

impl UpdateReleaseNotesArgs {
    pub(crate) fn run(self) -> Result<()> {
        let plan = ReleasePlan::load_for_publication(&self.directory, &self.plan_sha256)?;
        if !self.dry_run {
            plan.selection.release.require_publication_context()?;
        }
        let endpoint = format!("releases/{}", plan.selection.release.id);
        let current: ReleaseBody = github::get(&endpoint)?;
        let current = current.body.unwrap_or_default();
        let replacement = updated_body(&current, &plan, self.status)?;
        plan.selection.release.ensure_unchanged()?;
        if self.dry_run || replacement == current {
            println!(
                "CLI release section {}.",
                if self.dry_run {
                    "rehearsal passed"
                } else {
                    "already matches"
                }
            );
            return Ok(());
        }
        let latest: ReleaseBody = github::get(&endpoint)?;
        ensure!(
            latest.body.unwrap_or_default() == current,
            InvalidInputSnafu {
                message: "release body changed while preparing the CLI section; retry without replacing other edits"
            }
        );
        let request = tempfile::NamedTempFile::new().context(IoSnafu {
            operation: "create release notes request",
        })?;
        let bytes =
            serde_json::to_vec(&serde_json::json!({"body": replacement})).context(JsonSnafu {
                subject: "CLI release notes request",
            })?;
        fs::write(request.path(), bytes).context(FileSnafu {
            path: request.path(),
        })?;
        plan.selection.release.ensure_unchanged()?;
        let response = command_output(
            Command::new("gh")
                .args([
                    "api",
                    "--hostname",
                    "github.com",
                    "--method",
                    "PATCH",
                    "-H",
                    "X-GitHub-Api-Version: 2022-11-28",
                    "-H",
                    "Content-Type: application/json",
                    &format!("repos/{REPOSITORY}/{endpoint}"),
                    "--input",
                ])
                .arg(request.path()),
        )?;
        let response: ReleaseBody = serde_json::from_str(&response).context(JsonSnafu {
            subject: "updated CLI release notes",
        })?;
        ensure!(
            response.body.as_deref() == Some(&replacement),
            InvalidInputSnafu {
                message: "GitHub did not retain the requested CLI release section"
            }
        );
        plan.selection.release.ensure_unchanged()?;
        println!(
            "Updated the CLI section of {}; preserved the release title and other notes.",
            plan.selection.release.tag
        );
        Ok(())
    }
}

fn updated_body(body: &str, plan: &ReleasePlan, status: PublicationStatus) -> Result<String> {
    let record = &plan.selection.artifacts.validation_record;
    let state = PublicationState {
        status,
        release_commit: plan.selection.release.commit.clone(),
        validation_run_id: record.run_id,
        record_artifact_id: plan.selection.artifacts.record_artifact.id,
    };
    let start_count = body.matches(START).count();
    let end_count = body.matches(END).count();
    ensure!(
        matches!((start_count, end_count), (0, 0) | (1, 1)),
        InvalidInputSnafu {
            message: "release has malformed or duplicate CLI section markers"
        }
    );
    let range = if start_count == 1 {
        let start = body.find(START).unwrap();
        let end = body.find(END).unwrap();
        ensure!(
            start < end,
            InvalidInputSnafu {
                message: "CLI section end precedes its start"
            }
        );
        let section = &body[start + START.len()..end];
        let previous = section
            .trim_start()
            .lines()
            .next()
            .and_then(|line| line.strip_prefix(STATE))
            .and_then(|line| line.strip_suffix(" -->"))
            .ok_or_else(|| {
                InvalidInputSnafu {
                    message: "existing CLI section has no validation identity",
                }
                .build()
            })?;
        let previous: PublicationState = serde_json::from_str(previous).context(JsonSnafu {
            subject: "existing CLI publication state",
        })?;
        ensure!(
            previous.release_commit == state.release_commit,
            InvalidInputSnafu {
                message: "existing CLI section belongs to a different release commit"
            }
        );
        if previous.validation_run_id != state.validation_run_id
            || previous.record_artifact_id != state.record_artifact_id
        {
            ensure!(
                previous.status == PublicationStatus::Pending
                    && status == PublicationStatus::Pending,
                InvalidInputSnafu {
                    message: "completed CLI publication cannot select a different validation record"
                }
            );
            release_assets::require_no_published_assets(plan)?;
            release_images::require_no_published_images(plan)?;
        } else if previous.status == PublicationStatus::Complete
            && status == PublicationStatus::Pending
        {
            return Ok(body.into());
        }
        Some(start..end + END.len())
    } else {
        None
    };
    let section = render_section(plan, &state)?;
    Ok(if let Some(range) = range {
        format!("{}{}{}", &body[..range.start], section, &body[range.end..])
    } else if body.is_empty() {
        format!("{section}\n")
    } else {
        format!("{body}\n\n{section}\n")
    })
}

fn render_section(plan: &ReleasePlan, state: &PublicationState) -> Result<String> {
    let record = &plan.selection.artifacts.validation_record;
    let release = &plan.selection.release;
    let state_json = serde_json::to_string(state).context(JsonSnafu {
        subject: "CLI publication state",
    })?;
    let availability = match state.status {
        PublicationStatus::Pending => {
            "CLI publication is incomplete. Wait for the CLI release workflow to finish before installing this version."
        }
        PublicationStatus::Complete => {
            "The CLI binaries and both container platforms passed download verification."
        }
    };
    let mut text = format!(
        "{START}\n{STATE}{state_json} -->\n\n\
         ## CLI {tag}\n\n\
         {availability}\n\n\
         - Version: `{version}`\n\
         - Tested source: `{tested_commit}`\n\
         - Release commit: `{release_commit}`\n\
         - Validation: [run {run_id}](https://github.com/{REPOSITORY}/actions/runs/{run_id}), record artifact `{record_id}`\n\n\
         Linux `amd64` and `arm64` require glibc 2.36 or newer, libgcc, and CA certificates. \
         The archives were tested on Debian 12, Ubuntu 24.04, and Fedora 44. \
         Alpine/musl and other platforms are not supported.\n\n\
         | File | SHA256 |\n\
         | --- | --- |\n",
        tag = release.tag,
        version = release.version,
        tested_commit = record.source_commit,
        release_commit = release.commit,
        run_id = record.run_id,
        record_id = state.record_artifact_id,
    );
    for asset in &plan.github_assets {
        text.push_str(&format!(
            "| [{}](https://github.com/{REPOSITORY}/releases/download/{}/{}) | `{}` |\n",
            asset.name, release.tag, asset.name, asset.sha256
        ));
    }
    text.push_str(&format!(
        "\nContainer: `{}`\n\n\
         Index digest: `{}`\n\n\
         | Platform | Manifest digest |\n\
         | --- | --- |\n",
        plan.container_image.reference, plan.container_image.index_digest
    ));
    for platform in &plan.container_image.platforms {
        text.push_str(&format!(
            "| linux/{} | `{}` |\n",
            platform.architecture.name(),
            platform.manifest_digest
        ));
    }
    text.push_str(&format!(
        "\nUse a version or digest when running the image or copying `dar` into a Go service image. \
         See [installation and checksum verification](https://github.com/{REPOSITORY}/blob/{commit}/crates/delta-arrow-reader-core/docs/public/installation.md#cli-binaries-and-container-images) \
         and the [Go consumer](https://github.com/{REPOSITORY}/tree/{commit}/examples/go-cli).\n\n{END}",
        commit = release.commit,
    ));
    Ok(text)
}
