use std::{env, fs, io::Write, num::NonZeroU64, path::PathBuf};

use serde_json::Value;
use snafu::{ResultExt, ensure};

use crate::{
    build::{is_full_commit_sha, is_stable_version},
    error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
    release_selection::ReleaseSelectionArgs,
    validation_record::REPOSITORY,
};

pub(crate) fn check_release_request() -> Result<()> {
    ensure!(
        env::var("GITHUB_ACTIONS").as_deref() == Ok("true")
            && env::var("GITHUB_REPOSITORY").as_deref() == Ok(REPOSITORY),
        InvalidInputSnafu {
            message: "CLI release requests require the upstream GitHub Actions workflow"
        }
    );
    let path = required_path("GITHUB_EVENT_PATH")?;
    let event: Value = serde_json::from_slice(&fs::read(&path).context(FileSnafu { path: &path })?)
        .context(JsonSnafu {
            subject: "CLI release event",
        })?;
    ensure!(
        event["repository"]["full_name"] == REPOSITORY,
        InvalidInputSnafu {
            message: "CLI release event repository differs"
        }
    );
    let automatic = env::var("GITHUB_EVENT_NAME").as_deref() == Ok("release");
    let (tag, publish, run_id, record_artifact_id) = if automatic {
        ensure!(
            event["action"] == "published"
                && event["release"]["draft"] == false
                && event["release"]["prerelease"] == false,
            InvalidInputSnafu {
                message: "CLI publication requires a stable published release event"
            }
        );
        (
            string_field(&event["release"], "tag_name")?,
            true,
            None,
            None,
        )
    } else {
        ensure!(
            env::var("GITHUB_EVENT_NAME").as_deref() == Ok("workflow_dispatch"),
            InvalidInputSnafu {
                message: "CLI publication only accepts release or workflow_dispatch events"
            }
        );
        let inputs = &event["inputs"];
        let mode = string_field(inputs, "mode")?;
        ensure!(
            matches!(mode, "rehearse" | "publish"),
            InvalidInputSnafu {
                message: "CLI release mode must be rehearse or publish"
            }
        );
        (
            string_field(inputs, "tag")?,
            mode == "publish",
            Some(positive_id(inputs, "validation-run-id")?),
            Some(positive_id(inputs, "record-artifact-id")?),
        )
    };
    ensure!(
        tag.strip_prefix('v').is_some_and(is_stable_version),
        InvalidInputSnafu {
            message: "CLI release tag must be stable vX.Y.Z"
        }
    );
    let reference = if automatic {
        format!("refs/tags/{tag}")
    } else {
        "refs/heads/main".into()
    };
    ensure!(
        env::var("GITHUB_REF").as_deref() == Ok(reference.as_str())
            && env::var("GITHUB_WORKFLOW_REF").as_deref()
                == Ok(
                    format!("{REPOSITORY}/.github/workflows/cli-release.yml@{reference}").as_str()
                )
            && env::var("GITHUB_SHA").is_ok_and(|sha| is_full_commit_sha(&sha)),
        InvalidInputSnafu {
            message: "CLI release requests require the trusted workflow at the release tag or main recovery ref"
        }
    );
    let selection = ReleaseSelectionArgs {
        tag: tag.into(),
        run_id,
        record_artifact_id,
    }
    .select()?;
    if automatic {
        ensure!(
            event["release"]["id"].as_u64() == Some(selection.release.id.get()),
            InvalidInputSnafu {
                message: "release event ID differs from the current stable release"
            }
        );
    }
    selection.release.require_publication_context()?;
    selection.release.ensure_unchanged()?;
    let record = &selection.artifacts.validation_record;
    let output = format!(
        "tag={tag}\npublish={publish}\nrelease-commit={}\nvalidation-run-id={}\nrecord-artifact-id={}\n",
        selection.release.commit, record.run_id, selection.artifacts.record_artifact.id,
    );
    let path = required_path("GITHUB_OUTPUT")?;
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .context(FileSnafu { path: &path })?
        .write_all(output.as_bytes())
        .context(FileSnafu { path: &path })?;
    println!(
        "CLI release {tag}: PR #{}, validation run {}, record artifact {}, binary artifact {}, image artifact {}.",
        record.pull_request,
        record.run_id,
        selection.artifacts.record_artifact.id,
        record.binary_artifact.id,
        record.image_artifact.id
    );
    Ok(())
}

fn string_field<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field].as_str().ok_or_else(|| {
        InvalidInputSnafu {
            message: format!("CLI release event requires string field {field}"),
        }
        .build()
    })
}

fn positive_id(value: &Value, field: &str) -> Result<NonZeroU64> {
    let text = string_field(value, field)?;
    ensure!(
        !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()),
        InvalidInputSnafu {
            message: format!("{field} must be a positive integer")
        }
    );
    text.parse().map_err(|_| {
        InvalidInputSnafu {
            message: format!("{field} must be a positive integer"),
        }
        .build()
    })
}

fn required_path(name: &str) -> Result<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            InvalidInputSnafu {
                message: format!("{name} is required"),
            }
            .build()
        })
}
