use std::{
    collections::BTreeMap,
    fs,
    num::NonZeroU64,
    path::{Path, PathBuf},
    process::Command,
};

use clap::Args;
use serde::Deserialize;
use snafu::{ResultExt, ensure};

use crate::{
    command::command_output,
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, JsonSnafu, Result},
    release_plan::{PlannedAsset, ReleasePlan},
    release_selection::github,
    validation_record::REPOSITORY,
};

#[derive(Args)]
pub(crate) struct UploadReleaseAssetsArgs {
    /// Completed plan-release directory, including the original binary and image files.
    directory: PathBuf,
    /// SHA256 of release-plan.json from the trusted read-only verification step.
    #[arg(long)]
    plan_sha256: String,
    /// Verify the current remote assets and report missing uploads without writing to GitHub.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Deserialize)]
struct PublishedAsset {
    id: NonZeroU64,
    name: String,
    state: String,
    size: u64,
    digest: Option<String>,
}

impl UploadReleaseAssetsArgs {
    pub(crate) fn run(self) -> Result<()> {
        let plan = ReleasePlan::load_for_publication(&self.directory, &self.plan_sha256)?;
        if !self.dry_run {
            plan.selection.release.require_publication_context()?;
        }
        eprintln!(
            "CLI release {}: validation run {}, record artifact {}, binary artifact {}, image artifact {}.",
            plan.selection.release.tag,
            plan.selection.artifacts.validation_record.run_id,
            plan.selection.artifacts.record_artifact.id,
            plan.selection.artifacts.binary_artifact.id,
            plan.selection.artifacts.image_artifact.id
        );
        // Upload from a private snapshot, not files another process can replace after validation.
        let staging = tempfile::tempdir().context(IoSnafu {
            operation: "create release asset upload directory",
        })?;
        for asset in &plan.github_assets {
            let path = staging.path().join(&asset.name);
            fs::copy(self.directory.join(&asset.path), &path).context(FileSnafu { path: &path })?;
            asset.verify_file(&path)?;
        }
        let mut verified_ids = BTreeMap::new();
        let mut uploaded = 0;
        loop {
            // Check the entire remote set before every write, including conflicts in later files.
            let missing = check_existing_assets(&plan, &mut verified_ids)?;
            if self.dry_run {
                plan.selection.release.ensure_unchanged()?;
                for asset in missing {
                    println!("Would upload {} (SHA256 {})", asset.name, asset.sha256);
                }
                println!(
                    "Read-only asset rehearsal passed: {} existing files match.",
                    verified_ids.len()
                );
                return Ok(());
            }
            let Some(asset) = missing.first() else { break };
            plan.selection.release.ensure_unchanged()?;
            let published = upload_asset(
                plan.selection.release.id,
                asset,
                &staging.path().join(&asset.name),
            )?;
            verify_remote_asset(&published, asset)?;
            verified_ids.insert(asset.name.clone(), published.id);
            uploaded += 1;
        }
        plan.selection.release.ensure_unchanged()?;
        println!(
            "Verified all four CLI assets on {}: uploaded {uploaded}, reused {}.",
            plan.selection.release.tag,
            plan.github_assets.len() - uploaded,
        );
        Ok(())
    }
}

fn upload_asset(
    release_id: NonZeroU64,
    asset: &PlannedAsset,
    path: &Path,
) -> Result<PublishedAsset> {
    let endpoint = format!(
        "https://uploads.github.com/repos/{REPOSITORY}/releases/{release_id}/assets?name={}",
        asset.name
    );
    let response = command_output(
        Command::new("gh")
            .args([
                "api",
                "--hostname",
                "github.com",
                "--method",
                "POST",
                "-H",
                "X-GitHub-Api-Version: 2022-11-28",
                "-H",
                "Content-Type: application/octet-stream",
                &endpoint,
                "--input",
            ])
            .arg(path),
    )?;
    serde_json::from_str(&response).context(JsonSnafu {
        subject: "uploaded GitHub release asset",
    })
}

fn check_existing_assets<'a>(
    plan: &'a ReleasePlan,
    verified_ids: &mut BTreeMap<String, NonZeroU64>,
) -> Result<Vec<&'a PlannedAsset>> {
    let assets: Vec<_> = github::pages::<Vec<PublishedAsset>>(&format!(
        "releases/{}/assets?per_page=100",
        plan.selection.release.id
    ))?
    .into_iter()
    .flatten()
    .collect();
    let mut missing = Vec::new();
    for expected in &plan.github_assets {
        let mut matching = assets.iter().filter(|asset| asset.name == expected.name);
        if let Some(asset) = matching.next() {
            ensure!(
                matching.next().is_none(),
                InvalidInputSnafu {
                    message: format!("duplicate release asset: {}", expected.name)
                }
            );
            check_asset_metadata(asset, expected)?;
            // Replacing GitHub asset contents creates a new ID, so download each ID only once.
            if verified_ids.get(&expected.name) != Some(&asset.id) {
                verify_remote_asset(asset, expected)?;
                verified_ids.insert(expected.name.clone(), asset.id);
            }
        } else {
            ensure!(
                !verified_ids.contains_key(&expected.name),
                InvalidInputSnafu {
                    message: format!(
                        "release asset disappeared during publication: {}",
                        expected.name
                    )
                }
            );
            missing.push(expected);
        }
    }
    Ok(missing)
}

fn check_asset_metadata(asset: &PublishedAsset, expected: &PlannedAsset) -> Result<()> {
    ensure!(
        asset.name == expected.name
            && asset.state == "uploaded"
            && asset.size == expected.size
            && asset
                .digest
                .as_ref()
                .is_none_or(|digest| digest == &format!("sha256:{}", expected.sha256)),
        InvalidInputSnafu {
            message: format!(
                "release asset {} has conflicting or incomplete content; existing assets cannot be replaced",
                expected.name
            )
        }
    );
    Ok(())
}

fn verify_remote_asset(asset: &PublishedAsset, expected: &PlannedAsset) -> Result<()> {
    check_asset_metadata(asset, expected)?;
    let mut request = github::request(&format!("releases/assets/{}", asset.id));
    request.args(["-H", "Accept: application/octet-stream"]);
    let download = github::download_to_temp_file(request)?;
    expected.verify_file(download.path())
}
