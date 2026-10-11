use std::{fs, path::Path};

use sha2::{Digest, Sha256};
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::{self, file_sha256},
    error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
    image_artifacts,
    platform::Architecture,
    release_selection::github::is_sha256,
};

use super::{IMAGE_REPOSITORY, PlannedAsset, ReleasePlan, check_manifest_hash};

impl ReleasePlan {
    /// The expected hash must come from the trusted read-only verification step.
    pub(crate) fn load_for_publication(directory: &Path, expected_hash: &str) -> Result<Self> {
        artifacts::check_directory(
            directory,
            &[
                "binaries".into(),
                "images".into(),
                "image-index.json".into(),
                "release-plan.json".into(),
            ],
        )?;
        let path = directory.join("release-plan.json");
        artifacts::require_regular_file(&path)?;
        let bytes = fs::read(&path).context(FileSnafu { path: &path })?;
        ensure!(
            is_sha256(expected_hash) && format!("{:x}", Sha256::digest(&bytes)) == expected_hash,
            InvalidInputSnafu {
                message: "release plan SHA256 differs from the trusted verification handoff"
            }
        );
        let plan: Self = serde_json::from_slice(&bytes).context(JsonSnafu {
            subject: "CLI release plan",
        })?;
        ensure!(
            plan.schema_version == 1,
            InvalidInputSnafu {
                message: "unsupported CLI release plan schema"
            }
        );
        plan.selection.revalidate()?;
        let record = &plan.selection.artifacts.validation_record;

        let mut binary_names: Vec<_> = Architecture::ALL
            .into_iter()
            .map(|architecture| artifacts::archive_name(&record.version, architecture))
            .collect();
        binary_names.extend([artifacts::MANIFEST.into(), "SHA256SUMS".into()]);
        artifacts::check_files(&directory.join("binaries"), &binary_names)?;
        ensure!(
            plan.github_assets.len() == binary_names.len(),
            InvalidInputSnafu {
                message: "release plan must contain exactly four binary assets"
            }
        );
        for (asset, name) in plan.github_assets.iter().zip(binary_names) {
            ensure!(
                asset.name == name && asset.path == format!("binaries/{name}"),
                InvalidInputSnafu {
                    message: "unexpected asset name or path in release plan"
                }
            );
            asset.verify_file(&directory.join(&asset.path))?;
        }
        check_manifest_hash(
            &directory.join("binaries").join(artifacts::MANIFEST),
            &record.binary_artifact.manifest_sha256,
        )?;

        let mut image_names: Vec<_> = Architecture::ALL
            .into_iter()
            .map(|architecture| image_artifacts::archive_name(&record.version, architecture))
            .collect();
        image_names.push(image_artifacts::MANIFEST.into());
        artifacts::check_files(&directory.join("images"), &image_names)?;
        check_manifest_hash(
            &directory.join("images").join(image_artifacts::MANIFEST),
            &record.image_artifact.manifest_sha256,
        )?;
        let image = &plan.container_image;
        ensure!(
            image.reference == format!("{IMAGE_REPOSITORY}:{}", plan.selection.release.tag)
                && image.index_path == "image-index.json"
                && image.platforms.len() == Architecture::ALL.len(),
            InvalidInputSnafu {
                message: "unexpected image destination or inventory in release plan"
            }
        );
        for (platform, architecture) in image.platforms.iter().zip(Architecture::ALL) {
            let name = image_artifacts::archive_name(&record.version, architecture);
            ensure!(
                platform.architecture == architecture
                    && platform.reference
                        == format!("{}-linux-{}", image.reference, architecture.name())
                    && platform.archive_path == format!("images/{name}")
                    && is_sha256(&platform.archive_sha256)
                    && platform
                        .manifest_digest
                        .strip_prefix("sha256:")
                        .is_some_and(is_sha256)
                    && file_sha256(&directory.join(&platform.archive_path))?
                        == platform.archive_sha256,
                InvalidInputSnafu {
                    message: format!(
                        "release plan image identity or file hash differs: {}",
                        architecture.name()
                    )
                }
            );
        }
        let index = directory.join(&image.index_path);
        artifacts::require_regular_file(&index)?;
        ensure!(
            image.index_digest == format!("sha256:{}", file_sha256(&index)?),
            InvalidInputSnafu {
                message: "release plan image index SHA256 differs"
            }
        );
        Ok(plan)
    }
}

impl PlannedAsset {
    pub(crate) fn verify_file(&self, path: &Path) -> Result<()> {
        artifacts::require_regular_file(path)?;
        ensure!(
            self.size > 0
                && fs::metadata(path).context(FileSnafu { path })?.len() == self.size
                && is_sha256(&self.sha256)
                && file_sha256(path)? == self.sha256,
            InvalidInputSnafu {
                message: format!(
                    "release asset {} has different bytes from the verified plan",
                    self.name
                )
            }
        );
        Ok(())
    }
}
