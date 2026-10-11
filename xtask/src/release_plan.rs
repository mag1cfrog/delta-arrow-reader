mod download;

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use clap::Args;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::{self, ArtifactManifest, file_sha256, write_new_file},
    build::BuildIdentity,
    command::command_output,
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, JsonSnafu, Result},
    image_artifacts::{self, ImageManifest, transport_path},
    platform::Architecture,
    release_selection::{ReleaseSelectionArgs, SelectedArtifacts},
};

const IMAGE_REPOSITORY: &str = "ghcr.io/mag1cfrog/delta-arrow-reader";

#[derive(Args)]
pub(crate) struct PlanReleaseArgs {
    /// New directory for verified binary/image sets, image-index.json, and release-plan.json.
    output: PathBuf,
    #[command(flatten)]
    selection: ReleaseSelectionArgs,
}

#[derive(Serialize)]
struct ReleasePlan {
    schema_version: u32,
    selection: SelectedArtifacts,
    github_assets: Vec<ReleaseAsset>,
    container_image: ContainerImagePlan,
}

#[derive(Serialize)]
struct ReleaseAsset {
    name: String,
    path: String,
    size: u64,
    sha256: String,
}

#[derive(Serialize)]
struct ContainerImagePlan {
    reference: String,
    index_path: &'static str,
    index_digest: String,
    platforms: Vec<PlatformImagePlan>,
}

#[derive(Serialize)]
struct PlatformImagePlan {
    architecture: Architecture,
    reference: String,
    archive_path: String,
    archive_sha256: String,
    manifest_digest: String,
}

impl PlanReleaseArgs {
    pub(crate) fn run(self) -> Result<()> {
        let output = std::path::absolute(&self.output).context(FileSnafu { path: &self.output })?;
        ensure!(
            !output.try_exists().context(FileSnafu { path: &output })?,
            InvalidInputSnafu {
                message: "release plan directory already exists",
            }
        );
        let selection = self.selection.select()?;
        let parent = output.parent().ok_or_else(|| {
            InvalidInputSnafu {
                message: "release plan directory has no parent",
            }
            .build()
        })?;
        let temporary = tempfile::Builder::new()
            .prefix(".dar-release-")
            .tempdir_in(parent)
            .context(IoSnafu {
                operation: "create temporary release plan directory",
            })?;
        let stage = temporary.path();
        let record = &selection.artifacts.validation_record;
        let binaries = stage.join("binaries");
        let images = stage.join("images");
        let mut binary_names: Vec<_> = Architecture::ALL
            .into_iter()
            .map(|architecture| artifacts::archive_name(&record.version, architecture))
            .collect();
        binary_names.extend([artifacts::MANIFEST.into(), "SHA256SUMS".into()]);
        let mut image_names: Vec<_> = Architecture::ALL
            .into_iter()
            .map(|architecture| image_artifacts::archive_name(&record.version, architecture))
            .collect();
        image_names.push(image_artifacts::MANIFEST.into());
        download::extract_artifact(
            &selection.artifacts.binary_artifact,
            &binaries,
            &binary_names,
        )?;
        let manifest_path = binaries.join(artifacts::MANIFEST);
        check_manifest_hash(&manifest_path, &record.binary_artifact.manifest_sha256)?;
        let manifest: ArtifactManifest =
            serde_json::from_slice(&fs::read(&manifest_path).context(FileSnafu {
                path: &manifest_path,
            })?)
            .context(JsonSnafu {
                subject: "binary artifact manifest",
            })?;
        let identity = BuildIdentity {
            version: record.version.clone(),
            source_commit: record.source_commit.clone(),
            rust_toolchain: manifest.rust_toolchain,
        };
        identity.validate()?;
        let binary_manifest = artifacts::verify(&binaries, &identity)?;
        download::extract_artifact(&selection.artifacts.image_artifact, &images, &image_names)?;
        check_manifest_hash(
            &images.join(image_artifacts::MANIFEST),
            &record.image_artifact.manifest_sha256,
        )?;
        let image_manifest =
            image_artifacts::verify_complete(&images, &binaries, &identity, &binary_manifest)?;

        let index = image_index(&images, &image_manifest)?;
        let index_bytes = serde_json::to_vec(&index).context(JsonSnafu {
            subject: "OCI release index",
        })?;
        write_new_file(&stage.join("image-index.json"), &index_bytes)?;
        let container_image = ContainerImagePlan {
            reference: format!("{IMAGE_REPOSITORY}:{}", selection.release.tag),
            index_path: "image-index.json",
            index_digest: format!("sha256:{:x}", Sha256::digest(&index_bytes)),
            platforms: image_manifest
                .targets
                .into_iter()
                .map(|target| PlatformImagePlan {
                    architecture: target.architecture,
                    reference: format!(
                        "{IMAGE_REPOSITORY}:{}-linux-{}",
                        selection.release.tag,
                        target.architecture.name()
                    ),
                    archive_path: format!("images/{}", target.archive),
                    archive_sha256: target.archive_sha256,
                    manifest_digest: target.manifest_digest,
                })
                .collect(),
        };
        let github_assets = binary_names
            .into_iter()
            .map(|name| {
                let path = binaries.join(&name);
                Ok(ReleaseAsset {
                    size: fs::metadata(&path)
                        .context(FileSnafu { path: &path })?
                        .len(),
                    sha256: file_sha256(&path)?,
                    path: format!("binaries/{name}"),
                    name,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        selection.release.ensure_unchanged()?;
        let plan = ReleasePlan {
            schema_version: 1,
            selection,
            github_assets,
            container_image,
        };
        let mut bytes = serde_json::to_vec_pretty(&plan).context(JsonSnafu {
            subject: "CLI release plan",
        })?;
        bytes.push(b'\n');
        // Create the destination only after verification; create_dir also rejects a concurrent creator.
        fs::create_dir(&output).context(FileSnafu { path: &output })?;
        for name in ["binaries", "images", "image-index.json"] {
            let destination = output.join(name);
            fs::rename(stage.join(name), &destination).context(FileSnafu { path: &destination })?;
        }
        // A failed download, verification, or move must never leave a completed plan.
        write_new_file(&output.join("release-plan.json"), &bytes)?;
        println!(
            "Prepared read-only CLI release plan for {} in {}",
            plan.selection.release.tag,
            output.display()
        );
        Ok(())
    }
}

fn check_manifest_hash(path: &Path, expected: &str) -> Result<()> {
    ensure!(
        file_sha256(path)? == expected,
        InvalidInputSnafu {
            message: format!(
                "{} SHA256 differs from the approved validation record",
                path.display()
            ),
        }
    );
    Ok(())
}

fn image_index(directory: &Path, images: &ImageManifest) -> Result<Value> {
    let mut manifests = Vec::new();
    for target in &images.targets {
        let path = directory.join(&target.archive);
        let raw = command_output(
            Command::new("skopeo")
                .args(["inspect", "--raw"])
                .arg(format!("oci-archive:{}", transport_path(&path)?)),
        )?;
        ensure!(
            format!("sha256:{:x}", Sha256::digest(raw.as_bytes())) == target.manifest_digest,
            InvalidInputSnafu {
                message: "OCI manifest changed while preparing the release index"
            }
        );
        manifests.push(json!({
            "mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": target.manifest_digest,
            "size": raw.len(), "platform": {"architecture": target.architecture, "os": "linux"},
        }));
    }
    Ok(
        json!({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": manifests}),
    )
}
