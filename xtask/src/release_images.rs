use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use clap::Args;
use sha2::{Digest, Sha256};
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::{self, ArtifactManifest, file_sha256},
    build::BuildIdentity,
    command::run_command,
    error::{
        CommandFailedSnafu, CommandStartSnafu, FileSnafu, InvalidInputSnafu, IoSnafu, JsonSnafu,
        Result,
    },
    image_artifacts,
    release_plan::{IMAGE_REPOSITORY, ReleasePlan},
};

#[derive(Args)]
pub(crate) struct UploadReleaseImagesArgs {
    /// Completed plan-release directory, including the original binary and image files.
    directory: PathBuf,
    /// SHA256 of release-plan.json from the trusted read-only verification step.
    #[arg(long)]
    plan_sha256: String,
    /// Check existing registry content and report missing uploads without writing to GHCR.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Args)]
pub(crate) struct VerifyReleaseImagesArgs {
    /// Completed plan-release directory, including the original binary and image files.
    directory: PathBuf,
    /// SHA256 of release-plan.json from the trusted read-only verification step.
    #[arg(long)]
    plan_sha256: String,
}

impl UploadReleaseImagesArgs {
    pub(crate) fn run(self) -> Result<()> {
        let plan = ReleasePlan::load_for_publication(&self.directory, &self.plan_sha256)?;
        if !self.dry_run {
            plan.selection.release.require_publication_context()?;
        }
        let staging = tempfile::tempdir().context(IoSnafu {
            operation: "create image publication directory",
        })?;
        let image = &plan.container_image;
        // Snapshot all inputs before the first write, including the index published last.
        for (relative, digest) in image
            .platforms
            .iter()
            .map(|platform| {
                (
                    platform.archive_path.as_str(),
                    platform.archive_sha256.as_str(),
                )
            })
            .chain([(
                image.index_path.as_str(),
                image.index_digest.strip_prefix("sha256:").unwrap(),
            )])
        {
            let path = staging.path().join(relative);
            fs::create_dir_all(path.parent().unwrap()).context(FileSnafu { path: &path })?;
            fs::copy(self.directory.join(relative), &path).context(FileSnafu { path: &path })?;
            ensure!(
                file_sha256(&path)? == digest,
                InvalidInputSnafu {
                    message: format!("image publication input changed: {relative}")
                }
            );
        }
        let mut verified = BTreeSet::new();
        loop {
            let missing = check_existing_images(&plan, &mut verified)?;
            if self.dry_run {
                plan.selection.release.ensure_unchanged()?;
                for reference in missing {
                    println!("Would publish {reference}");
                }
                println!(
                    "Read-only image rehearsal passed: {} existing references match.",
                    verified.len()
                );
                return Ok(());
            }
            let Some(reference) = missing.first() else {
                break;
            };
            if let Some(platform) = image.platforms.iter().find(|p| &p.reference == *reference) {
                plan.selection.release.ensure_unchanged()?;
                run_command(
                    Command::new("regctl")
                        .args(["image", "import", reference])
                        .arg(staging.path().join(&platform.archive_path)),
                )?;
                require_manifest(reference, &platform.manifest_digest, None)?;
            } else {
                let path = staging.path().join(&image.index_path);
                for platform in &image.platforms {
                    require_manifest(
                        &format!("{IMAGE_REPOSITORY}@{}", platform.manifest_digest),
                        &platform.manifest_digest,
                        None,
                    )?;
                }
                plan.selection.release.ensure_unchanged()?;
                run_command(
                    Command::new("regctl")
                        .args([
                            "manifest",
                            "put",
                            reference,
                            "--content-type",
                            "application/vnd.oci.image.index.v1+json",
                        ])
                        .stdin(fs::File::open(&path).context(FileSnafu { path: &path })?),
                )?;
                require_manifest(reference, &image.index_digest, None)?;
            }
            verified.insert((*reference).clone());
        }
        let anonymous = anonymous_config()?;
        verify_public_manifests(&plan, anonymous.path())?;
        plan.selection.release.ensure_unchanged()?;
        println!(
            "Published CLI image {} at {}; both platforms are publicly readable.",
            image.reference, image.index_digest
        );
        Ok(())
    }
}

impl VerifyReleaseImagesArgs {
    pub(crate) fn run(self) -> Result<()> {
        let plan = ReleasePlan::load_for_publication(&self.directory, &self.plan_sha256)?;
        let anonymous = anonymous_config()?;
        verify_public_manifests(&plan, anonymous.path())?;
        let binaries = self.directory.join("binaries");
        let path = binaries.join(artifacts::MANIFEST);
        let manifest: ArtifactManifest = serde_json::from_slice(
            &fs::read(&path).context(FileSnafu { path: &path })?,
        )
        .context(JsonSnafu {
            subject: "binary artifact manifest",
        })?;
        let record = &plan.selection.artifacts.validation_record;
        let identity = BuildIdentity {
            version: record.version.clone(),
            source_commit: record.source_commit.clone(),
            rust_toolchain: manifest.rust_toolchain,
        };
        let manifest = artifacts::verify(&binaries, &identity)?;
        let downloads = tempfile::tempdir().context(IoSnafu {
            operation: "create anonymous image download directory",
        })?;
        for platform in &plan.container_image.platforms {
            let path = downloads.path().join(image_artifacts::archive_name(
                &identity.version,
                platform.architecture,
            ));
            run_command(
                Command::new("regctl")
                    .env("REGCTL_CONFIG", anonymous.path())
                    .args([
                        "image",
                        "export",
                        &format!("{IMAGE_REPOSITORY}@{}", platform.manifest_digest),
                    ])
                    .arg(&path),
            )?;
            image_artifacts::verify_downloaded_image(
                downloads.path(),
                &binaries,
                &identity,
                &manifest,
                platform.architecture,
                &platform.manifest_digest,
            )?;
        }
        verify_public_manifests(&plan, anonymous.path())?;
        plan.selection.release.ensure_unchanged()?;
        println!(
            "Anonymous CLI image downloads match the verified platform digests and binary archives."
        );
        Ok(())
    }
}

fn check_existing_images<'a>(
    plan: &'a ReleasePlan,
    verified: &mut BTreeSet<String>,
) -> Result<Vec<&'a String>> {
    let image = &plan.container_image;
    let mut missing = Vec::new();
    for (reference, digest) in image
        .platforms
        .iter()
        .map(|p| (&p.reference, &p.manifest_digest))
        .chain([(&image.reference, &image.index_digest)])
    {
        if let Some(bytes) = read_manifest(reference, None)? {
            check_manifest_digest(reference, &bytes, digest)?;
            verified.insert(reference.clone());
        } else {
            ensure!(
                !verified.contains(reference),
                InvalidInputSnafu {
                    message: format!("image reference disappeared during publication: {reference}")
                }
            );
            missing.push(reference);
        }
    }
    Ok(missing)
}

pub(crate) fn require_no_published_images(plan: &ReleasePlan) -> Result<()> {
    ensure!(
        check_existing_images(plan, &mut BTreeSet::new())?.len()
            == plan.container_image.platforms.len() + 1,
        InvalidInputSnafu {
            message: "cannot replace the validation record after CLI images have been published"
        }
    );
    Ok(())
}

fn read_manifest(reference: &str, config: Option<&Path>) -> Result<Option<Vec<u8>>> {
    let mut command = Command::new("regctl");
    command.args(["manifest", "get", reference, "--format", "raw-body"]);
    if let Some(config) = config {
        command.env("REGCTL_CONFIG", config);
    }
    let output = command
        .output()
        .context(CommandStartSnafu { program: "regctl" })?;
    if output.status.success() {
        return Ok(Some(output.stdout));
    }
    let error = String::from_utf8_lossy(&output.stderr);
    // regctl v0.9.2 identifies registry HTTP 404 separately from auth and transport failures.
    let not_found =
        format!("failed to get manifest {reference}: request failed: not found [http 404]");
    if error.trim() == not_found || error.trim().starts_with(&format!("{not_found}: {{")) {
        return Ok(None);
    }
    eprintln!("{}", error.trim_end());
    CommandFailedSnafu {
        program: "regctl",
        status: output.status,
    }
    .fail()
}

fn check_manifest_digest(reference: &str, bytes: &[u8], expected: &str) -> Result<()> {
    ensure!(
        format!("sha256:{:x}", Sha256::digest(bytes)) == expected,
        InvalidInputSnafu {
            message: format!(
                "image {reference} has conflicting content; existing references cannot be replaced"
            )
        }
    );
    Ok(())
}

fn require_manifest(reference: &str, digest: &str, config: Option<&Path>) -> Result<()> {
    let bytes = read_manifest(reference, config)?.ok_or_else(|| {
        InvalidInputSnafu {
            message: format!("published image reference is missing: {reference}"),
        }
        .build()
    })?;
    check_manifest_digest(reference, &bytes, digest)
}

fn anonymous_config() -> Result<tempfile::NamedTempFile> {
    let config = tempfile::NamedTempFile::new().context(IoSnafu {
        operation: "create anonymous registry configuration",
    })?;
    fs::write(
        config.path(),
        br#"{"version":1,"incDockerCred":false,"incDockerCert":false,"hosts":{}}"#,
    )
    .context(FileSnafu {
        path: config.path(),
    })?;
    Ok(config)
}

fn verify_public_manifests(plan: &ReleasePlan, config: &Path) -> Result<()> {
    let image = &plan.container_image;
    eprintln!(
        "Checking anonymous GHCR access; the container package must be public. If access fails, set its visibility to public and retry this same plan."
    );
    require_manifest(&image.reference, &image.index_digest, Some(config))?;
    for platform in &image.platforms {
        require_manifest(&platform.reference, &platform.manifest_digest, Some(config))?;
        require_manifest(
            &format!("{IMAGE_REPOSITORY}@{}", platform.manifest_digest),
            &platform.manifest_digest,
            Some(config),
        )?;
    }
    Ok(())
}
