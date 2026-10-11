use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    process::Command,
};

use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::{self, ArtifactManifest, ArtifactTarget},
    build::BuildIdentity,
    command::{command_output, run_command},
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, JsonSnafu, Result},
    image::base_image,
    platform::Architecture,
};

pub(crate) const MANIFEST: &str = "cli-images.json";

#[derive(Subcommand)]
pub(crate) enum ImageArtifactTask {
    /// Export one local image to OCI and check its configuration and files.
    Export {
        #[command(flatten)]
        input: ImageArtifactInput,
        #[arg(long)]
        architecture: Architecture,
        /// Local image tag or ID to export with docker save.
        #[arg(long)]
        image: String,
    },
    /// Combine both platform directories without repacking their OCI archives.
    Assemble {
        #[command(flatten)]
        input: ImageArtifactInput,
        /// Directory containing exactly the amd64 and arm64 input directories.
        #[arg(long)]
        inputs: PathBuf,
    },
    /// Reimport OCI archives and verify their digests, configuration, and files.
    Verify {
        #[command(flatten)]
        input: ImageArtifactInput,
        /// Verify one platform; omit to require the complete three-file set.
        #[arg(long)]
        architecture: Option<Architecture>,
    },
}

#[derive(Args)]
pub(crate) struct ImageArtifactInput {
    /// Image artifact directory; export and assemble require a new destination.
    directory: PathBuf,
    /// Complete, validated two-platform binary artifact set.
    #[arg(long)]
    binary_artifacts: PathBuf,
    #[command(flatten)]
    identity: BuildIdentity,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImageManifest {
    schema_version: u32,
    version: String,
    source_commit: String,
    rust_toolchain: String,
    base_image: String,
    pub(crate) targets: Vec<ImageTarget>,
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImageTarget {
    os: String,
    pub(crate) architecture: Architecture,
    binary_archive_sha256: String,
    dar_sha256: String,
    pub(crate) archive: String,
    pub(crate) archive_sha256: String,
    pub(crate) manifest_digest: String,
    configuration: Value,
}

impl ImageArtifactTask {
    pub(crate) fn run(self) -> Result<()> {
        let input = match &self {
            Self::Export { input, .. }
            | Self::Assemble { input, .. }
            | Self::Verify { input, .. } => input,
        };
        input.identity.validate()?;
        let binaries = artifacts::verify(&input.binary_artifacts, &input.identity)?;
        let manifest = match &self {
            Self::Export {
                architecture,
                image,
                ..
            } => export_image(input, binary_target(&binaries, *architecture)?, image)?,
            Self::Assemble { inputs, .. } => assemble_images(inputs, input, &binaries)?,
            Self::Verify { architecture, .. } => verify(
                &input.directory,
                input,
                &binaries,
                architecture
                    .as_ref()
                    .map(std::slice::from_ref)
                    .unwrap_or(&Architecture::ALL),
            )?,
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&manifest).context(JsonSnafu {
                subject: "image artifact manifest"
            })?
        );
        Ok(())
    }
}

fn export_image(
    input: &ImageArtifactInput,
    binary: &ArtifactTarget,
    image: &str,
) -> Result<ImageManifest> {
    artifacts::create_directory(&input.directory)?;
    let temporary = tempfile::tempdir().context(IoSnafu {
        operation: "create the image export directory",
    })?;
    let saved = temporary.path().join("image.tar");
    run_command(
        Command::new("docker")
            .args(["save", "--output"])
            .arg(&saved)
            .arg("--")
            .arg(image),
    )?;
    let archive = input
        .directory
        .join(archive_name(&input.identity.version, binary.architecture));
    run_command(
        Command::new("skopeo")
            .args(["copy", "--format", "oci"])
            .arg(format!("docker-archive:{}", transport_path(&saved)?))
            .arg(format!("oci-archive:{}:dar", transport_path(&archive)?)),
    )?;
    let target = describe_image(&input.directory, input, binary)?;
    write_manifest(&input.directory, &input.identity, vec![target])
}

fn assemble_images(
    inputs: &Path,
    input: &ImageArtifactInput,
    binaries: &ArtifactManifest,
) -> Result<ImageManifest> {
    artifacts::check_directory(inputs, &["amd64".into(), "arm64".into()])?;
    let mut targets = Vec::new();
    for architecture in Architecture::ALL {
        targets.extend(
            verify(
                &inputs.join(architecture.name()),
                input,
                binaries,
                &[architecture],
            )?
            .targets,
        );
    }
    artifacts::create_directory(&input.directory)?;
    for target in &targets {
        let source = inputs
            .join(target.architecture.name())
            .join(&target.archive);
        let destination = input.directory.join(&target.archive);
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .context(FileSnafu { path: &destination })?;
        io::copy(
            &mut File::open(&source).context(FileSnafu { path: &source })?,
            &mut output,
        )
        .context(FileSnafu { path: &destination })?;
        ensure!(
            artifacts::file_sha256(&destination)? == target.archive_sha256,
            InvalidInputSnafu {
                message: "OCI archive changed during assembly"
            }
        );
    }
    write_manifest(&input.directory, &input.identity, targets)
}

fn binary_target(
    binaries: &ArtifactManifest,
    architecture: Architecture,
) -> Result<&ArtifactTarget> {
    binaries
        .targets
        .iter()
        .find(|target| target.architecture == architecture)
        .ok_or_else(|| {
            InvalidInputSnafu {
                message: "selected architecture is missing from the binary artifact set",
            }
            .build()
        })
}

pub(crate) fn archive_name(version: &str, architecture: Architecture) -> String {
    format!("dar-v{}-linux-{}.oci.tar", version, architecture.name())
}

pub(crate) fn transport_path(path: &Path) -> Result<&str> {
    path.to_str()
        .filter(|path| !path.contains(':'))
        .ok_or_else(|| {
            InvalidInputSnafu {
                message: "container tool paths must be UTF-8 and cannot contain ':'",
            }
            .build()
        })
}

fn write_manifest(
    directory: &Path,
    identity: &BuildIdentity,
    targets: Vec<ImageTarget>,
) -> Result<ImageManifest> {
    let manifest = ImageManifest {
        schema_version: 1,
        version: identity.version.clone(),
        source_commit: identity.source_commit.clone(),
        rust_toolchain: identity.rust_toolchain.clone(),
        base_image: base_image()?.into(),
        targets,
    };
    let mut contents = serde_json::to_vec_pretty(&manifest).context(JsonSnafu {
        subject: "image artifact manifest",
    })?;
    contents.push(b'\n');
    artifacts::write_new_file(&directory.join(MANIFEST), &contents)?;
    Ok(manifest)
}

pub(crate) fn verify_complete(
    directory: &Path,
    binary_artifacts: &Path,
    identity: &BuildIdentity,
    binaries: &ArtifactManifest,
) -> Result<ImageManifest> {
    verify(
        directory,
        &ImageArtifactInput {
            directory: directory.into(),
            binary_artifacts: binary_artifacts.into(),
            identity: identity.clone(),
        },
        binaries,
        &Architecture::ALL,
    )
}

fn verify(
    directory: &Path,
    input: &ImageArtifactInput,
    binaries: &ArtifactManifest,
    architectures: &[Architecture],
) -> Result<ImageManifest> {
    let mut names = vec![MANIFEST.into()];
    names.extend(
        architectures
            .iter()
            .map(|architecture| archive_name(&input.identity.version, *architecture)),
    );
    artifacts::check_files(directory, &names)?;
    let path = directory.join(MANIFEST);
    let manifest: ImageManifest = serde_json::from_slice(
        &fs::read(&path).context(FileSnafu { path: &path })?,
    )
    .context(JsonSnafu {
        subject: "image artifact manifest",
    })?;
    ensure!(
        manifest.schema_version == 1
            && manifest.version == input.identity.version
            && manifest.source_commit == input.identity.source_commit
            && manifest.rust_toolchain == input.identity.rust_toolchain
            && manifest.base_image == base_image()?,
        InvalidInputSnafu {
            message: "image manifest build identity differs"
        }
    );
    ensure!(
        manifest.targets.len() == architectures.len(),
        InvalidInputSnafu {
            message: "incomplete image target list"
        }
    );
    for (target, architecture) in manifest.targets.iter().zip(architectures) {
        let binary = binary_target(binaries, *architecture)?;
        let name = archive_name(&input.identity.version, *architecture);
        ensure!(
            target.architecture == *architecture
                && target.archive == name
                && target.binary_archive_sha256 == binary.archive_sha256
                && target.archive_sha256 == artifacts::file_sha256(&directory.join(&name))?,
            InvalidInputSnafu {
                message: "image archive identity or hash differs"
            }
        );
        ensure!(
            *target == describe_image(directory, input, binary)?,
            InvalidInputSnafu {
                message: "image metadata, files, or manifest digest differ"
            }
        );
    }
    Ok(manifest)
}

pub(crate) fn verify_downloaded_image(
    directory: &Path,
    binary_artifacts: &Path,
    identity: &BuildIdentity,
    binaries: &ArtifactManifest,
    architecture: Architecture,
    manifest_digest: &str,
) -> Result<()> {
    let input = ImageArtifactInput {
        directory: directory.into(),
        binary_artifacts: binary_artifacts.into(),
        identity: identity.clone(),
    };
    let observed = describe_image(directory, &input, binary_target(binaries, architecture)?)?;
    // Exporting changes the outer tar file, while the OCI manifest and its blobs stay identical.
    ensure!(
        observed.manifest_digest == manifest_digest,
        InvalidInputSnafu {
            message: "downloaded image has a different platform manifest digest"
        }
    );
    Ok(())
}

fn describe_image(
    directory: &Path,
    input: &ImageArtifactInput,
    binary: &ArtifactTarget,
) -> Result<ImageTarget> {
    let archive = archive_name(&input.identity.version, binary.architecture);
    let path = directory.join(&archive);
    artifacts::require_regular_file(&path)?;
    let archive_sha256 = artifacts::file_sha256(&path)?;
    let temporary = tempfile::tempdir().context(IoSnafu {
        operation: "create the image verification directory",
    })?;
    let layout = temporary.path().join("layout");
    let reference = format!("oci:{}:dar", transport_path(&layout)?);
    let digest_path = temporary.path().join("digest");
    // Omitting the source tag rejects archives containing more than one image.
    run_command(
        Command::new("skopeo")
            .args(["copy", "--all", "--preserve-digests", "--digestfile"])
            .arg(&digest_path)
            .arg(format!("oci-archive:{}", transport_path(&path)?))
            .arg(&reference),
    )?;
    let manifest_digest =
        fs::read_to_string(&digest_path).context(FileSnafu { path: &digest_path })?;
    let manifest_digest = manifest_digest.trim().to_owned();
    let raw = command_output(Command::new("skopeo").args(["inspect", "--raw", &reference]))?;
    let manifest: Value = serde_json::from_str(&raw).context(JsonSnafu {
        subject: "OCI image manifest",
    })?;
    ensure!(
        manifest["schemaVersion"] == 2
            && manifest["mediaType"] == "application/vnd.oci.image.manifest.v1+json"
            && manifest_digest == format!("sha256:{:x}", Sha256::digest(raw.as_bytes())),
        InvalidInputSnafu {
            message: "expected a single OCI platform manifest with its preserved digest"
        }
    );
    let raw = command_output(Command::new("skopeo").args(["inspect", "--config", &reference]))?;
    let configuration: Value = serde_json::from_str(&raw).context(JsonSnafu {
        subject: "OCI image configuration",
    })?;
    check_configuration(&configuration, &input.identity, binary.architecture)?;
    let bundle = temporary.path().join("bundle");
    run_command(
        Command::new("umoci")
            .args(["unpack", "--rootless", "--image"])
            .arg(format!("{}:dar", transport_path(&layout)?))
            .arg(&bundle),
    )?;
    let expected = temporary.path().join("binary");
    artifacts::extract_verified_archive(&input.binary_artifacts, &expected, binary)?;
    check_image_files(&bundle.join("rootfs"), &expected)?;
    ensure!(
        artifacts::file_sha256(&path)? == archive_sha256,
        InvalidInputSnafu {
            message: "OCI archive changed during verification"
        }
    );
    Ok(ImageTarget {
        os: "linux".into(),
        architecture: binary.architecture,
        binary_archive_sha256: binary.archive_sha256.clone(),
        dar_sha256: binary.dar_sha256.clone(),
        archive,
        archive_sha256,
        manifest_digest,
        configuration: configuration["config"].clone(),
    })
}

fn check_configuration(
    configuration: &Value,
    identity: &BuildIdentity,
    architecture: Architecture,
) -> Result<()> {
    ensure!(
        configuration["os"] == "linux" && configuration["architecture"] == architecture.name(),
        InvalidInputSnafu {
            message: "image platform differs from the binary artifact"
        }
    );
    let config = &configuration["config"];
    for (field, expected) in [
        ("User", json!("65532:65532")),
        ("WorkingDir", json!("/work")),
        ("Entrypoint", json!(["/usr/local/bin/dar"])),
        ("Cmd", json!(["--help"])),
        (
            "Env",
            json!(["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"]),
        ),
    ] {
        ensure!(
            config[field] == expected,
            InvalidInputSnafu {
                message: format!("unexpected image configuration: {field}")
            }
        );
    }
    for field in ["Volumes", "ExposedPorts", "OnBuild"] {
        ensure!(
            config[field].is_null() || config[field] == json!({}) || config[field] == json!([]),
            InvalidInputSnafu {
                message: format!("unexpected image configuration: {field}")
            }
        );
    }
    for (name, expected) in [
        ("source", "https://github.com/mag1cfrog/delta-arrow-reader"),
        ("licenses", "Apache-2.0"),
        ("revision", identity.source_commit.as_str()),
        ("version", identity.version.as_str()),
        ("base.name", base_image()?),
    ] {
        ensure!(
            config["Labels"][format!("org.opencontainers.image.{name}")] == expected,
            InvalidInputSnafu {
                message: format!("image {name} label differs")
            }
        );
    }
    Ok(())
}

#[cfg(unix)]
fn check_image_files(rootfs: &Path, expected: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let rootfs = rootfs.canonicalize().context(FileSnafu { path: rootfs })?;
    for (name, mode) in artifacts::MEMBERS {
        let path = rootfs.join(if name == "dar" {
            "usr/local/bin/dar".into()
        } else {
            format!("usr/share/doc/dar/{name}")
        });
        artifacts::require_regular_file(&path)?;
        ensure!(
            path.canonicalize()
                .context(FileSnafu { path: &path })?
                .starts_with(&rootfs),
            InvalidInputSnafu {
                message: format!("image file escapes the root filesystem: {name}")
            }
        );
        ensure!(
            artifacts::file_sha256(&path)? == artifacts::file_sha256(&expected.join(name))?,
            InvalidInputSnafu {
                message: format!("image {name} differs from the binary archive")
            }
        );
        ensure!(
            fs::metadata(&path)
                .context(FileSnafu { path: &path })?
                .permissions()
                .mode()
                & 0o7777
                == mode,
            InvalidInputSnafu {
                message: format!("image {name} permissions differ from the binary archive")
            }
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_image_files(_rootfs: &Path, _expected: &Path) -> Result<()> {
    InvalidInputSnafu {
        message: "image verification requires Unix file permissions",
    }
    .fail()
}
