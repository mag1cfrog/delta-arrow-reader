use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use clap::{Args, Subcommand};
use flate2::{Compression, GzBuilder, read::MultiGzDecoder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snafu::{ResultExt, ensure};
use tar::{Archive, Builder, Header};

use crate::{
    binary::MIN_GLIBC,
    build::BuildIdentity,
    error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
    platform::Architecture,
};

const MANIFEST: &str = "cli-artifacts.json";
pub(crate) const MEMBERS: [(&str, u32); 3] =
    [("dar", 0o755), ("LICENSE", 0o644), ("NOTICE", 0o644)];
const LICENSE: &[u8] = include_bytes!("../../LICENSE");
const NOTICE: &[u8] = include_bytes!("../../NOTICE");

#[derive(Subcommand)]
pub(crate) enum ArtifactTask {
    /// Package one validated binary and its declared build metadata.
    Package {
        #[command(flatten)]
        input: ArtifactInput,
        #[arg(long)]
        binary: PathBuf,
        #[arg(long)]
        dependency_notices: PathBuf,
        #[arg(long)]
        architecture: Architecture,
        /// ELF library name; repeat for every required library.
        #[arg(long = "runtime-dependency", required = true)]
        runtime_dependencies: Vec<String>,
    },
    /// Validate and extract one platform archive into a new directory.
    Extract {
        #[command(flatten)]
        input: ArtifactInput,
        #[arg(long)]
        architecture: Architecture,
        #[arg(long)]
        destination: PathBuf,
    },
    /// Combine both platforms without rebuilding or repacking their archives.
    Assemble {
        #[command(flatten)]
        input: ArtifactInput,
        /// Directory containing exactly the amd64 and arm64 input directories.
        #[arg(long)]
        inputs: PathBuf,
    },
    /// Verify the declared identity, exact file set, archive contents, and hashes.
    Verify {
        #[command(flatten)]
        input: ArtifactInput,
        /// Verify one platform; omit to require the complete two-platform set.
        #[arg(long)]
        architecture: Option<Architecture>,
    },
}

#[derive(Args)]
pub(crate) struct ArtifactInput {
    /// Artifact directory; package and assemble require a new destination.
    directory: PathBuf,
    #[command(flatten)]
    identity: BuildIdentity,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactManifest {
    schema_version: u32,
    version: String,
    source_commit: String,
    rust_toolchain: String,
    pub(crate) targets: Vec<ArtifactTarget>,
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactTarget {
    os: String,
    pub(crate) architecture: Architecture,
    rust_target: String,
    libc: String,
    min_glibc: String,
    pub(crate) runtime_dependencies: Vec<String>,
    archive: String,
    pub(crate) archive_sha256: String,
    pub(crate) dar_sha256: String,
}

impl ArtifactTask {
    pub(crate) fn run(self) -> Result<()> {
        let input = match &self {
            Self::Package { input, .. }
            | Self::Extract { input, .. }
            | Self::Assemble { input, .. }
            | Self::Verify { input, .. } => input,
        };
        input.identity.validate()?;
        let manifest = match self {
            Self::Package {
                input,
                binary,
                dependency_notices,
                architecture,
                mut runtime_dependencies,
            } => {
                runtime_dependencies.sort();
                package(
                    &binary,
                    &dependency_notices,
                    &input.directory,
                    &input.identity,
                    architecture,
                    &runtime_dependencies,
                )?
            }
            Self::Extract {
                input,
                architecture,
                destination,
            } => extract(
                &input.directory,
                &destination,
                &input.identity,
                architecture,
            )?,
            Self::Assemble { input, inputs } => {
                assemble(&inputs, &input.directory, &input.identity)?
            }
            Self::Verify {
                input,
                architecture,
            } => match architecture {
                Some(architecture) => {
                    verify_platform(&input.directory, &input.identity, architecture)?
                }
                None => verify(&input.directory, &input.identity)?,
            },
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&manifest).context(JsonSnafu {
                subject: "artifact manifest"
            })?
        );
        Ok(())
    }
}

impl BuildIdentity {
    fn manifest(&self, targets: Vec<ArtifactTarget>) -> ArtifactManifest {
        ArtifactManifest {
            schema_version: 1,
            version: self.version.clone(),
            source_commit: self.source_commit.clone(),
            rust_toolchain: self.rust_toolchain.clone(),
            targets,
        }
    }
}

fn archive_name(identity: &BuildIdentity, architecture: Architecture) -> String {
    format!(
        "dar-v{}-linux-{}-gnu.tar.gz",
        identity.version,
        architecture.name()
    )
}

fn reader_sha256(mut reader: impl Read) -> io::Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("{hash:x}", hash = hash.finalize()))
}

pub(crate) fn file_sha256(path: &Path) -> Result<String> {
    reader_sha256(File::open(path).context(FileSnafu { path })?).context(FileSnafu { path })
}

pub(crate) fn require_regular_file(path: &Path) -> Result<()> {
    ensure!(
        fs::symlink_metadata(path)
            .context(FileSnafu { path })?
            .is_file(),
        InvalidInputSnafu {
            message: format!("expected a regular file: {}", path.display())
        }
    );
    Ok(())
}

pub(crate) fn check_directory(directory: &Path, expected: &[String]) -> Result<()> {
    ensure!(
        fs::symlink_metadata(directory)
            .context(FileSnafu { path: directory })?
            .is_dir(),
        InvalidInputSnafu {
            message: format!("expected a directory: {}", directory.display())
        }
    );
    let mut actual = Vec::new();
    for entry in fs::read_dir(directory).context(FileSnafu { path: directory })? {
        actual.push(entry.context(FileSnafu { path: directory })?.file_name());
    }
    let mut expected: Vec<_> = expected.iter().map(OsString::from).collect();
    actual.sort();
    expected.sort();
    ensure!(
        actual == expected,
        InvalidInputSnafu {
            message: format!(
                "{}: expected {expected:?}, got {actual:?}",
                directory.display()
            ),
        }
    );
    Ok(())
}

pub(crate) fn check_files(directory: &Path, expected: &[String]) -> Result<()> {
    check_directory(directory, expected)?;
    for name in expected {
        require_regular_file(&directory.join(name))?;
    }
    Ok(())
}

pub(crate) fn create_directory(directory: &Path) -> Result<()> {
    if let Some(parent) = directory.parent() {
        fs::create_dir_all(parent).context(FileSnafu { path: parent })?;
    }
    fs::create_dir(directory).context(FileSnafu { path: directory })
}

pub(crate) fn write_new_file(path: &Path, contents: &[u8]) -> Result<()> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .context(FileSnafu { path })?
        .write_all(contents)
        .context(FileSnafu { path })
}

fn write_manifest(directory: &Path, manifest: &ArtifactManifest) -> Result<()> {
    let mut contents = serde_json::to_vec_pretty(manifest).context(JsonSnafu {
        subject: "artifact manifest",
    })?;
    contents.push(b'\n');
    write_new_file(&directory.join(MANIFEST), &contents)
}

pub(crate) fn check_runtime_dependencies(dependencies: &[String]) -> Result<()> {
    ensure!(
        !dependencies.is_empty()
            && dependencies.windows(2).all(|pair| pair[0] < pair[1])
            && dependencies.iter().all(|name| {
                let Some((library, suffix)) = name.rsplit_once(".so") else {
                    return false;
                };
                !library.is_empty()
                    && library
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_+.-".contains(&byte))
                    && (suffix.is_empty()
                        || suffix.strip_prefix('.').is_some_and(|versions| {
                            versions.split('.').all(|part| {
                                !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())
                            })
                        }))
            }),
        InvalidInputSnafu {
            message: "runtime_dependencies must be sorted, unique ELF library names"
        }
    );
    Ok(())
}

fn check_member<R: Read>(
    entry: &tar::Entry<'_, R>,
    name: &str,
    mode: u32,
    path: &Path,
) -> Result<()> {
    // EntryType normalizes the legacy NUL flag to Regular; the format requires b'0'.
    ensure!(
        entry.path_bytes() == name.as_bytes()
            && entry.header().as_old().linkflag[0] == b'0'
            && entry.header().mode().context(FileSnafu { path })? == mode
            && entry.size() > 0,
        InvalidInputSnafu {
            message: format!(
                "{}: expected regular {name} with mode {mode:o}",
                path.display()
            )
        }
    );
    Ok(())
}

fn check_notice(reader: &mut impl Read, path: &Path) -> Result<()> {
    let prefix = [NOTICE, b"\n"].concat();
    let mut actual = vec![0; prefix.len()];
    reader.read_exact(&mut actual).context(FileSnafu { path })?;
    ensure!(
        actual == prefix,
        InvalidInputSnafu {
            message: "repository NOTICE differs"
        }
    );
    let mut buffer = [0; 64 * 1024];
    let mut has_dependency_text = false;
    loop {
        let count = reader.read(&mut buffer).context(FileSnafu { path })?;
        if count == 0 {
            break;
        }
        has_dependency_text |= buffer[..count]
            .iter()
            .any(|byte| !b" \t\n\r\x0b\x0c".contains(byte));
    }
    ensure!(
        has_dependency_text,
        InvalidInputSnafu {
            message: "dependency notices missing"
        }
    );
    Ok(())
}

fn describe_archive(
    directory: &Path,
    identity: &BuildIdentity,
    architecture: Architecture,
    dependencies: &[String],
) -> Result<ArtifactTarget> {
    check_runtime_dependencies(dependencies)?;
    let filename = archive_name(identity, architecture);
    let path = directory.join(&filename);
    require_regular_file(&path)?;
    let mut archive = Archive::new(MultiGzDecoder::new(
        File::open(&path).context(FileSnafu { path: &path })?,
    ));
    let mut dar_sha256 = String::new();
    let mut content_end = 0;
    {
        // Raw entries expose PAX and GNU extension records so they cannot hide extra metadata.
        let mut entries = archive
            .entries()
            .context(FileSnafu { path: &path })?
            .raw(true);
        for (name, mode) in MEMBERS {
            let mut entry = entries
                .next()
                .ok_or_else(|| {
                    InvalidInputSnafu {
                        message: "archive member is missing",
                    }
                    .build()
                })?
                .context(FileSnafu { path: &path })?;
            check_member(&entry, name, mode, &path)?;
            content_end = entry
                .raw_file_position()
                .checked_add(entry.size())
                .ok_or_else(|| {
                    InvalidInputSnafu {
                        message: "archive size overflow",
                    }
                    .build()
                })?;
            match name {
                "dar" => {
                    dar_sha256 = reader_sha256(&mut entry).context(FileSnafu { path: &path })?
                }
                "LICENSE" => {
                    ensure!(
                        entry.size() == LICENSE.len() as u64,
                        InvalidInputSnafu {
                            message: "repository LICENSE differs"
                        }
                    );
                    let mut text = vec![0; LICENSE.len()];
                    entry
                        .read_exact(&mut text)
                        .context(FileSnafu { path: &path })?;
                    ensure!(
                        text == LICENSE,
                        InvalidInputSnafu {
                            message: "repository LICENSE differs"
                        }
                    );
                }
                _ => check_notice(&mut entry, &path)?,
            }
        }
    }
    // Read past the last member to validate both tar end blocks and every gzip footer.
    let mut compressed = archive.into_inner();
    let mut buffer = [0; 64 * 1024];
    let mut padding_size = 0u64;
    loop {
        let count = compressed
            .read(&mut buffer)
            .context(FileSnafu { path: &path })?;
        if count == 0 {
            break;
        }
        ensure!(
            buffer[..count].iter().all(|byte| *byte == 0),
            InvalidInputSnafu {
                message: "unexpected data after archive members"
            }
        );
        padding_size = padding_size.checked_add(count as u64).ok_or_else(|| {
            InvalidInputSnafu {
                message: "archive size overflow",
            }
            .build()
        })?;
    }
    ensure!(
        padding_size >= (512 - content_end % 512) % 512 + 1024
            && (content_end % 512 + padding_size % 512).is_multiple_of(512),
        InvalidInputSnafu {
            message: "incomplete tar end blocks"
        }
    );
    Ok(ArtifactTarget {
        os: "linux".into(),
        architecture,
        rust_target: architecture.rust_target().into(),
        libc: "glibc".into(),
        min_glibc: MIN_GLIBC.into(),
        runtime_dependencies: dependencies.to_vec(),
        archive: filename,
        archive_sha256: file_sha256(&path)?,
        dar_sha256,
    })
}

fn check_manifest(
    directory: &Path,
    identity: &BuildIdentity,
    architectures: &[Architecture],
) -> Result<ArtifactManifest> {
    let path = directory.join(MANIFEST);
    let text = fs::read(&path).context(FileSnafu { path: &path })?;
    let manifest: ArtifactManifest = serde_json::from_slice(&text).context(JsonSnafu {
        subject: "artifact manifest",
    })?;
    ensure!(
        manifest.schema_version == 1
            && manifest.version == identity.version
            && manifest.source_commit == identity.source_commit
            && manifest.rust_toolchain == identity.rust_toolchain,
        InvalidInputSnafu {
            message: "manifest build identity differs"
        }
    );
    ensure!(
        manifest.targets.len() == architectures.len(),
        InvalidInputSnafu {
            message: "incomplete target list"
        }
    );
    for (architecture, target) in architectures.iter().zip(&manifest.targets) {
        let expected = describe_archive(
            directory,
            identity,
            *architecture,
            &target.runtime_dependencies,
        )?;
        ensure!(
            *target == expected,
            InvalidInputSnafu {
                message: format!("{} metadata or hashes differ", architecture.name())
            }
        );
    }
    Ok(manifest)
}

pub(crate) fn package(
    binary: &Path,
    dependency_notices: &Path,
    directory: &Path,
    identity: &BuildIdentity,
    architecture: Architecture,
    dependencies: &[String],
) -> Result<ArtifactManifest> {
    check_runtime_dependencies(dependencies)?;
    for path in [binary, dependency_notices] {
        require_regular_file(path)?;
        ensure!(
            fs::metadata(path).context(FileSnafu { path })?.len() > 0,
            InvalidInputSnafu {
                message: format!("empty input: {}", path.display())
            }
        );
    }
    let notices = fs::read(dependency_notices).context(FileSnafu {
        path: dependency_notices,
    })?;
    let notice = [NOTICE, b"\n", &notices].concat();
    create_directory(directory)?;
    let path = directory.join(archive_name(identity, architecture));
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .context(FileSnafu { path: &path })?;
    let mut archive = Builder::new(
        GzBuilder::new()
            .mtime(0)
            .operating_system(255)
            .write(output, Compression::best()),
    );
    for (name, mode) in MEMBERS {
        let mut file;
        let (size, source): (u64, &mut dyn Read) = match name {
            "dar" => {
                file = File::open(binary).context(FileSnafu { path: binary })?;
                (
                    file.metadata().context(FileSnafu { path: binary })?.len(),
                    &mut file,
                )
            }
            "LICENSE" => (LICENSE.len() as u64, &mut &LICENSE[..]),
            _ => (notice.len() as u64, &mut &notice[..]),
        };
        let mut header = Header::new_ustar();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(size);
        header.set_mode(mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        archive
            .append_data(&mut header, name, source)
            .context(FileSnafu { path: &path })?;
    }
    archive
        .into_inner()
        .context(FileSnafu { path: &path })?
        .finish()
        .context(FileSnafu { path: &path })?;
    let manifest = identity.manifest(vec![describe_archive(
        directory,
        identity,
        architecture,
        dependencies,
    )?]);
    write_manifest(directory, &manifest)?;
    Ok(manifest)
}

pub(crate) fn verify_platform(
    directory: &Path,
    identity: &BuildIdentity,
    architecture: Architecture,
) -> Result<ArtifactManifest> {
    check_files(
        directory,
        &[archive_name(identity, architecture), MANIFEST.into()],
    )?;
    check_manifest(directory, identity, &[architecture])
}

fn extract(
    directory: &Path,
    destination: &Path,
    identity: &BuildIdentity,
    architecture: Architecture,
) -> Result<ArtifactManifest> {
    let manifest = verify_platform(directory, identity, architecture)?;
    extract_verified_archive(directory, destination, &manifest.targets[0])?;
    Ok(manifest)
}

/// Extract a target from a verified manifest and recheck the copied bytes.
pub(crate) fn extract_verified_archive(
    directory: &Path,
    destination: &Path,
    target: &ArtifactTarget,
) -> Result<()> {
    ensure!(
        cfg!(unix),
        InvalidInputSnafu {
            message: "archive extraction requires Unix file permissions"
        }
    );
    create_directory(destination)?;
    let path = directory.join(&target.archive);
    let mut archive = Archive::new(MultiGzDecoder::new(
        File::open(&path).context(FileSnafu { path: &path })?,
    ));
    let mut entries = archive
        .entries()
        .context(FileSnafu { path: &path })?
        .raw(true);
    for (name, mode) in MEMBERS {
        let mut entry = entries
            .next()
            .ok_or_else(|| {
                InvalidInputSnafu {
                    message: "archive member is missing",
                }
                .build()
            })?
            .context(FileSnafu { path: &path })?;
        check_member(&entry, name, mode, &path)?;
        let output_path = destination.join(name);
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output_path)
            .context(FileSnafu { path: &output_path })?;
        io::copy(&mut entry, &mut output).context(FileSnafu { path: &output_path })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            output
                .set_permissions(fs::Permissions::from_mode(mode))
                .context(FileSnafu { path: &output_path })?;
        }
    }
    ensure!(
        file_sha256(&destination.join("dar"))? == target.dar_sha256
            && file_sha256(&path)? == target.archive_sha256,
        InvalidInputSnafu {
            message: "archive or binary changed during extraction"
        }
    );
    Ok(())
}

fn checksum_text(directory: &Path, identity: &BuildIdentity) -> Result<String> {
    let mut names: Vec<_> = Architecture::ALL
        .into_iter()
        .map(|arch| archive_name(identity, arch))
        .collect();
    names.push(MANIFEST.into());
    let mut text = String::new();
    for name in names {
        text.push_str(&format!(
            "{}  {name}\n",
            file_sha256(&directory.join(&name))?
        ));
    }
    Ok(text)
}

fn assemble(inputs: &Path, directory: &Path, identity: &BuildIdentity) -> Result<ArtifactManifest> {
    check_directory(inputs, &["amd64".into(), "arm64".into()])?;
    let mut targets = Vec::new();
    for architecture in Architecture::ALL {
        targets.extend(
            verify_platform(&inputs.join(architecture.name()), identity, architecture)?.targets,
        );
    }
    create_directory(directory)?;
    for target in &targets {
        let source = inputs
            .join(target.architecture.name())
            .join(&target.archive);
        let destination = directory.join(&target.archive);
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
    }
    write_manifest(directory, &identity.manifest(targets))?;
    write_new_file(
        &directory.join("SHA256SUMS"),
        checksum_text(directory, identity)?.as_bytes(),
    )?;
    verify(directory, identity)
}

pub(crate) fn verify(directory: &Path, identity: &BuildIdentity) -> Result<ArtifactManifest> {
    let mut names: Vec<_> = Architecture::ALL
        .into_iter()
        .map(|arch| archive_name(identity, arch))
        .collect();
    names.extend([MANIFEST.into(), "SHA256SUMS".into()]);
    check_files(directory, &names)?;
    let manifest = check_manifest(directory, identity, &Architecture::ALL)?;
    let path = directory.join("SHA256SUMS");
    ensure!(
        fs::read_to_string(&path).context(FileSnafu { path: &path })?
            == checksum_text(directory, identity)?,
        InvalidInputSnafu {
            message: "SHA256SUMS differs from the complete artifact set"
        }
    );
    Ok(manifest)
}

#[cfg(test)]
mod tests;
