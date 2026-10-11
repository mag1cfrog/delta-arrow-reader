use std::{
    env,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use clap::Args;
use serde::Serialize;
use serde_json::json;
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::{ArtifactManifest, file_sha256, package},
    binary::{self, BinaryMetadata, RUNTIME_PACKAGES},
    command::{command_output, run_command},
    error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
    notices::{self, NoticeArgs},
    platform::Architecture,
    repository::{WorkspaceManifest, read_toml, repository_root},
};

const DEBIAN_IMAGE: &str = "docker.io/library/debian@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587";

#[derive(Args, Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct BuildIdentity {
    /// Stable workspace version, without the v prefix.
    #[arg(long)]
    pub(crate) version: String,
    /// Full lowercase source commit SHA.
    #[arg(long)]
    pub(crate) source_commit: String,
    /// Exact numeric Rust release used for the binary build.
    #[arg(long)]
    pub(crate) rust_toolchain: String,
}

#[derive(Args)]
pub(crate) struct PrepareArgs {
    #[arg(long)]
    source_commit: String,
    #[arg(long)]
    expected_version: Option<String>,
}

#[derive(Args)]
pub(crate) struct BuildArgs {
    #[arg(long)]
    source_commit: String,
    #[arg(long)]
    architecture: Architecture,
    /// Cargo build directory; keep it outside the checkout or under an ignored path.
    #[arg(long, default_value = "target/cli-build")]
    target_dir: PathBuf,
    /// Installed Rust toolchain; defaults to Cargo.toml's rust-version.
    #[arg(long)]
    rust_toolchain: Option<String>,
}

#[derive(Args)]
pub(crate) struct BuildArchiveArgs {
    #[command(flatten)]
    identity: BuildIdentity,
    #[arg(long)]
    architecture: Architecture,
    #[arg(long, default_value = "target/cli-build")]
    target_dir: PathBuf,
    /// New directory for the archive and its platform manifest.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Serialize)]
struct BuildMetadata {
    schema_version: u32,
    #[serde(flatten)]
    identity: BuildIdentity,
    #[serde(flatten)]
    binary: BinaryMetadata,
    runtime_packages: [&'static str; 3],
}

impl PrepareArgs {
    pub(crate) fn run(self) -> Result<()> {
        print_json(&self.resolve(&repository_root())?)
    }

    fn resolve(self, repository: &Path) -> Result<serde_json::Value> {
        check_source(repository, &self.source_commit)?;
        let manifest: WorkspaceManifest = read_toml(&repository.join("Cargo.toml"))?;
        if let Some(expected) = self.expected_version.filter(|version| !version.is_empty()) {
            ensure!(
                manifest.workspace.package.version == expected,
                InvalidInputSnafu {
                    message: format!(
                        "expected workspace version {expected:?}, got {:?}",
                        manifest.workspace.package.version
                    )
                }
            );
        }
        let compiler = command_output(Command::new("rustup").args([
            "run",
            &manifest.workspace.package.rust_version,
            "rustc",
            "--version",
            "--verbose",
        ]))?;
        let release = compiler_release(&compiler, &manifest.workspace.package.rust_version)?;
        let identity = BuildIdentity {
            version: manifest.workspace.package.version,
            source_commit: self.source_commit,
            rust_toolchain: release,
        };
        identity.validate()?;
        Ok(json!({
            "source-commit": identity.source_commit,
            "version": identity.version,
            "rust-toolchain": identity.rust_toolchain,
            "image": DEBIAN_IMAGE,
            "about-version": manifest.workspace.metadata.cli_release.cargo_about_version,
        }))
    }
}

impl BuildArgs {
    pub(crate) fn run(self) -> Result<()> {
        let metadata = build_binary(&repository_root(), &self)?;
        print_json(&metadata)
    }
}

impl BuildArchiveArgs {
    pub(crate) fn run(self) -> Result<()> {
        print_json(&self.build(&repository_root())?)
    }

    fn build(self, repository: &Path) -> Result<ArtifactManifest> {
        self.identity.validate()?;
        let target_dir = std::path::absolute(&self.target_dir).context(FileSnafu {
            path: &self.target_dir,
        })?;
        let metadata = build_binary(
            repository,
            &BuildArgs {
                source_commit: self.identity.source_commit.clone(),
                architecture: self.architecture,
                target_dir: target_dir.clone(),
                rust_toolchain: Some(self.identity.rust_toolchain.clone()),
            },
        )?;
        ensure!(
            metadata.identity == self.identity,
            InvalidInputSnafu {
                message: "build version, source commit, or Rust toolchain differs"
            }
        );
        let temporary = tempfile::Builder::new()
            .prefix("cli-notices-")
            .tempdir()
            .context(FileSnafu {
                path: env::temp_dir(),
            })?;
        let notices = temporary.path().join("notices.txt");
        notices::generate(
            repository,
            NoticeArgs {
                architecture: self.architecture,
                rust_toolchain: Some(self.identity.rust_toolchain.clone()),
                output: notices.clone(),
            },
        )?;
        check_source(repository, &self.identity.source_commit)?;
        let binary = target_dir
            .join(self.architecture.rust_target())
            .join("release/dar");
        package_validated_build(
            &binary,
            &notices,
            &self.output,
            self.architecture,
            &metadata,
        )
    }
}

fn package_validated_build(
    binary: &Path,
    notices: &Path,
    output: &Path,
    architecture: Architecture,
    metadata: &BuildMetadata,
) -> Result<ArtifactManifest> {
    let manifest = package(
        binary,
        notices,
        output,
        &metadata.identity,
        architecture,
        &metadata.binary.elf.runtime_dependencies,
    )?;
    ensure!(
        manifest.targets[0].dar_sha256 == metadata.binary.dar_sha256,
        InvalidInputSnafu {
            message: "packaged binary differs from the validated build"
        }
    );
    Ok(manifest)
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).context(JsonSnafu {
            subject: "CLI build metadata"
        })?
    );
    Ok(())
}

pub(crate) fn is_full_commit_sha(commit: &str) -> bool {
    commit.len() == 40
        && commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn check_source(repository: &Path, source_commit: &str) -> Result<()> {
    ensure!(
        is_full_commit_sha(source_commit),
        InvalidInputSnafu {
            message: format!("invalid source commit: {source_commit:?}")
        }
    );
    let head = command_output(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repository),
    )?;
    ensure!(
        head.trim() == source_commit,
        InvalidInputSnafu {
            message: format!(
                "requested source commit {source_commit:?}, checked out {}",
                head.trim()
            )
        }
    );
    let status = command_output(
        Command::new("git")
            .args(["status", "--porcelain", "--untracked-files=all"])
            .current_dir(repository),
    )?;
    ensure!(
        status.is_empty(),
        InvalidInputSnafu {
            message: "the source checkout must have no modified or untracked files"
        }
    );
    Ok(())
}

fn compiler_field<'a>(output: &'a str, name: &str) -> Result<&'a str> {
    output
        .lines()
        .filter_map(|line| line.split_once(": "))
        .find_map(|(key, value)| (key == name).then_some(value))
        .ok_or_else(|| {
            InvalidInputSnafu {
                message: format!("rustc did not report {name}"),
            }
            .build()
        })
}

fn compiler_release(output: &str, declared: &str) -> Result<String> {
    let expected: Vec<_> = declared.split('.').collect();
    ensure!(
        (2..=3).contains(&expected.len())
            && expected
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())),
        InvalidInputSnafu {
            message: format!("expected a numeric Rust toolchain version, got {declared:?}")
        }
    );
    let release = compiler_field(output, "release")?;
    ensure!(
        release.split('.').take(expected.len()).eq(expected),
        InvalidInputSnafu {
            message: format!("expected Rust {declared}, got {release}")
        }
    );
    Ok(release.into())
}

fn build_binary(repository: &Path, args: &BuildArgs) -> Result<BuildMetadata> {
    binary::check_build_environment(args.architecture)?;
    check_source(repository, &args.source_commit)?;
    let manifest: WorkspaceManifest = read_toml(&repository.join("Cargo.toml"))?;
    let toolchain = args
        .rust_toolchain
        .as_deref()
        .unwrap_or(&manifest.workspace.package.rust_version);
    let compiler = command_output(
        rustup_command(toolchain, args.architecture, repository).args([
            "rustc",
            "--version",
            "--verbose",
        ]),
    )?;
    let rust_target = args.architecture.rust_target();
    ensure!(
        compiler_field(&compiler, "host")? == rust_target,
        InvalidInputSnafu {
            message: format!("expected a compiler running on {rust_target}")
        }
    );
    let identity = BuildIdentity {
        version: manifest.workspace.package.version,
        source_commit: args.source_commit.clone(),
        rust_toolchain: compiler_release(&compiler, &manifest.workspace.package.rust_version)?,
    };
    identity.validate()?;
    let target_dir = std::path::absolute(&args.target_dir).context(FileSnafu {
        path: &args.target_dir,
    })?;
    let metadata_file = target_dir.join(rust_target).join("build-info.json");
    match fs::remove_file(&metadata_file) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).context(FileSnafu {
                path: &metadata_file,
            });
        }
    }
    run_command(
        rustup_command(toolchain, args.architecture, repository)
            .args([
                "cargo",
                "build",
                "--release",
                "--locked",
                "--package",
                "delta-arrow-reader-cli",
                "--target",
                rust_target,
                "--target-dir",
            ])
            .arg(&target_dir),
    )?;
    check_source(repository, &args.source_commit)?;
    let binary_path = target_dir.join(rust_target).join("release/dar");
    let binary = binary::check_binary(
        &binary_path,
        args.architecture,
        &identity.version,
        &file_sha256(&binary_path)?,
    )?;
    let metadata = BuildMetadata {
        schema_version: 1,
        identity,
        binary,
        runtime_packages: RUNTIME_PACKAGES,
    };
    let mut text = serde_json::to_vec_pretty(&metadata).context(JsonSnafu {
        subject: "CLI build metadata",
    })?;
    text.push(b'\n');
    fs::write(&metadata_file, text).context(FileSnafu {
        path: &metadata_file,
    })?;
    Ok(metadata)
}

pub(crate) fn is_stable_version(version: &str) -> bool {
    is_numeric_version(version)
        && version
            .split('.')
            .all(|part| part == "0" || !part.starts_with('0'))
}

fn is_numeric_version(version: &str) -> bool {
    version.split('.').count() == 3
        && version
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

impl BuildIdentity {
    pub(crate) fn validate(&self) -> Result<()> {
        for (name, value, valid) in [
            ("version", &self.version, is_stable_version(&self.version)),
            (
                "source commit",
                &self.source_commit,
                is_full_commit_sha(&self.source_commit),
            ),
            (
                "Rust toolchain",
                &self.rust_toolchain,
                is_numeric_version(&self.rust_toolchain),
            ),
        ] {
            ensure!(
                valid,
                InvalidInputSnafu {
                    message: format!("invalid {name}: {value:?}")
                }
            );
        }
        Ok(())
    }
}

pub(crate) fn rustup_command(
    toolchain: &str,
    architecture: Architecture,
    repository: &Path,
) -> Command {
    let mut command = Command::new("rustup");
    // Match the binary build's environment so flags cannot change dependency resolution.
    command
        .env_clear()
        .envs(env::vars_os().filter(|(key, _)| keep_build_variable(key)));
    let (cpu, cflags) = match architecture {
        Architecture::Amd64 => ("x86-64", "-march=x86-64 -mtune=generic"),
        Architecture::Arm64 => ("generic", "-march=armv8-a -mtune=generic"),
    };
    command
        .current_dir(repository)
        .args(["run", toolchain])
        .env("LC_ALL", "C")
        .env("RUSTUP_TOOLCHAIN", toolchain)
        .env("RUSTFLAGS", format!("-Ctarget-cpu={cpu}"))
        .env("CFLAGS", cflags)
        .env("CXXFLAGS", cflags);
    command
}

fn keep_build_variable(key: &OsStr) -> bool {
    let Some(key) = key.to_str() else {
        return false;
    };
    matches!(
        key,
        "PATH" | "HOME" | "CARGO_HOME" | "RUSTUP_HOME" | "CARGO_BUILD_JOBS" | "CARGO_NET_OFFLINE"
    ) || matches!(
        key.to_ascii_lowercase().as_str(),
        "http_proxy" | "https_proxy" | "no_proxy"
    )
}

#[cfg(test)]
mod tests;
