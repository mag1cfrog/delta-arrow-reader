use std::{collections::BTreeSet, fs, path::PathBuf, process::Command};

use clap::Args;
use snafu::{ResultExt, ensure};

use crate::{
    artifacts,
    binary::inspect_elf,
    build::BuildIdentity,
    command::run_command,
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, Result},
    platform::Architecture,
};

const DOCKERFILE: &str = include_str!("../../crates/delta-arrow-reader-cli/Dockerfile");

pub(crate) fn base_image() -> Result<&'static str> {
    DOCKERFILE
        .lines()
        .find_map(|line| line.strip_prefix("ARG BASE_IMAGE="))
        .ok_or_else(|| {
            InvalidInputSnafu {
                message: "image Dockerfile has no default BASE_IMAGE",
            }
            .build()
        })
}

#[derive(Args)]
pub(crate) struct BuildImageArgs {
    /// Directory containing both archives, cli-artifacts.json, and SHA256SUMS.
    directory: PathBuf,
    #[command(flatten)]
    identity: BuildIdentity,
    #[arg(long)]
    architecture: Architecture,
    /// Local image tag to create or replace.
    #[arg(long)]
    tag: String,
}

impl BuildImageArgs {
    pub(crate) fn run(self) -> Result<()> {
        self.identity.validate()?;
        let manifest = artifacts::verify(&self.directory, &self.identity)?;
        let target = manifest
            .targets
            .iter()
            .find(|target| target.architecture == self.architecture)
            .ok_or_else(|| {
                InvalidInputSnafu {
                    message: "selected architecture is missing from the binary artifact set",
                }
                .build()
            })?;
        let temporary = tempfile::Builder::new()
            .prefix("dar-image-")
            .tempdir()
            .context(IoSnafu {
                operation: "create the temporary image build directory",
            })?;
        let context = temporary.path().join("context");
        artifacts::extract_verified_archive(&self.directory, &context, target)?;
        let metadata = inspect_elf(&context.join("dar"), self.architecture)?;
        ensure!(
            metadata.dar_sha256 == target.dar_sha256
                && metadata.elf.runtime_dependencies == target.runtime_dependencies,
            InvalidInputSnafu {
                message: "binary ELF metadata differs from its artifact manifest"
            }
        );
        let packages = debian_runtime_packages(&metadata.elf.runtime_dependencies)?;
        let packages_path = context.join("runtime-packages.txt");
        fs::write(&packages_path, packages.join("\n") + "\n").context(FileSnafu {
            path: &packages_path,
        })?;
        let dockerfile = context.join("Dockerfile");
        fs::write(&dockerfile, DOCKERFILE).context(FileSnafu { path: &dockerfile })?;
        run_command(
            Command::new("docker")
                .args(["build", "--platform"])
                .arg(format!("linux/{}", self.architecture.name()))
                .arg("--tag")
                .arg(&self.tag)
                .arg("--build-arg")
                .arg(format!("CLI_VERSION={}", self.identity.version))
                .arg("--build-arg")
                .arg(format!("CLI_SOURCE_COMMIT={}", self.identity.source_commit))
                .arg(&context),
        )?;
        println!("Built CLI image {}", self.tag);
        Ok(())
    }
}

fn debian_runtime_packages(dependencies: &[String]) -> Result<Vec<&'static str>> {
    let mut packages = BTreeSet::from(["ca-certificates"]);
    for library in dependencies {
        packages.insert(match library.as_str() {
            "libc.so.6" | "libm.so.6" | "ld-linux-x86-64.so.2" | "ld-linux-aarch64.so.1" => "libc6",
            "libgcc_s.so.1" => "libgcc-s1",
            _ => {
                return InvalidInputSnafu {
                    message: format!("no supported Debian runtime package for {library:?}"),
                }
                .fail();
            }
        });
    }
    Ok(packages.into_iter().collect())
}
