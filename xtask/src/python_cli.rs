use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use clap::Args;
use serde::Deserialize;
use snafu::{ResultExt, ensure};

use crate::{
    build::{compiler_field, compiler_release},
    command::{command_output, output_with_timeout},
    error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
    repository::{WorkspaceManifest, read_toml, repository_root},
};

const EXECUTABLE_NAMES: [&str; 2] = ["delta-arrow-reader", "delta-arrow-reader.exe"];

#[derive(Args)]
pub(crate) struct StagePythonCliArgs {
    /// Generated wheel data directory; the executable goes in scripts/.
    ///
    /// Its parent must be writable and on the same filesystem.
    #[arg(long)]
    destination: PathBuf,
    /// Native Rust target: Linux x86_64, macOS arm64, or Windows x86_64 MSVC.
    #[arg(long)]
    target: String,
    /// Cargo build profile.
    #[arg(long, default_value = "release", value_parser = ["dev", "release"])]
    profile: String,
}

impl StagePythonCliArgs {
    pub(crate) fn run(self) -> Result<()> {
        let repository = repository_root();
        let executable = stage_cli(&self.destination, &self.target, || {
            self.build_cli(&repository)
        })?;
        eprintln!("Staged {}", executable.display());
        Ok(())
    }

    fn build_cli(&self, repository: &Path) -> Result<(PathBuf, String)> {
        let manifest: WorkspaceManifest = read_toml(&repository.join("Cargo.toml"))?;
        let compiler = command_output(
            Command::new(env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
                .arg("-vV")
                .current_dir(repository),
        )?;
        compiler_release(&compiler, &manifest.workspace.package.rust_version)?;
        check_native_target(&self.target, compiler_field(&compiler, "host")?)?;
        let output = command_output(
            Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
                .current_dir(repository)
                .args([
                    "build",
                    "--locked",
                    "--package",
                    "delta-arrow-reader-cli",
                    "--bin",
                    "dar",
                    "--target",
                    &self.target,
                    "--profile",
                    &self.profile,
                    "--message-format=json-render-diagnostics",
                ]),
        )?;
        Ok((
            built_executable(&output)?,
            manifest.workspace.package.version,
        ))
    }
}

fn executable_name(target: &str) -> Result<&'static str> {
    match target {
        "x86_64-unknown-linux-gnu" | "aarch64-apple-darwin" => Ok(EXECUTABLE_NAMES[0]),
        "x86_64-pc-windows-msvc" => Ok(EXECUTABLE_NAMES[1]),
        _ => InvalidInputSnafu {
            message: format!("unsupported Python CLI target: {target}"),
        }
        .fail(),
    }
}

fn check_native_target(target: &str, host: &str) -> Result<()> {
    executable_name(target)?;
    ensure!(
        target == host,
        InvalidInputSnafu {
            message: format!(
                "Python CLI target {target} requires a native compiler; host is {host}"
            )
        }
    );
    Ok(())
}

#[derive(Deserialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
enum CargoMessage {
    CompilerArtifact {
        target: CargoTarget,
        executable: Option<PathBuf>,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct CargoTarget {
    name: String,
    kind: Vec<String>,
}

fn built_executable(output: &str) -> Result<PathBuf> {
    let mut executable = None;
    for line in output.lines() {
        let message = serde_json::from_str::<CargoMessage>(line).context(JsonSnafu {
            subject: "Cargo build output",
        })?;
        if let CargoMessage::CompilerArtifact {
            target,
            executable: Some(path),
        } = message
            && target.name == "dar"
            && target.kind == ["bin"]
        {
            ensure!(
                executable.replace(path).is_none(),
                InvalidInputSnafu {
                    message: "Cargo reported more than one dar executable"
                }
            );
        }
    }
    executable.ok_or_else(|| {
        InvalidInputSnafu {
            message: "Cargo did not report a built dar executable",
        }
        .build()
    })
}

fn stage_cli(
    destination: &Path,
    target: &str,
    build: impl FnOnce() -> Result<(PathBuf, String)>,
) -> Result<PathBuf> {
    // Trailing separators or "/." must not bypass the directory symlink check.
    let destination: PathBuf = destination.components().collect();
    let scripts = destination.join("scripts");
    for directory in [destination.as_path(), scripts.as_path()] {
        fs::create_dir_all(directory).context(FileSnafu { path: directory })?;
        ensure!(
            fs::symlink_metadata(directory)
                .context(FileSnafu { path: directory })?
                .is_dir(),
            InvalidInputSnafu {
                message: format!(
                    "staging directory must not be a symlink: {}",
                    directory.display()
                )
            }
        );
    }
    let resolved_destination =
        fs::canonicalize(&destination).context(FileSnafu { path: &destination })?;
    let parent = resolved_destination.parent().ok_or_else(|| {
        InvalidInputSnafu {
            message: "wheel data directory must not be a filesystem root",
        }
        .build()
    })?;
    // Clear both platform names before any fallible build or identity checks.
    for name in EXECUTABLE_NAMES {
        let path = scripts.join(name);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context(FileSnafu { path }),
        }
    }
    let name = executable_name(target)?;
    let (built, version) = build()?;
    // A killed verification must not leave temporary entries in the wheel data directory.
    let temporary = tempfile::Builder::new()
        .prefix(".python-cli-")
        .tempdir_in(parent)
        .context(FileSnafu { path: parent })?;
    let staged = temporary.path().join(name);
    fs::copy(&built, &staged).context(FileSnafu { path: &staged })?;
    let output = output_with_timeout(
        Command::new(&staged).arg("--version"),
        Duration::from_secs(15),
    )?;
    ensure!(
        output.status.success()
            && output.stdout == format!("delta-arrow-reader {version}\n").as_bytes()
            && output.stderr.is_empty(),
        InvalidInputSnafu {
            message: format!(
                "staged Python CLI must report delta-arrow-reader {version} with no errors"
            )
        }
    );
    let executable = scripts.join(name);
    fs::rename(&staged, &executable).context(FileSnafu { path: &executable })?;
    Ok(executable)
}

#[cfg(test)]
mod tests;
