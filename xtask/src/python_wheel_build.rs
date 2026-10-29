use std::{
    env,
    ffi::OsString,
    fs::{self, File},
    path::{Path, PathBuf},
    process::Command,
};

use clap::Args;
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, ensure};

use crate::{
    build::compiler_field,
    command::{command_output, remove_cargo_package_environment},
    error::{FileSnafu, InvalidInputSnafu, Result},
    notices::dependency_notices,
    python_cli::{build_cli, macos_deployment_target, stage_cli},
    repository::{cargo_workspace_root, read_toml},
};

const WHEEL_DATA: &str = "delta_arrow_reader.data";

#[derive(Args)]
pub(crate) struct BuildPythonWheelArgs {
    /// Directory containing the package's pyproject.toml.
    #[arg(long, default_value = ".")]
    project_directory: PathBuf,
    /// Frontend's wheel output directory.
    #[arg(long, required_unless_present = "prepare_only")]
    wheel_directory: Option<PathBuf>,
    /// Prepare data for the release workflow's Maturin action.
    #[arg(long, conflicts_with_all = ["wheel_directory", "editable", "metadata_directory"])]
    prepare_only: bool,
    /// Stage the CLI for the release job that splits the two distributions.
    #[arg(long, requires = "prepare_only")]
    include_cli: bool,
    /// Build an editable wheel.
    #[arg(long)]
    editable: bool,
    /// Previously prepared PEP 517 metadata.
    #[arg(long)]
    metadata_directory: Option<PathBuf>,
    /// Python interpreter selected by the frontend.
    #[arg(long)]
    python: Option<PathBuf>,
    /// Override the frontend's Python interpreter.
    #[arg(long, short = 'i')]
    interpreter: Option<PathBuf>,
    /// Native Rust target; defaults to CARGO_BUILD_TARGET or the compiler host.
    #[arg(long)]
    target: Option<String>,
    /// Cargo profile; defaults to the package's Maturin profile.
    #[arg(long, value_parser = ["dev", "release"])]
    profile: Option<String>,
    /// Cargo artifact directory, relative to the project directory.
    #[arg(long)]
    target_dir: Option<PathBuf>,
    /// Cargo parallel job count.
    #[arg(long, short = 'j')]
    jobs: Option<String>,
    /// Require Cargo.lock to remain unchanged (always enforced).
    #[arg(long)]
    locked: bool,
    /// Disable Cargo network access.
    #[arg(long)]
    offline: bool,
    /// Require Cargo.lock to remain unchanged and disable Cargo network access.
    #[arg(long)]
    frozen: bool,
    /// Maturin platform compatibility tag.
    #[arg(long, alias = "manylinux", default_value = "off")]
    compatibility: String,
    /// Maturin's extension dependency audit mode.
    #[arg(long, value_parser = ["repair", "check", "warn", "skip"])]
    auditwheel: Option<String>,
    /// Strip the extension; release wheels default to true, editable builds to false.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    strip: Option<bool>,
}

#[derive(Deserialize)]
struct PythonProject {
    tool: PythonTools,
}

#[derive(Deserialize)]
struct PythonTools {
    maturin: MaturinSettings,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MaturinSettings {
    manifest_path: PathBuf,
    profile: Option<String>,
    editable_profile: Option<String>,
}

impl BuildPythonWheelArgs {
    pub(crate) fn run(self) -> Result<()> {
        let project = fs::canonicalize(&self.project_directory).context(FileSnafu {
            path: &self.project_directory,
        })?;
        let repository = cargo_workspace_root(&project)?;
        let config: PythonProject = read_toml(&project.join("pyproject.toml"))?;
        let manifest = project.join(&config.tool.maturin.manifest_path);
        let expected_manifest = repository.join("crates/delta-arrow-reader-python/Cargo.toml");
        ensure!(
            fs::canonicalize(&manifest).context(FileSnafu { path: &manifest })?
                == fs::canonicalize(&expected_manifest).context(FileSnafu {
                    path: &expected_manifest
                })?,
            InvalidInputSnafu {
                message: "wheel must use the selected source workspace"
            }
        );
        let profile = self.build_profile(&config.tool.maturin)?;
        let compiler = command_output(tool_command("rustc", &repository).arg("-vV"))?;
        let target = self
            .target
            .clone()
            .or_else(|| env::var("CARGO_BUILD_TARGET").ok())
            .unwrap_or(compiler_field(&compiler, "host")?.to_owned());
        let cargo_arguments = self.cargo_arguments(&project);

        // Serialize staging and assembly when frontends build the same source concurrently.
        let lock_directory = repository.join("target");
        fs::create_dir_all(&lock_directory).context(FileSnafu {
            path: &lock_directory,
        })?;
        let lock_path = lock_directory.join("python-wheel.lock");
        let lock = File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .context(FileSnafu { path: &lock_path })?;
        lock.lock().context(FileSnafu { path: &lock_path })?;

        let data = project.join(WHEEL_DATA);
        remove_generated_data(&data)?;
        let staged = tempfile::Builder::new()
            .prefix(".python-wheel-")
            .tempdir_in(&project)
            .context(FileSnafu { path: &project })?;
        if self.include_cli {
            stage_cli(staged.path(), &target, || {
                build_cli(&repository, &target, profile, &cargo_arguments)
            })?;
        }
        stage_notices(
            &repository,
            staged.path(),
            &target,
            self.offline || self.frozen,
            self.include_cli,
        )?;
        fs::rename(staged.path(), &data).context(FileSnafu { path: &data })?;
        if self.prepare_only {
            eprintln!("Prepared Python wheel data in {}", data.display());
            return Ok(());
        }

        let result = self.assemble(&project, &target, profile, &cargo_arguments);
        remove_generated_data(&data)?;
        let filename = result?;
        println!("{filename}");
        Ok(())
    }

    fn build_profile<'a>(&'a self, config: &'a MaturinSettings) -> Result<&'a str> {
        let profile = self
            .profile
            .as_deref()
            .or(if self.editable {
                config.editable_profile.as_deref()
            } else {
                None
            })
            .or(config.profile.as_deref())
            .unwrap_or("release");
        ensure!(
            matches!(profile, "dev" | "release"),
            InvalidInputSnafu {
                message: format!("unsupported Python wheel profile: {profile}")
            }
        );
        Ok(profile)
    }

    fn cargo_arguments(&self, project: &Path) -> Vec<OsString> {
        let mut args = Vec::new();
        if let Some(directory) = self
            .target_dir
            .clone()
            .or_else(|| env::var_os("CARGO_TARGET_DIR").map(PathBuf::from))
        {
            args.extend([
                "--target-dir".into(),
                project.join(directory).into_os_string(),
            ]);
        }
        if let Some(jobs) = &self.jobs {
            args.extend(["--jobs".into(), jobs.into()]);
        }
        if self.offline {
            args.push("--offline".into());
        }
        if self.frozen {
            args.push("--frozen".into());
        }
        args
    }

    fn assemble(
        &self,
        project: &Path,
        target: &str,
        profile: &str,
        cargo_arguments: &[OsString],
    ) -> Result<String> {
        let output_directory = self.wheel_directory.as_ref().context(InvalidInputSnafu {
            message: "wheel assembly requires an output directory",
        })?;
        fs::create_dir_all(output_directory).context(FileSnafu {
            path: output_directory,
        })?;
        let output_directory = fs::canonicalize(output_directory).context(FileSnafu {
            path: output_directory,
        })?;
        let mut command = Command::new("maturin");
        remove_cargo_package_environment(&mut command);
        command
            .current_dir(project)
            .args([
                "pep517",
                "build-wheel",
                "--locked",
                "--profile",
                profile,
                "--target",
                target,
                "--compatibility",
                &self.compatibility,
                "--compression-level",
                "9",
            ])
            .args(cargo_arguments);
        if let Some(interpreter) = self.interpreter.as_ref().or(self.python.as_ref()) {
            command.arg("--interpreter").arg(interpreter);
        }
        if let Some(auditwheel) = &self.auditwheel {
            command.args(["--auditwheel", auditwheel]);
        }
        let strip = self.strip.unwrap_or(profile == "release" && !self.editable);
        command.arg(format!("--strip={strip}"));
        if self.editable {
            command.arg("--editable");
        }
        if let Some(metadata) = &self.metadata_directory {
            command.env("MATURIN_PEP517_METADATA_DIR", metadata);
        }
        if let Some(deployment) = macos_deployment_target(target) {
            command.env("MACOSX_DEPLOYMENT_TARGET", deployment);
        }
        let output = command_output(&mut command)?;
        let wheel = Path::new(output.trim().lines().last().unwrap_or_default());
        ensure!(
            wheel.is_file() && wheel.extension().is_some_and(|ext| ext == "whl"),
            InvalidInputSnafu {
                message: "Maturin did not return a built wheel"
            }
        );
        let filename = wheel.file_name().context(InvalidInputSnafu {
            message: "Maturin did not return a built wheel",
        })?;
        let destination = output_directory.join(filename);
        if wheel != destination {
            copy_wheel(wheel, &destination)?;
        }
        Ok(filename.to_string_lossy().into_owned())
    }
}

fn copy_wheel(source: &Path, destination: &Path) -> Result<()> {
    // Copy before replacing the destination, which may alias the source.
    let parent = destination.parent().context(InvalidInputSnafu {
        message: "wheel destination must have a parent directory",
    })?;
    let temporary =
        tempfile::NamedTempFile::new_in(parent).context(FileSnafu { path: destination })?;
    fs::copy(source, temporary.path()).context(FileSnafu { path: source })?;
    temporary
        .persist(destination)
        .map_err(|error| error.error)
        .context(FileSnafu { path: destination })?;
    Ok(())
}

fn tool_command(program: &str, repository: &Path) -> Command {
    let mut command =
        Command::new(env::var_os(program.to_ascii_uppercase()).unwrap_or_else(|| program.into()));
    command.current_dir(repository);
    command
}

fn stage_notices(
    repository: &Path,
    data: &Path,
    target: &str,
    offline: bool,
    include_cli: bool,
) -> Result<()> {
    let destination = data.join("data/share/delta-arrow-reader");
    fs::create_dir_all(&destination).context(FileSnafu { path: &destination })?;
    for package in std::iter::once("python").chain(include_cli.then_some("cli")) {
        let text = dependency_notices(
            repository,
            &repository.join(format!("crates/delta-arrow-reader-{package}/Cargo.toml")),
            target,
            |program| {
                let mut command = tool_command(program, repository);
                // Keep license downloads enabled while Cargo resolves dependencies offline.
                if offline {
                    command.env("CARGO_NET_OFFLINE", "true");
                }
                command
            },
        )?;
        let path = destination.join(format!("{package}-THIRD_PARTY_NOTICES.txt"));
        fs::write(&path, text).context(FileSnafu { path: &path })?;
    }
    Ok(())
}

fn remove_generated_data(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_dir(),
                InvalidInputSnafu {
                    message: format!(
                        "generated wheel data must be a real directory: {}",
                        path.display()
                    )
                }
            );
            fs::remove_dir_all(path).context(FileSnafu { path })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context(FileSnafu { path }),
    }
}

#[cfg(test)]
mod tests;
