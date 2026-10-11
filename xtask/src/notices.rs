use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
};

use clap::{Args, ValueEnum};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use snafu::{ResultExt, ensure};

use crate::error::{
    CommandFailedSnafu, CommandStartSnafu, FileSnafu, InvalidInputSnafu, JsonSnafu, Result,
    TomlSnafu, Utf8Snafu,
};

#[derive(Args)]
pub(crate) struct NoticeArgs {
    /// Linux architecture of the CLI being packaged.
    #[arg(long, value_enum)]
    architecture: Architecture,
    /// Rust toolchain used to build the CLI; defaults to Cargo.toml's rust-version.
    #[arg(long)]
    rust_toolchain: Option<String>,
    /// Destination for the complete notices; must not already exist.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Clone, Copy, ValueEnum)]
enum Architecture {
    Amd64,
    Arm64,
}

impl Architecture {
    fn rust_target(self) -> &'static str {
        match self {
            Self::Amd64 => "x86_64-unknown-linux-gnu",
            Self::Arm64 => "aarch64-unknown-linux-gnu",
        }
    }
}

#[derive(Deserialize)]
struct WorkspaceManifest {
    package: RootPackage,
    workspace: Workspace,
}

#[derive(Deserialize)]
struct RootPackage {
    #[serde(rename = "rust-version")]
    rust_version: String,
}

#[derive(Deserialize)]
struct Workspace {
    metadata: WorkspaceMetadata,
}

#[derive(Deserialize)]
struct WorkspaceMetadata {
    #[serde(rename = "cli-release")]
    cli_release: ReleaseTools,
}

#[derive(Deserialize)]
struct ReleaseTools {
    #[serde(rename = "cargo-about-version")]
    cargo_about_version: String,
}

#[derive(Clone, Deserialize)]
struct LicenseReport {
    crates: Vec<ResolvedCrate>,
    licenses: Vec<License>,
}

#[derive(Clone, Deserialize)]
struct ResolvedCrate {
    package: DependencyPackage,
}

#[derive(Clone, Deserialize)]
struct DependencyPackage {
    id: String,
    name: String,
    version: String,
    manifest_path: PathBuf,
    license_file: Option<PathBuf>,
    source: Option<String>,
}

#[derive(Clone, Deserialize)]
struct License {
    id: String,
    text: String,
    used_by: Vec<LicenseUse>,
}

#[derive(Clone, Deserialize)]
struct LicenseUse {
    #[serde(rename = "crate")]
    package: DependencyIdentity,
}

#[derive(Clone, Deserialize)]
struct DependencyIdentity {
    id: String,
    name: String,
    version: String,
}

#[derive(Deserialize)]
struct ExtraNoticesConfig {
    files: Vec<ExtraNotice>,
}

#[derive(Clone, Deserialize)]
struct ExtraNotice {
    package: String,
    version: String,
    url: String,
    sha256: String,
}

#[derive(Deserialize)]
struct CrateSource {
    git: GitRevision,
}

#[derive(Deserialize)]
struct GitRevision {
    sha1: String,
}

struct NoticeText {
    name: String,
    text: String,
}

type ExtraNotices = BTreeMap<String, Vec<NoticeText>>;

pub(crate) fn run(args: NoticeArgs) -> Result<()> {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let manifest: WorkspaceManifest = read_toml(&repository.join("Cargo.toml"))?;
    let toolchain = args
        .rust_toolchain
        .as_deref()
        .unwrap_or(&manifest.package.rust_version);
    let about_version = &manifest.workspace.metadata.cli_release.cargo_about_version;
    let release = repository.join("crates/delta-arrow-reader-cli/release");
    let mut command = rustup_command(toolchain, args.architecture, &repository);
    let version = command_output(command.args(["cargo", "about", "--version"]))?;
    ensure!(
        version.trim() == format!("cargo-about {about_version}"),
        InvalidInputSnafu {
            message: format!("expected cargo-about {about_version}, got {version:?}")
        }
    );
    let report = command_output(
        rustup_command(toolchain, args.architecture, &repository)
            .args([
                "cargo",
                "about",
                "generate",
                "--locked",
                "--fail",
                "--manifest-path",
            ])
            .arg(repository.join("crates/delta-arrow-reader-cli/Cargo.toml"))
            .arg("--config")
            .arg(release.join("about.toml"))
            .args([
                "--target",
                args.architecture.rust_target(),
                "--format",
                "json",
            ]),
    )?;
    let report: LicenseReport = serde_json::from_str(&report).context(JsonSnafu {
        subject: "cargo-about report",
    })?;
    let config: ExtraNoticesConfig = read_toml(&release.join("extra-notices.toml"))?;
    let extra = collect_extra_notices(&report.crates, &config.files, download_notice)?;
    let sysroot = command_output(
        rustup_command(toolchain, args.architecture, &repository)
            .args(["rustc", "--print", "sysroot"]),
    )?;
    let text = render_notices(
        &report,
        &Path::new(sysroot.trim()).join("share/doc/rust"),
        &extra,
        about_version,
    )?;
    write_notices(&args.output, &text)?;
    println!("Dependency notices written to {}", args.output.display());
    Ok(())
}

fn read_toml<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let text = fs::read_to_string(path).context(FileSnafu { path })?;
    toml::from_str(&text).context(TomlSnafu { path })
}

fn rustup_command(toolchain: &str, architecture: Architecture, repository: &Path) -> Command {
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

fn command_output(command: &mut Command) -> Result<String> {
    let program = command.get_program().to_string_lossy().into_owned();
    let output = command
        .stderr(Stdio::inherit())
        .output()
        .context(CommandStartSnafu { program: &program })?;
    ensure!(
        output.status.success(),
        CommandFailedSnafu {
            program: &program,
            status: output.status
        }
    );
    String::from_utf8(output.stdout).context(Utf8Snafu { subject: program })
}

fn read_notice(path: &Path) -> Result<String> {
    ensure!(
        fs::symlink_metadata(path)
            .context(FileSnafu { path })?
            .is_file(),
        InvalidInputSnafu {
            message: format!("expected a regular file: {}", path.display())
        }
    );
    let text = fs::read_to_string(path).context(FileSnafu { path })?;
    ensure!(
        !text.trim().is_empty(),
        InvalidInputSnafu {
            message: format!("empty license or notice: {}", path.display())
        }
    );
    Ok(text)
}

fn dependency_license_files(package: &DependencyPackage) -> Result<Vec<NoticeText>> {
    let root = package.manifest_path.parent().ok_or_else(|| {
        InvalidInputSnafu {
            message: format!(
                "invalid dependency manifest path: {}",
                package.manifest_path.display()
            ),
        }
        .build()
    })?;
    let mut paths = BTreeSet::new();
    let mut directories = vec![PathBuf::new()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(root.join(&directory)).context(FileSnafu {
            path: root.join(&directory),
        })? {
            let entry = entry.context(FileSnafu {
                path: root.join(&directory),
            })?;
            let relative = directory.join(entry.file_name());
            let kind = entry
                .file_type()
                .context(FileSnafu { path: entry.path() })?;
            ensure!(
                !kind.is_symlink(),
                InvalidInputSnafu {
                    message: format!("symlink in dependency source: {}", entry.path().display()),
                }
            );
            if kind.is_dir() {
                directories.push(relative);
            } else if is_license_filename(&entry.file_name())
                || directory.components().any(|part| {
                    let name = part.as_os_str().to_string_lossy().to_ascii_lowercase();
                    name == "licenses" || name == "licences"
                })
            {
                paths.insert(relative);
            }
        }
    }
    if let Some(path) = &package.license_file {
        ensure!(
            path.components()
                .all(|part| matches!(part, Component::Normal(_) | Component::CurDir)),
            InvalidInputSnafu {
                message: format!("license_file escapes dependency source: {}", path.display())
            }
        );
        paths.insert(path.clone());
    }
    paths
        .into_iter()
        .map(|path| {
            Ok(NoticeText {
                name: path.to_string_lossy().into_owned(),
                text: read_notice(&root.join(&path))?,
            })
        })
        .collect()
}

fn is_license_filename(name: &OsStr) -> bool {
    let name = name.to_string_lossy().to_ascii_lowercase();
    matches!(
        name.split(['-', '_', '.']).next(),
        Some(
            "license"
                | "licenses"
                | "licence"
                | "licences"
                | "copying"
                | "copyright"
                | "copyrights"
                | "notice"
                | "notices"
        )
    )
}

fn collect_extra_notices(
    packages: &[ResolvedCrate],
    files: &[ExtraNotice],
    mut download: impl FnMut(&str) -> Result<String>,
) -> Result<ExtraNotices> {
    let mut notices = ExtraNotices::new();
    for entry in files {
        let matching: Vec<_> = packages
            .iter()
            .map(|item| &item.package)
            .filter(|package| package.name == entry.package)
            .collect();
        if matching.is_empty() {
            continue;
        }
        let parts: Vec<_> = entry
            .url
            .strip_prefix("https://raw.githubusercontent.com/")
            .unwrap_or_default()
            .splitn(4, '/')
            .collect();
        ensure!(
            parts.len() == 4
                && parts.iter().all(|part| !part.is_empty())
                && parts[2].len() == 40
                && parts[2]
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                && !entry.url.contains(['?', '#']),
            InvalidInputSnafu {
                message: format!("notice URL must name a full source commit: {}", entry.url)
            }
        );
        for package in &matching {
            let source_path = package.manifest_path.with_file_name(".cargo_vcs_info.json");
            let source: CrateSource =
                serde_json::from_str(&read_notice(&source_path)?).context(JsonSnafu {
                    subject: "crate source revision",
                })?;
            ensure!(
                package.version == entry.version && source.git.sha1 == parts[2],
                InvalidInputSnafu {
                    message: format!(
                        "update extra-notices.toml for {} {}",
                        package.name, package.version
                    )
                }
            );
        }
        let text = download(&entry.url)?;
        ensure!(
            format!("{:x}", Sha256::digest(text.as_bytes())) == entry.sha256,
            InvalidInputSnafu {
                message: format!("downloaded notice SHA-256 differs: {}", entry.url)
            }
        );
        ensure!(
            !text.trim().is_empty(),
            InvalidInputSnafu {
                message: format!("empty downloaded notice: {}", entry.url)
            }
        );
        for package in matching {
            notices
                .entry(package.id.clone())
                .or_default()
                .push(NoticeText {
                    name: entry.url.clone(),
                    text: text.clone(),
                });
        }
    }
    Ok(notices)
}

fn download_notice(url: &str) -> Result<String> {
    command_output(Command::new("curl").args([
        "--disable",
        "--fail",
        "--silent",
        "--show-error",
        "--location",
        "--proto",
        "=https",
        "--proto-redir",
        "=https",
        "--max-time",
        "30",
        url,
    ]))
}

fn render_notices(
    report: &LicenseReport,
    rust_docs: &Path,
    extra: &ExtraNotices,
    about_version: &str,
) -> Result<String> {
    let packages: BTreeMap<_, _> = report
        .crates
        .iter()
        .map(|item| (&item.package.id, &item.package))
        .collect();
    ensure!(
        !packages.is_empty() && !report.licenses.is_empty(),
        InvalidInputSnafu {
            message: "cargo-about returned no crates or licenses"
        }
    );
    ensure!(
        packages.len() == report.crates.len(),
        InvalidInputSnafu {
            message: "cargo-about returned duplicate crate IDs"
        }
    );
    let mut output =
        format!("Dependency license texts generated with cargo-about {about_version}\n");
    let mut covered = BTreeSet::new();
    for license in &report.licenses {
        ensure!(
            !license.text.trim().is_empty() && !license.used_by.is_empty(),
            InvalidInputSnafu {
                message: "cargo-about returned a license without text or crates"
            }
        );
        let mut names = Vec::new();
        for usage in &license.used_by {
            covered.insert(&usage.package.id);
            names.push(format!("{} {}", usage.package.name, usage.package.version));
        }
        output.push_str(&format!(
            "\n{}\nUsed by: {}\n\n{}\n",
            license.id,
            names.join(", "),
            license.text
        ));
    }
    ensure!(
        covered == packages.keys().copied().collect(),
        InvalidInputSnafu {
            message: "cargo-about did not cover every resolved crate"
        }
    );
    let mut packages: Vec<_> = packages.values().copied().collect();
    packages.sort_by(|a, b| (&a.name, &a.version).cmp(&(&b.name, &b.version)));
    // Keep original attributions and combined licenses that the scanner may omit.
    for package in packages {
        if package.source.is_none() {
            ensure!(
                matches!(
                    package.name.as_str(),
                    "delta-arrow-reader" | "delta-arrow-reader-cli"
                ),
                InvalidInputSnafu {
                    message: format!("unreviewed local dependency: {}", package.name)
                }
            );
            continue;
        }
        let files = dependency_license_files(package)?;
        let files: Vec<_> = files
            .iter()
            .chain(extra.get(&package.id).into_iter().flatten())
            .collect();
        ensure!(
            !files.is_empty(),
            InvalidInputSnafu {
                message: format!("no original license files for {}", package.name)
            }
        );
        for file in files {
            output.push_str(&format!(
                "\n{} {}: {}\n\n{}\n",
                package.name, package.version, file.name, file.text
            ));
        }
    }
    // The linked standard library is absent from Cargo's dependency graph.
    output.push_str("\nRust standard library: COPYRIGHT-library.html\n\n");
    output.push_str(&read_notice(&rust_docs.join("COPYRIGHT-library.html"))?);
    let directory = rust_docs.join("licenses");
    let mut files = BTreeSet::new();
    for entry in fs::read_dir(&directory).context(FileSnafu { path: &directory })? {
        files.insert(entry.context(FileSnafu { path: &directory })?.file_name());
    }
    ensure!(
        ["Apache-2.0.txt", "MIT.txt"]
            .iter()
            .all(|name| files.contains(OsStr::new(name))),
        InvalidInputSnafu {
            message: "Rust toolchain license files are missing"
        }
    );
    for name in files {
        output.push_str(&format!(
            "\nRust toolchain: licenses/{}\n\n{}\n",
            name.to_string_lossy(),
            read_notice(&directory.join(&name))?
        ));
    }
    Ok(output)
}

fn write_notices(path: &Path, text: &str) -> Result<()> {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .context(FileSnafu { path })?;
    output
        .write_all(text.as_bytes())
        .context(FileSnafu { path })
}

#[cfg(test)]
mod tests;
