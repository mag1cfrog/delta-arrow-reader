use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use clap::Args;
use regex::Regex;
use serde::Serialize;
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::{check_runtime_dependencies, file_sha256, require_regular_file},
    command::output_with_timeout,
    error::{
        CommandFailedSnafu, FileSnafu, InvalidInputSnafu, JsonSnafu, PatternSnafu, Result,
        Utf8Snafu,
    },
    platform::Architecture,
};

pub(crate) const MIN_GLIBC: &str = "2.36";
pub(crate) const RUNTIME_PACKAGES: [&str; 3] = ["ca-certificates", "libc6", "libgcc-s1"];
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Args)]
pub(crate) struct CheckBinaryArgs {
    binary: PathBuf,
    #[arg(long)]
    architecture: Architecture,
    #[arg(long)]
    expected_version: String,
    #[arg(long)]
    expected_sha256: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct BinaryMetadata {
    architecture: Architecture,
    rust_target: &'static str,
    #[serde(flatten)]
    pub(crate) elf: ElfMetadata,
    pub(crate) dar_sha256: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ElfMetadata {
    interpreter: &'static str,
    pub(crate) runtime_dependencies: Vec<String>,
    max_glibc: String,
}

impl CheckBinaryArgs {
    pub(crate) fn run(self) -> Result<()> {
        let metadata = check_binary(
            &self.binary,
            self.architecture,
            &self.expected_version,
            &self.expected_sha256,
        )?;
        println!(
            "{}",
            serde_json::to_string_pretty(&metadata).context(JsonSnafu {
                subject: "binary metadata"
            })?
        );
        Ok(())
    }
}

impl Architecture {
    fn elf_machine(self) -> u16 {
        match self {
            Self::Amd64 => 62,
            Self::Arm64 => 183,
        }
    }

    fn uname_machine(self) -> &'static str {
        match self {
            Self::Amd64 => "x86_64",
            Self::Arm64 => "aarch64",
        }
    }

    fn interpreter(self) -> &'static str {
        match self {
            Self::Amd64 => "/lib64/ld-linux-x86-64.so.2",
            Self::Arm64 => "/lib/ld-linux-aarch64.so.1",
        }
    }
}

fn inspection_command_output(command: &mut Command) -> Result<String> {
    let output = output_with_timeout(command, COMMAND_TIMEOUT)?;
    let program = command.get_program().to_string_lossy();
    ensure!(
        output.status.success(),
        CommandFailedSnafu {
            program: program.as_ref(),
            status: output.status
        }
    );
    ensure!(
        output.stderr.is_empty(),
        InvalidInputSnafu {
            message: format!(
                "{program} reported a problem: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
        }
    );
    String::from_utf8(output.stdout).context(Utf8Snafu { subject: program })
}

pub(crate) fn check_environment(architecture: Architecture) -> Result<()> {
    let path = Path::new("/etc/os-release");
    let release = fs::read_to_string(path).context(FileSnafu { path })?;
    let glibc = inspection_command_output(
        Command::new("getconf")
            .arg("GNU_LIBC_VERSION")
            .env("LC_ALL", "C"),
    )?;
    let machine = inspection_command_output(Command::new("uname").arg("-m").env("LC_ALL", "C"))?;
    check_runtime_environment(&release, glibc.trim(), machine.trim(), architecture)
}

fn check_runtime_environment(
    release: &str,
    glibc: &str,
    machine: &str,
    architecture: Architecture,
) -> Result<()> {
    let release: BTreeMap<_, _> = release
        .lines()
        .filter_map(|line| {
            line.split_once('=')
                .map(|(key, value)| (key, value.trim().trim_matches(['\'', '"'])))
        })
        .collect();
    ensure!(
        release.get("ID") == Some(&"debian") && release.get("VERSION_ID") == Some(&"12"),
        InvalidInputSnafu {
            message: "build and runtime checks require Debian 12"
        }
    );
    ensure!(
        glibc == format!("glibc {MIN_GLIBC}"),
        InvalidInputSnafu {
            message: format!("expected the Debian 12 glibc {MIN_GLIBC} runtime")
        }
    );
    ensure!(
        machine == architecture.uname_machine(),
        InvalidInputSnafu {
            message: format!(
                "{} must execute on its matching architecture",
                architecture.name()
            )
        }
    );
    Ok(())
}

fn check_elf_header(header: &[u8], architecture: Architecture) -> Result<()> {
    ensure!(
        header.len() == 20 && header.starts_with(b"\x7fELF\x02\x01\x01"),
        InvalidInputSnafu {
            message: "expected a 64-bit little-endian ELF binary"
        }
    );
    let file_type = u16::from_le_bytes([header[16], header[17]]);
    let machine = u16::from_le_bytes([header[18], header[19]]);
    ensure!(
        [2, 3].contains(&file_type) && machine == architecture.elf_machine(),
        InvalidInputSnafu {
            message: format!("expected an {} ELF executable", architecture.name())
        }
    );
    Ok(())
}

fn captures<'a>(pattern: &str, text: &'a str) -> Result<Vec<&'a str>> {
    Ok(Regex::new(pattern)
        .context(PatternSnafu)?
        .captures_iter(text)
        .filter_map(|capture| capture.get(1).map(|value| value.as_str()))
        .collect())
}

fn parse_glibc_version(version: &str) -> Result<Vec<u64>> {
    let parts: Vec<_> = version.split('.').collect();
    ensure!(
        (2..=3).contains(&parts.len())
            && parts
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())),
        InvalidInputSnafu {
            message: "missing or unsupported GLIBC symbol requirements"
        }
    );
    parts
        .into_iter()
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            InvalidInputSnafu {
                message: format!("invalid GLIBC version {version}: {error}"),
            }
            .build()
        })
}

fn check_readelf_output(output: &str, architecture: Architecture) -> Result<ElfMetadata> {
    let interpreter = architecture.interpreter();
    ensure!(
        captures(r"\[Requesting program interpreter: (.+)\]", output)? == [interpreter],
        InvalidInputSnafu {
            message: format!("expected ELF interpreter {interpreter}")
        }
    );
    ensure!(
        !output.contains("(RPATH)") && !output.contains("(RUNPATH)"),
        InvalidInputSnafu {
            message: "release binaries must not depend on an RPATH or RUNPATH"
        }
    );
    let mut dependencies: Vec<String> =
        captures(r"\(NEEDED\)\s+Shared library: \[([^\]]+)\]", output)?
            .into_iter()
            .map(String::from)
            .collect();
    dependencies.sort();
    check_runtime_dependencies(&dependencies)?;
    let loader = interpreter.rsplit('/').next().unwrap_or(interpreter);
    let supported = ["libc.so.6", "libm.so.6", "libgcc_s.so.1", loader];
    ensure!(
        dependencies.iter().any(|name| name == "libc.so.6")
            && dependencies
                .iter()
                .all(|name| supported.contains(&name.as_str())),
        InvalidInputSnafu {
            message: format!("libraries outside the supported runtime: {dependencies:?}")
        }
    );
    let versions = captures(r"Name: GLIBC_(\S+)", output)?
        .into_iter()
        .map(parse_glibc_version)
        .collect::<Result<Vec<_>>>()?;
    let maximum = versions.into_iter().max().ok_or_else(|| {
        InvalidInputSnafu {
            message: "missing or unsupported GLIBC symbol requirements",
        }
        .build()
    })?;
    let max_glibc = maximum
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(".");
    let mut supported_glibc = parse_glibc_version(MIN_GLIBC)?;
    supported_glibc.resize(3, 0);
    ensure!(
        maximum <= supported_glibc,
        InvalidInputSnafu {
            message: format!("binary requires GLIBC_{max_glibc}, above {MIN_GLIBC}")
        }
    );
    ensure!(
        captures(r"x86 ISA needed: ([^\n]+)", output)?
            .iter()
            .all(|value| value.trim() == "x86-64-baseline"),
        InvalidInputSnafu {
            message: "binary requires an x86 instruction set above the baseline"
        }
    );
    Ok(ElfMetadata {
        interpreter,
        runtime_dependencies: dependencies,
        max_glibc,
    })
}

fn inspect_elf(binary: &Path, architecture: Architecture) -> Result<BinaryMetadata> {
    ensure!(
        cfg!(unix),
        InvalidInputSnafu {
            message: "binary checks require Unix file permissions"
        }
    );
    require_regular_file(binary)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(binary)
            .context(FileSnafu { path: binary })?
            .permissions()
            .mode();
        ensure!(
            mode & 0o111 != 0 && mode & 0o7000 == 0,
            InvalidInputSnafu {
                message: "binary must be executable without special permission bits"
            }
        );
    }
    let mut header = [0; 20];
    File::open(binary)
        .context(FileSnafu { path: binary })?
        .read_exact(&mut header)
        .context(FileSnafu { path: binary })?;
    check_elf_header(&header, architecture)?;
    let output = inspection_command_output(
        Command::new("readelf")
            .args([
                "--program-headers",
                "--dynamic",
                "--version-info",
                "--notes",
                "--wide",
            ])
            .arg(binary)
            .env("LC_ALL", "C"),
    )?;
    Ok(BinaryMetadata {
        architecture,
        rust_target: architecture.rust_target(),
        elf: check_readelf_output(&output, architecture)?,
        dar_sha256: file_sha256(binary)?,
    })
}

pub(crate) fn check_binary(
    binary: &Path,
    architecture: Architecture,
    version: &str,
    expected_sha256: &str,
) -> Result<BinaryMetadata> {
    check_environment(architecture)?;
    inspect_and_execute_binary(binary, architecture, version, expected_sha256)
}

fn inspect_and_execute_binary(
    binary: &Path,
    architecture: Architecture,
    version: &str,
    expected_sha256: &str,
) -> Result<BinaryMetadata> {
    let binary = std::path::absolute(binary).context(FileSnafu { path: binary })?;
    let metadata = inspect_elf(&binary, architecture)?;
    ensure!(
        metadata.dar_sha256 == expected_sha256,
        InvalidInputSnafu {
            message: "binary SHA-256 differs from the expected validated bytes"
        }
    );
    let output = output_with_timeout(Command::new(&binary).arg("--version"), COMMAND_TIMEOUT)?;
    ensure!(
        output.status.success(),
        CommandFailedSnafu {
            program: binary.to_string_lossy(),
            status: output.status
        }
    );
    ensure!(
        output.stdout == format!("dar {version}\n").as_bytes() && output.stderr.is_empty(),
        InvalidInputSnafu {
            message: format!("binary does not report exactly dar {version}")
        }
    );
    ensure!(
        file_sha256(&binary)? == expected_sha256,
        InvalidInputSnafu {
            message: "binary changed during execution"
        }
    );
    Ok(metadata)
}

#[cfg(test)]
mod tests;
