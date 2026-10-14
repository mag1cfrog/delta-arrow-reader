use std::{fs::File, io::Read, path::PathBuf, process::Command};

use clap::Args;
use serde_json::{Value, json};
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::require_regular_file,
    binary::{ElfBaseline, inspect_elf, inspection_command_output},
    error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
    platform::Architecture,
};

#[derive(Args)]
pub(crate) struct AuditPythonCliArgs {
    /// Installed executable from the final, repaired wheel.
    binary: PathBuf,
    #[arg(long, value_parser = ["manylinux_2_28_x86_64", "macosx_12_0_arm64", "win_amd64"])]
    platform_tag: String,
}

impl AuditPythonCliArgs {
    pub(crate) fn run(self) -> Result<()> {
        require_regular_file(&self.binary)?;
        let binary = std::path::absolute(&self.binary).context(FileSnafu { path: &self.binary })?;
        let report = match self.platform_tag.as_str() {
            "manylinux_2_28_x86_64" => {
                let metadata = inspect_elf(&binary, Architecture::Amd64, ElfBaseline::PythonWheel)?;
                serde_json::to_value(metadata.elf).context(JsonSnafu {
                    subject: "ELF metadata",
                })?
            }
            "macosx_12_0_arm64" => {
                let mut header = [0; 16];
                File::open(&binary)
                    .context(FileSnafu { path: &binary })?
                    .read_exact(&mut header)
                    .context(FileSnafu { path: &binary })?;
                check_macos_header(&header)?;
                let output = inspection_command_output(
                    Command::new("otool")
                        .arg("-l")
                        .arg(&binary)
                        .env("LC_ALL", "C"),
                )?;
                check_macos_load_commands(&output)?
            }
            "win_amd64" => {
                // --coff-imports includes both regular and delay-loaded DLLs.
                let output = inspection_command_output(
                    Command::new("llvm-readobj")
                        .args(["--file-headers", "--coff-imports"])
                        .arg(&binary)
                        .env("LC_ALL", "C"),
                )?;
                check_windows_imports(&output)?
            }
            _ => {
                return InvalidInputSnafu {
                    message: "unsupported wheel platform",
                }
                .fail();
            }
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context(JsonSnafu {
                subject: "wheel CLI native dependencies",
            })?
        );
        Ok(())
    }
}

fn check_macos_header(header: &[u8]) -> Result<()> {
    // Thin, little-endian Mach-O 64, CPU_TYPE_ARM64, CPU_SUBTYPE_ARM64_ALL, MH_EXECUTE.
    ensure!(
        header == b"\xcf\xfa\xed\xfe\x0c\x00\x00\x01\x00\x00\x00\x00\x02\x00\x00\x00",
        InvalidInputSnafu {
            message: "wheel CLI must be a native arm64 Mach-O executable"
        }
    );
    Ok(())
}

fn single_field<'a>(output: &'a str, prefix: &str) -> Result<&'a str> {
    let values: Vec<_> = output
        .lines()
        .filter_map(|line| line.trim().strip_prefix(prefix))
        .collect();
    ensure!(
        values.len() == 1 && !values[0].is_empty(),
        InvalidInputSnafu {
            message: format!("native inspection requires exactly one {prefix}field"),
        }
    );
    Ok(values[0])
}

fn check_macos_load_commands(output: &str) -> Result<Value> {
    let mut minimum = None;
    let mut libraries = Vec::new();
    let mut loaders = Vec::new();
    for command in output.split("\nLoad command ").skip(1) {
        match single_field(command, "cmd ")? {
            "LC_BUILD_VERSION" => {
                let platform = single_field(command, "platform ")?;
                ensure!(
                    minimum.is_none()
                        && (platform == "1" || platform.eq_ignore_ascii_case("macos")),
                    InvalidInputSnafu {
                        message: "wheel CLI must declare one macOS deployment target"
                    }
                );
                let version = single_field(command, "minos ")?;
                let components: Vec<_> = version.split('.').collect();
                ensure!(
                    (1..=3).contains(&components.len()),
                    InvalidInputSnafu {
                        message: "invalid macOS deployment target",
                    }
                );
                let mut numeric = [0u32; 3];
                for (index, component) in components.iter().enumerate() {
                    numeric[index] = component
                        .parse()
                        .ok()
                        .filter(|_| component.bytes().all(|byte| byte.is_ascii_digit()))
                        .ok_or_else(|| {
                            InvalidInputSnafu {
                                message: "invalid macOS deployment target",
                            }
                            .build()
                        })?;
                }
                ensure!(
                    ([11, 0, 0]..=[12, 0, 0]).contains(&numeric),
                    InvalidInputSnafu {
                        message: format!(
                            "wheel CLI deployment target {version} is outside supported arm64 macOS 11.0..=12.0"
                        ),
                    }
                );
                minimum = Some(version);
            }
            "LC_RPATH" => {
                return InvalidInputSnafu {
                    message: "wheel CLI must not depend on a macOS rpath",
                }
                .fail();
            }
            "LC_LOAD_DYLINKER"
            | "LC_LOAD_DYLIB"
            | "LC_LOAD_WEAK_DYLIB"
            | "LC_REEXPORT_DYLIB"
            | "LC_LOAD_UPWARD_DYLIB"
            | "LC_LAZY_LOAD_DYLIB" => {
                let name = single_field(command, "name ")?;
                let (path, _) = name.rsplit_once(" (offset ").ok_or_else(|| {
                    InvalidInputSnafu {
                        message: "missing macOS load command path",
                    }
                    .build()
                })?;
                if single_field(command, "cmd ")? == "LC_LOAD_DYLINKER" {
                    loaders.push(path);
                } else {
                    ensure!(
                        (path.starts_with("/usr/lib/")
                            || path.starts_with("/System/Library/Frameworks/"))
                            && path
                                .split('/')
                                .skip(1)
                                .all(|part| !matches!(part, "" | "." | "..")),
                        InvalidInputSnafu {
                            message: format!(
                                "wheel CLI requires a non-system macOS library: {path}"
                            )
                        }
                    );
                    libraries.push(path);
                }
            }
            _ => {}
        }
    }
    ensure!(
        minimum.is_some() && !libraries.is_empty() && loaders == ["/usr/lib/dyld"],
        InvalidInputSnafu {
            message: "wheel CLI is missing its macOS deployment target, system libraries, or system loader",
        }
    );
    libraries.sort_unstable();
    Ok(
        json!({"architecture": "arm64", "minimum_macos": minimum, "runtime_dependencies": libraries}),
    )
}

fn check_windows_imports(output: &str) -> Result<Value> {
    ensure!(
        single_field(output, "Format: ")? == "COFF-x86-64"
            && single_field(output, "Arch: ")? == "x86_64"
            && single_field(output, "Machine: ")? == "IMAGE_FILE_MACHINE_AMD64 (0x8664)"
            && output
                .lines()
                .any(|line| line.trim() == "IMAGE_FILE_EXECUTABLE_IMAGE (0x2)")
            && !output.contains("IMAGE_FILE_DLL"),
        InvalidInputSnafu {
            message: "wheel CLI must be a native x86_64 Windows executable"
        }
    );
    let mut libraries: Vec<_> = output
        .lines()
        .filter_map(|line| line.strip_prefix("  Name: "))
        .map(str::to_ascii_lowercase)
        .collect();
    let imports = output
        .lines()
        .filter(|line| matches!(*line, "Import {" | "DelayImport {"))
        .count();
    ensure!(
        imports > 0 && imports == libraries.len(),
        InvalidInputSnafu {
            message: "missing or incomplete Windows import table",
        }
    );
    // Windows system DLLs and the existing CPython/MSVC runtime prerequisites.
    const SYSTEM_DLLS: &[&str] = &[
        "advapi32.dll",
        "bcrypt.dll",
        "bcryptprimitives.dll",
        "crypt32.dll",
        "dbghelp.dll",
        "iphlpapi.dll",
        "kernel32.dll",
        "msvcrt.dll",
        "ncrypt.dll",
        "ntdll.dll",
        "ole32.dll",
        "secur32.dll",
        "shell32.dll",
        "ucrtbase.dll",
        "user32.dll",
        "userenv.dll",
        "vcruntime140.dll",
        "vcruntime140_1.dll",
        "ws2_32.dll",
    ];
    for library in &libraries {
        let api_set = (library.starts_with("api-ms-win-crt-")
            || library.starts_with("api-ms-win-core-"))
            && library.ends_with(".dll")
            && library
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._".contains(&byte));
        ensure!(
            SYSTEM_DLLS.contains(&library.as_str()) || api_set,
            InvalidInputSnafu {
                message: format!(
                    "wheel CLI requires a DLL outside the supported Windows runtime: {library}"
                ),
            }
        );
    }
    libraries.sort_unstable();
    libraries.dedup();
    Ok(json!({"architecture": "x86_64", "runtime_dependencies": libraries}))
}

#[cfg(test)]
mod tests;
