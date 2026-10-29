//! Produce independent Python and CLI installations from the release build.
//!
//! The input is the repaired wheel containing both native artifacts. The outputs
//! share no installed files: the library keeps PyArrow and the CLI has no Python
//! dependencies. Unstripped binaries stay in a separate directory for debugging.

use std::{
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    process::Command,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::Args;
use flate2::{Compression, read::DeflateDecoder, write::DeflateEncoder};
use rawzip::{CompressionMethod, ZipArchive, ZipArchiveWriter, path::EntryPath};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    Result,
    artifacts::file_sha256,
    command::command_output,
    python_wheels::{WheelContents, check_wheel, check_wheel_contents},
};

#[derive(Args)]
pub(crate) struct SplitPythonWheelArgs {
    /// Repaired release build containing the Python extension and native CLI.
    wheel: PathBuf,
    #[arg(long)]
    expected_version: String,
    #[arg(long, value_parser = ["manylinux_2_28_x86_64", "macosx_12_0_arm64", "win_amd64", "linux_x86_64"])]
    platform_tag: String,
    /// New directory for the two distributions.
    #[arg(long)]
    destination: PathBuf,
    /// New directory for matching unstripped binaries and their identities.
    #[arg(long)]
    symbols_directory: PathBuf,
}

impl SplitPythonWheelArgs {
    pub(crate) fn run(self) -> Result<()> {
        if self.destination.exists()
            || self.symbols_directory.exists()
            || self.destination == self.symbols_directory
        {
            return Err("wheel and symbol destinations must be distinct new directories".into());
        }
        let version = &self.expected_version;
        let platform = &self.platform_tag;
        check_wheel_contents(&self.wheel, version, platform, WheelContents::CombinedBuild)?;
        let parent = self
            .destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;
        let temporary = tempfile::Builder::new()
            .prefix(".split-wheels-")
            .tempdir_in(parent)?;
        let python = temporary.path().join("python");
        let cli = temporary.path().join("cli");
        let symbols = temporary.path().join("symbols");
        let output = temporary.path().join("wheels");
        fs::create_dir(&symbols)?;
        fs::create_dir(&output)?;
        unpack_checked_wheel(&self.wheel, &python)?;

        let python_stem = format!("delta_arrow_reader_py-{version}");
        let cli_stem = format!("delta_arrow_reader-{version}");
        let executable = if platform == "win_amd64" {
            "dar.exe"
        } else {
            "dar"
        };
        let cli_binary = cli.join(format!("{cli_stem}.data/scripts/{executable}"));
        for relative in [
            format!("scripts/{executable}"),
            "data/share/delta-arrow-reader/cli-THIRD_PARTY_NOTICES.txt".into(),
        ] {
            let destination = cli.join(format!("{cli_stem}.data/{relative}"));
            fs::create_dir_all(destination.parent().ok_or("CLI file has no parent")?)?;
            fs::rename(
                python.join(format!("{python_stem}.data/{relative}")),
                destination,
            )?;
        }
        let info = cli.join(format!("{cli_stem}.dist-info"));
        fs::create_dir_all(info.join("licenses"))?;
        for license in ["LICENSE", "NOTICE"] {
            fs::copy(
                python.join(format!("{python_stem}.dist-info/licenses/{license}")),
                info.join("licenses").join(license),
            )?;
        }
        fs::write(
            info.join("METADATA"),
            format!(
                "Metadata-Version: 2.4
Name: delta-arrow-reader
Version: {version}
Summary: Inspect, preview, and query Delta Lake tables
Requires-Python: >=3.10
License-Expression: Apache-2.0
License-File: LICENSE
License-File: NOTICE
Project-URL: Repository, https://github.com/mag1cfrog/delta-arrow-reader
Project-URL: Documentation, https://mag1cfrog.github.io/delta-arrow-reader/cli-quickstart/
Description-Content-Type: text/markdown

# Delta Arrow Reader CLI

Inspect Delta Lake snapshots, preview rows, and run read-only SQL.

```console
uvx --from delta-arrow-reader dar --help
```

See the [CLI quickstart](https://mag1cfrog.github.io/delta-arrow-reader/cli-quickstart/)
and [installation guide](https://mag1cfrog.github.io/delta-arrow-reader/installation/).
"
            ),
        )?;
        fs::write(
            info.join("WHEEL"),
            format!(
                "Wheel-Version: 1.0\nGenerator: delta-arrow-reader xtask\nRoot-Is-Purelib: false\nTag: {}\n",
                WheelContents::Cli.wheel_tag(platform)
            ),
        )?;
        let extension = if platform == "win_amd64" {
            "pyd"
        } else {
            "abi3.so"
        };
        let python_binary =
            python.join(format!("delta_arrow_reader/delta_arrow_reader.{extension}"));
        let mut identities = Vec::new();
        for binary in [&python_binary, &cli_binary] {
            let filename = binary.file_name().ok_or("native binary has no filename")?;
            fs::copy(binary, symbols.join(filename))?;
            let original_sha256 = file_sha256(binary)?;
            strip_native_symbols(binary, platform)?;
            identities.push(json!({
                "file": filename.to_str().ok_or("native filename is not UTF-8")?, "original_sha256": original_sha256,
                "installed_sha256": file_sha256(binary)?,
            }));
        }
        fs::write(
            symbols.join("binaries.json"),
            serde_json::to_vec_pretty(&json!({
                "version": version, "platform": platform, "binaries": identities,
            }))?,
        )?;
        for (directory, contents) in [
            (&python, WheelContents::PythonLibrary),
            (&cli, WheelContents::Cli),
        ] {
            let wheel = output.join(contents.filename(version, platform));
            write_wheel(directory, &wheel)?;
            check_wheel(&wheel, version, platform)?;
        }
        // Keep the input and both destinations untouched until every wheel passes validation.
        fs::create_dir(&self.symbols_directory)?;
        for entry in fs::read_dir(&symbols)? {
            let entry = entry?;
            fs::copy(entry.path(), self.symbols_directory.join(entry.file_name()))?;
        }
        fs::rename(output, &self.destination)?;
        Ok(())
    }
}

fn strip_native_symbols(binary: &Path, platform: &str) -> Result<()> {
    let binary = fs::canonicalize(binary)?;
    let (program, option) = match platform {
        "macosx_12_0_arm64" => ("strip", "-x"),
        "win_amd64" => ("llvm-strip", "--strip-debug"),
        _ => ("strip", "--strip-all"),
    };
    command_output(Command::new(program).arg(option).arg(&binary))?;
    if platform == "macosx_12_0_arm64" {
        // Wheel repair may have signed the library before stripping changed its bytes.
        command_output(
            Command::new("codesign")
                .args(["--force", "--sign", "-"])
                .arg(&binary),
        )?;
        command_output(
            Command::new("codesign")
                .args(["--verify", "--strict"])
                .arg(&binary),
        )?;
    }
    Ok(())
}

fn unpack_checked_wheel(source: &Path, destination: &Path) -> Result<()> {
    let mut buffer = vec![0; rawzip::RECOMMENDED_BUFFER_SIZE];
    let archive = ZipArchive::from_file(File::open(source)?, &mut buffer)?;
    let mut entries = archive.entries(&mut buffer);
    while let Some(entry) = entries.next_entry()? {
        if entry.is_dir() {
            continue;
        }
        let name = entry.file_path().try_normalize()?.as_str().to_owned();
        if name.ends_with(".dist-info/RECORD") {
            continue;
        }
        let path = destination.join(name);
        fs::create_dir_all(path.parent().ok_or("wheel file has no parent")?)?;
        let local = archive.get_entry(entry.wayfinder())?;
        let reader: Box<dyn Read + '_> = match entry.compression_method() {
            CompressionMethod::STORE => Box::new(local.reader()),
            CompressionMethod::DEFLATE => Box::new(DeflateDecoder::new(local.reader())),
            _ => return Err("unsupported wheel compression".into()),
        };
        io::copy(
            &mut local.verifying_reader(reader),
            &mut File::create(&path)?,
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                &path,
                fs::Permissions::from_mode(entry.mode().permissions()),
            )?;
        }
    }
    Ok(())
}

fn write_wheel(source: &Path, destination: &Path) -> Result<()> {
    let mut files = Vec::new();
    collect_files(source, &mut files)?;
    files.sort();
    let mut archive = ZipArchiveWriter::new(File::create(destination)?);
    let mut record = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    let record_name = files
        .iter()
        .find_map(|path| {
            let name = path.strip_prefix(source).ok()?.to_str()?.replace('\\', "/");
            name.strip_suffix("/WHEEL")
                .map(|info| format!("{info}/RECORD"))
        })
        .ok_or("wheel has no WHEEL metadata")?;
    for path in files {
        let name = path
            .strip_prefix(source)?
            .to_str()
            .ok_or("wheel path is not UTF-8")?
            .replace('\\', "/");
        let mut hash = Sha256::new();
        let size = io::copy(&mut File::open(&path)?, &mut hash)?;
        record.write_record([
            name.as_str(),
            &format!("sha256={}", URL_SAFE_NO_PAD.encode(hash.finalize())),
            &size.to_string(),
        ])?;
        let executable = name.contains(".data/scripts/");
        write_compressed_entry(
            &mut archive,
            &name,
            File::open(path)?,
            if executable { 0o755 } else { 0o644 },
        )?;
    }
    record.write_record([record_name.as_str(), "", ""])?;
    write_compressed_entry(
        &mut archive,
        &record_name,
        record.into_inner()?.as_slice(),
        0o644,
    )?;
    archive.finish()?;
    Ok(())
}

fn collect_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_files(&entry.path(), files)?;
        } else {
            files.push(entry.path());
        }
    }
    Ok(())
}

fn write_compressed_entry(
    archive: &mut ZipArchiveWriter<File>,
    name: &str,
    mut source: impl Read,
    permissions: u32,
) -> Result<()> {
    let (mut entry, config) = archive
        .new_file(EntryPath::verbatim(name.as_bytes()))
        .compression_method(CompressionMethod::DEFLATE)
        .unix_permissions(0o100000 | permissions)
        .start()?;
    let mut writer = config.wrap(DeflateEncoder::new(&mut entry, Compression::best()));
    io::copy(&mut source, &mut writer)?;
    let (encoder, descriptor) = writer.finish()?;
    encoder.finish()?;
    entry.finish(descriptor)?;
    Ok(())
}
