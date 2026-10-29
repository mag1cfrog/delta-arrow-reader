use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use flate2::read::DeflateDecoder;
use rawzip::{CompressionMethod, ZipArchive};
use sha2::{Digest, Sha256};

use crate::Result;

pub(crate) const ARTIFACT_PLATFORM_TAGS: [(&str, &str); 3] = [
    (
        "delta-arrow-reader-linux-x86_64-wheel",
        "manylinux_2_28_x86_64",
    ),
    ("delta-arrow-reader-windows-x86_64-wheel", "win_amd64"),
    ("delta-arrow-reader-macos-arm64-wheel", "macosx_12_0_arm64"),
];

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum WheelContents {
    PythonLibrary,
    Cli,
    CombinedBuild,
}

impl WheelContents {
    pub(crate) fn package_name(self) -> &'static str {
        match self {
            Self::Cli => "delta-arrow-reader-cli",
            Self::PythonLibrary | Self::CombinedBuild => "delta-arrow-reader",
        }
    }

    pub(crate) fn wheel_tag(self, platform: &str) -> String {
        let abi = if self == Self::Cli {
            "py3-none"
        } else {
            "cp310-abi3"
        };
        format!("{abi}-{platform}")
    }

    pub(crate) fn filename(self, version: &str, platform: &str) -> String {
        format!(
            "{}-{version}-{}.whl",
            self.package_name().replace('-', "_"),
            self.wheel_tag(platform)
        )
    }
}

pub fn check_wheel(path: &Path, version: &str, platform_tag: &str) -> Result<()> {
    let contents = if path.file_name().is_some_and(|name| {
        name.to_string_lossy()
            .starts_with("delta_arrow_reader_cli-")
    }) {
        WheelContents::Cli
    } else {
        WheelContents::PythonLibrary
    };
    check_wheel_contents(path, version, platform_tag, contents)
}

pub(crate) fn check_wheel_contents(
    path: &Path,
    version: &str,
    platform_tag: &str,
    contents: WheelContents,
) -> Result<()> {
    let wheel_tag = contents.wheel_tag(platform_tag);
    let filename = contents.filename(version, platform_tag);
    if path.file_name().and_then(|name| name.to_str()) != Some(&filename) {
        return Err(format!("expected wheel {filename:?}, got {}", path.display()).into());
    }
    let (extension_suffix, cli_suffix, native_signature): (&str, &str, &[u8]) = match platform_tag {
        "manylinux_2_28_x86_64" | "linux_x86_64" => ("abi3.so", "", b"\x7fELF"),
        "macosx_12_0_arm64" => ("abi3.so", "", b"\xcf\xfa\xed\xfe"),
        "win_amd64" => ("pyd", ".exe", b"MZ"),
        _ => return Err(format!("unsupported wheel platform: {platform_tag}").into()),
    };
    let distribution = contents.package_name().replace('-', "_");
    let dist_info = format!("{distribution}-{version}.dist-info");
    let data = format!("{distribution}-{version}.data");
    let cli = format!("{data}/scripts/delta-arrow-reader{cli_suffix}");
    let extension = format!("delta_arrow_reader/delta_arrow_reader.{extension_suffix}");
    let metadata_path = format!("{dist_info}/METADATA");
    let wheel_path = format!("{dist_info}/WHEEL");
    let record_path = format!("{dist_info}/RECORD");
    let mut required = vec![
        metadata_path.clone(),
        wheel_path.clone(),
        record_path.clone(),
        format!("{dist_info}/licenses/LICENSE"),
        format!("{dist_info}/licenses/NOTICE"),
    ];
    if contents != WheelContents::Cli {
        required.extend([
            "delta_arrow_reader/__init__.py".into(),
            "delta_arrow_reader/__init__.pyi".into(),
            "delta_arrow_reader/py.typed".into(),
            extension.clone(),
            format!("{data}/data/share/delta-arrow-reader/python-THIRD_PARTY_NOTICES.txt"),
        ]);
    }
    if contents != WheelContents::PythonLibrary {
        required.extend([
            cli.clone(),
            format!("{data}/data/share/delta-arrow-reader/cli-THIRD_PARTY_NOTICES.txt"),
        ]);
    }
    let mut buffer = vec![0; rawzip::RECOMMENDED_BUFFER_SIZE];
    let mut header_buffer = vec![0; rawzip::RECOMMENDED_BUFFER_SIZE];
    let archive = ZipArchive::from_file(fs::File::open(path)?, &mut buffer)?;
    let mut records = BTreeMap::new();
    let mut metadata = BTreeMap::new();
    let mut names = BTreeSet::new();
    let mut directories = Vec::new();
    let mut count = 0;
    let mut entries = archive.entries(&mut buffer);
    // Inspect every record before insertion so duplicate or aliased names cannot be hidden.
    while let Some(entry) = entries.next_entry()? {
        count += 1;
        let member = entry.file_path();
        let normalized = member.try_normalize()?;
        let name = normalized.as_str();
        if name.as_bytes() != member.as_bytes()
            || name
                .bytes()
                .any(|byte| byte.is_ascii_control() || b"<>:\"|?*".contains(&byte))
            || name.split('/').any(|part| part.ends_with(['.', ' ']))
        {
            return Err("ambiguous ZIP member name".into());
        }
        if !names.insert(name.trim_end_matches('/').to_ascii_lowercase()) {
            return Err(format!("duplicate or aliased ZIP member: {name}").into());
        }
        let is_directory = entry.is_dir();
        if entry.mode().value() & 0o170000 != if is_directory { 0o040000 } else { 0o100000 } {
            return Err(format!("wheel member must be a regular file or directory: {name}").into());
        }
        let allowed = is_directory
            || required.iter().any(|path| path == name)
            || (name.starts_with(&format!("{dist_info}/sboms/")) && name.ends_with(".json"))
            || (platform_tag == "win_amd64" && name == format!("{dist_info}/DELVEWHEEL"))
            || (contents != WheelContents::Cli && is_bundled_library(name, platform_tag));
        if !allowed {
            return Err(format!("unexpected wheel member: {name}").into());
        }
        if name == cli && cli_suffix.is_empty() && entry.mode().permissions() & 0o111 != 0o111 {
            return Err(format!("CLI must be executable by every user: {name}").into());
        }
        if name == cli
            && cli_suffix.is_empty()
            && (entry.external_attributes() >> 16) & 0o170000 != 0o100000
        {
            return Err(format!("CLI must declare a regular file type for pip: {name}").into());
        }
        let local = archive.get_entry(entry.wayfinder())?;
        let header = local.local_header(&mut header_buffer)?;
        if header.file_path().as_bytes() != member.as_bytes()
            || header.compression_method() != entry.compression_method()
        {
            return Err(format!("ZIP local header does not match {name}").into());
        }
        if entry.flags().is_encrypted() || header.flags().is_encrypted() {
            return Err(format!("encrypted wheel member: {name}").into());
        }
        let reader: Box<dyn Read + '_> = match entry.compression_method() {
            CompressionMethod::STORE => Box::new(local.reader()),
            CompressionMethod::DEFLATE => Box::new(DeflateDecoder::new(local.reader())),
            method => return Err(format!("unsupported wheel compression: {method}").into()),
        };
        let mut reader = local.verifying_reader(reader);
        let mut hash = Sha256::new();
        let size = if name == metadata_path || name == wheel_path || name == record_path {
            let mut contents = String::new();
            // Bound buffered metadata; native payloads are hashed without retaining them in memory.
            const MAX_METADATA_BYTES: u64 = 16 * 1024 * 1024;
            reader
                .by_ref()
                .take(MAX_METADATA_BYTES + 1)
                .read_to_string(&mut contents)?;
            if contents.len() as u64 > MAX_METADATA_BYTES {
                return Err(format!("wheel metadata exceeds 16 MiB: {name}").into());
            }
            hash.update(contents.as_bytes());
            let size = contents.len() as u64;
            metadata.insert(name.to_owned(), contents);
            size
        } else {
            let mut prefix_size = 0;
            if name == cli || name == extension {
                let mut prefix = vec![0; native_signature.len()];
                reader.read_exact(&mut prefix)?;
                if prefix != native_signature {
                    return Err(format!("expected native {platform_tag} payload: {name}").into());
                }
                hash.update(&prefix);
                prefix_size = prefix.len() as u64;
            }
            prefix_size + io::copy(&mut reader, &mut hash)?
        };
        if is_directory {
            if size != 0 {
                return Err(format!("nonempty ZIP directory: {name}").into());
            }
            directories.push(name.to_owned());
        } else {
            if size == 0 && name != "delta_arrow_reader/py.typed" {
                return Err(format!("empty wheel file: {name}").into());
            }
            records.insert(
                name.to_owned(),
                (URL_SAFE_NO_PAD.encode(hash.finalize()), size),
            );
        }
    }
    if count != archive.entries_hint() {
        return Err("ZIP central directory entry count does not match".into());
    }
    for name in &required {
        if !records.contains_key(name) {
            return Err(format!("missing required wheel file: {name}").into());
        }
    }
    for directory in directories {
        if !records.keys().any(|name| name.starts_with(&directory)) {
            return Err(format!("unexpected wheel directory: {directory}").into());
        }
    }
    check_headers(
        "METADATA",
        &metadata[&metadata_path],
        &[("Name", contents.package_name()), ("Version", version)],
    )?;
    if contents == WheelContents::Cli
        && metadata[&metadata_path]
            .lines()
            .take_while(|line| !line.is_empty())
            .any(|line| {
                line.split_once(':')
                    .is_some_and(|(name, _)| name.eq_ignore_ascii_case("Requires-Dist"))
            })
    {
        return Err("CLI wheel must not install Python dependencies".into());
    }
    check_headers(
        "WHEEL",
        &metadata[&wheel_path],
        &[("Root-Is-Purelib", "false"), ("Tag", &wheel_tag)],
    )?;
    check_record(&metadata[&record_path], &record_path, records)
}

fn is_bundled_library(name: &str, platform_tag: &str) -> bool {
    // Preserve the dependency directories emitted by Maturin and the existing repair tools.
    let prefixes: &[&str] = if platform_tag.starts_with("macosx_") {
        &["delta_arrow_reader.dylibs/", "delta_arrow_reader/.dylibs/"]
    } else {
        &["delta_arrow_reader.libs/"]
    };
    let Some(filename) = prefixes.iter().find_map(|prefix| name.strip_prefix(prefix)) else {
        return false;
    };
    if filename.contains('/') {
        return false;
    }
    match platform_tag {
        "win_amd64" => filename.to_ascii_lowercase().ends_with(".dll"),
        "macosx_12_0_arm64" => filename.ends_with(".dylib"),
        _ => {
            filename.ends_with(".so")
                || filename.split_once(".so.").is_some_and(|(_, suffix)| {
                    !suffix.is_empty()
                        && suffix
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || byte == b'.')
                })
        }
    }
}

fn check_record(
    contents: &str,
    record_path: &str,
    mut files: BTreeMap<String, (String, u64)>,
) -> Result<()> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(contents.as_bytes());
    for row in reader.records() {
        let row = row?;
        if row.len() != 3 {
            return Err("RECORD: expected path, hash, and size".into());
        }
        let name = &row[0];
        let (hash, size) = files
            .remove(name)
            .ok_or_else(|| format!("RECORD: duplicate or unknown path: {name}"))?;
        let matches = if name == record_path {
            row[1].is_empty() && row[2].is_empty()
        } else {
            row[1] == format!("sha256={hash}") && row[2] == size.to_string()
        };
        if !matches {
            return Err(format!("RECORD: incorrect hash or size for {name}").into());
        }
    }
    if !files.is_empty() {
        return Err(format!(
            "RECORD: missing files: {:?}",
            files.keys().collect::<Vec<_>>()
        )
        .into());
    }
    Ok(())
}

fn check_headers(name: &str, contents: &str, expected: &[(&str, &str)]) -> Result<()> {
    let mut headers: Vec<(&str, String)> = Vec::new();
    for line in contents.lines().take_while(|line| !line.is_empty()) {
        if line.starts_with([' ', '\t']) {
            let Some((_, value)) = headers.last_mut() else {
                return Err(format!("{name}: header continuation has no preceding field").into());
            };
            value.push('\n');
            value.push_str(line);
        } else if let Some((field, value)) = line.split_once(':') {
            if field.is_empty() || !field.bytes().all(|byte| byte.is_ascii_graphic()) {
                break;
            }
            headers.push((field, value.trim_start_matches([' ', '\t']).to_owned()));
        } else {
            break;
        }
    }
    for (field, value) in expected {
        let actual: Vec<_> = headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(field))
            .map(|(_, value)| value.as_str())
            .collect();
        if actual != [*value] {
            return Err(format!("{name}: expected one {field}: {value:?}, got {actual:?}").into());
        }
    }
    Ok(())
}

pub fn check_wheel_artifacts(directory: &Path, version: &str) -> Result<Vec<PathBuf>> {
    let mut artifacts = fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    artifacts.sort();
    let mut expected: Vec<_> = ARTIFACT_PLATFORM_TAGS
        .iter()
        .map(|(name, _)| *name)
        .collect();
    expected.sort();
    if artifacts != expected {
        return Err(format!("expected artifacts {expected:?}, got {artifacts:?}").into());
    }
    let mut wheels = Vec::new();
    for (artifact, platform) in ARTIFACT_PLATFORM_TAGS {
        let paths = fs::read_dir(directory.join(artifact))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        let mut actual: Vec<_> = paths
            .iter()
            .map(|path| {
                path.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        actual.sort();
        let mut expected = [WheelContents::PythonLibrary, WheelContents::Cli]
            .map(|contents| contents.filename(version, platform));
        expected.sort();
        if actual != expected {
            return Err(format!(
                "{artifact}: expected Python and CLI wheels {expected:?}, got {actual:?}"
            )
            .into());
        }
        for path in &paths {
            check_wheel(path, version, platform)?;
        }
        wheels.extend(paths);
    }
    Ok(wheels)
}
