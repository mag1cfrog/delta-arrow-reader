use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use flate2::read::DeflateDecoder;
use rawzip::{CompressionMethod, ZipArchive};

use crate::Result;

pub const ARTIFACT_PLATFORM_TAGS: [(&str, &str); 4] = [
    (
        "delta-arrow-reader-linux-x86_64-wheel",
        "manylinux_2_28_x86_64",
    ),
    ("delta-arrow-reader-windows-x86_64-wheel", "win_amd64"),
    ("delta-arrow-reader-macos-arm64-wheel", "macosx_12_0_arm64"),
    (
        "delta-arrow-reader-macos-x86_64-wheel",
        "macosx_12_0_x86_64",
    ),
];

pub fn check_wheel(path: &Path, version: &str, platform: &str) -> Result<()> {
    let tag = format!("cp310-abi3-{platform}");
    let filename = format!("delta_arrow_reader-{version}-{tag}.whl");
    if path.file_name().and_then(|name| name.to_str()) != Some(&filename) {
        return Err(format!("expected wheel {filename:?}, got {}", path.display()).into());
    }

    let mut buffer = vec![0; rawzip::RECOMMENDED_BUFFER_SIZE];
    let mut header_buffer = vec![0; rawzip::RECOMMENDED_BUFFER_SIZE];
    let archive = ZipArchive::from_file(fs::File::open(path)?, &mut buffer)?;
    for (name, expected_headers) in [
        (
            "METADATA",
            [("Name", "delta-arrow-reader"), ("Version", version)],
        ),
        (
            "WHEEL",
            [("Root-Is-Purelib", "false"), ("Tag", tag.as_str())],
        ),
    ] {
        let expected_path = format!("delta_arrow_reader-{version}.dist-info/{name}");
        let suffix = format!(".dist-info/{name}");
        let mut contents = None;
        let mut count = 0;
        let mut entries = archive.entries(&mut buffer);
        // Inspect every record so duplicate metadata names cannot be hidden by a map.
        while let Some(entry) = entries.next_entry()? {
            count += 1;
            let member = entry.file_path();
            // Installers can truncate NULs or treat backslashes as path separators.
            if member.as_bytes().contains(&0) || member.as_bytes().contains(&b'\\') {
                return Err("ambiguous ZIP member name".into());
            }
            if !member.as_bytes().ends_with(suffix.as_bytes()) {
                continue;
            }
            if contents.is_some() || member.as_bytes() != expected_path.as_bytes() {
                return Err(format!("expected exactly one {expected_path:?}").into());
            }
            let local = archive.get_entry(entry.wayfinder())?;
            let header = local.local_header(&mut header_buffer)?;
            if header.file_path().as_bytes() != member.as_bytes()
                || header.compression_method() != entry.compression_method()
            {
                return Err(format!("ZIP local header does not match {expected_path}").into());
            }
            if entry.flags().is_encrypted() || header.flags().is_encrypted() {
                return Err(format!("encrypted wheel metadata: {expected_path}").into());
            }
            let reader: Box<dyn Read + '_> = match entry.compression_method() {
                CompressionMethod::STORE => Box::new(local.reader()),
                CompressionMethod::DEFLATE => Box::new(DeflateDecoder::new(local.reader())),
                method => return Err(format!("unsupported wheel compression: {method}").into()),
            };
            let mut text = String::new();
            local.verifying_reader(reader).read_to_string(&mut text)?;
            contents = Some(text);
        }
        if count != archive.entries_hint() {
            return Err("ZIP central directory entry count does not match".into());
        }
        let contents = contents.ok_or_else(|| format!("expected exactly one {expected_path:?}"))?;
        check_headers(name, &contents, &expected_headers)?;
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

pub fn check_wheel_set(directory: &Path, version: &str) -> Result<Vec<PathBuf>> {
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
        if paths.len() != 1 {
            return Err(format!(
                "{artifact}: expected one wheel, got {} entries",
                paths.len()
            )
            .into());
        }
        check_wheel(&paths[0], version, platform)?;
        wheels.extend(paths);
    }
    Ok(wheels)
}
