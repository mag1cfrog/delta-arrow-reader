use std::{
    collections::BTreeMap,
    env, fs,
    io::{self, Write},
    path::Path,
};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    Result,
    python_wheels::{WheelContents, check_wheel_artifacts},
    run,
};

#[derive(Deserialize)]
struct IndexRelease {
    info: PackageInfo,
    urls: Vec<PublishedFile>,
}

#[derive(Deserialize)]
struct PackageInfo {
    name: String,
    version: String,
}

#[derive(Deserialize)]
struct PublishedFile {
    filename: String,
    digests: FileDigests,
    yanked: bool,
}

#[derive(Deserialize)]
struct FileDigests {
    sha256: String,
}

fn fetch_published_hashes(
    index: &str,
    package: &str,
    version: &str,
) -> Result<BTreeMap<String, String>> {
    let host = match index {
        "pypi" => "pypi.org",
        "testpypi" => "test.pypi.org",
        _ => return Err("index must be pypi or testpypi".into()),
    };
    let mut encoded_version = String::new();
    for byte in version.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded_version.push(char::from(byte));
        } else {
            encoded_version.push_str(&format!("%{byte:02X}"));
        }
    }
    let url = format!("https://{host}/pypi/{package}/{encoded_version}/json");
    let response = String::from_utf8(run(
        "curl",
        &[
            "--disable",
            "--silent",
            "--show-error",
            "--location",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "--max-time",
            "30",
            "--header",
            "Accept: application/json",
            "--header",
            "Cache-Control: no-cache",
            "--write-out",
            "\n%{http_code}",
            "--url",
            &url,
        ],
    )?)?;
    let (body, status) = response
        .rsplit_once('\n')
        .ok_or("curl response has no HTTP status")?;
    match status {
        "404" => return Ok(BTreeMap::new()),
        "200" => {}
        _ => return Err(format!("package index returned HTTP {status}").into()),
    }
    let release: IndexRelease = serde_json::from_str(body)?;
    let mut normalized_name = String::new();
    for character in release.info.name.chars() {
        if matches!(character, '-' | '_' | '.') {
            if !normalized_name.ends_with('-') {
                normalized_name.push('-');
            }
        } else {
            normalized_name.push(character.to_ascii_lowercase());
        }
    }
    if normalized_name != package || release.info.version != version {
        return Err("index response does not match the requested package and version".into());
    }
    let mut hashes = BTreeMap::new();
    for entry in release.urls {
        let filename = entry.filename;
        let sha256 = entry.digests.sha256;
        if filename.is_empty() {
            return Err("published file has no filename".into());
        }
        if sha256.len() != 64
            || !sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(format!("invalid SHA256 for {filename}").into());
        }
        if entry.yanked {
            return Err(format!("cannot resume a release with yanked files: {filename}").into());
        }
        if hashes.insert(filename.clone(), sha256).is_some() {
            return Err(format!("duplicate published filename: {filename}").into());
        }
    }
    Ok(hashes)
}

pub fn stage_missing_wheels(
    artifacts: &Path,
    version: &str,
    index: &str,
    destination: &Path,
) -> Result<()> {
    let mut wheels = BTreeMap::new();
    for path in check_wheel_artifacts(artifacts, version)? {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("wheel path has no UTF-8 filename")?
            .to_owned();
        wheels.insert(filename, path);
    }
    let mut published_hashes = BTreeMap::new();
    for package in [WheelContents::PythonLibrary, WheelContents::Cli] {
        let hashes = fetch_published_hashes(index, package.package_name(), version)?;
        let prefix = format!("{}-{version}-", package.package_name().replace('-', "_"));
        if hashes.keys().any(|name| !name.starts_with(&prefix)) {
            return Err("unexpected published file for package".into());
        }
        published_hashes.extend(hashes);
    }
    for (filename, expected_hash) in &published_hashes {
        let path = wheels
            .get(filename)
            .ok_or_else(|| format!("unexpected published file: {filename}"))?;
        let mut hasher = Sha256::new();
        io::copy(&mut fs::File::open(path)?, &mut hasher)?;
        if format!("{:x}", hasher.finalize()) != *expected_hash {
            return Err(format!(
                "SHA256 mismatch for {filename}; changed binaries need a new version"
            )
            .into());
        }
    }

    // Validate every published file before staging any pending upload.
    fs::create_dir(destination)?;
    let mut staged_count = 0;
    for (filename, path) in wheels {
        if !published_hashes.contains_key(&filename) {
            fs::copy(path, destination.join(filename))?;
            staged_count += 1;
        }
    }
    if let Some(output) = env::var_os("GITHUB_OUTPUT").filter(|path| !path.is_empty()) {
        let mut output = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(output)?;
        writeln!(output, "has-files={}", staged_count != 0)?;
    }
    println!("Staged {staged_count} missing wheel(s) for {index}");
    Ok(())
}
