use std::{
    collections::BTreeMap,
    env, fs,
    io::{self, Write},
    path::Path,
};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{Result, python_wheels::check_wheel_set, run};

#[derive(Deserialize)]
struct PublishedRelease {
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
    digests: Digests,
    yanked: bool,
}

#[derive(Deserialize)]
struct Digests {
    sha256: String,
}

fn published_files(index: &str, version: &str) -> Result<BTreeMap<String, String>> {
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
    let url = format!("https://{host}/pypi/delta-arrow-reader/{encoded_version}/json");
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
    let release: PublishedRelease = serde_json::from_str(body)?;
    let mut name = String::new();
    for character in release.info.name.chars() {
        if matches!(character, '-' | '_' | '.') {
            if !name.ends_with('-') {
                name.push('-');
            }
        } else {
            name.push(character.to_ascii_lowercase());
        }
    }
    if name != "delta-arrow-reader" || release.info.version != version {
        return Err("index response does not match the requested package and version".into());
    }
    let mut files = BTreeMap::new();
    for entry in release.urls {
        let filename = entry.filename;
        let digest = entry.digests.sha256;
        if filename.is_empty() {
            return Err("published file has no filename".into());
        }
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(format!("invalid SHA256 for {filename}").into());
        }
        if entry.yanked {
            return Err(format!("cannot resume a release with yanked files: {filename}").into());
        }
        if files.insert(filename.clone(), digest).is_some() {
            return Err(format!("duplicate published filename: {filename}").into());
        }
    }
    Ok(files)
}

pub fn prepare_upload(
    artifacts: &Path,
    version: &str,
    index: &str,
    destination: &Path,
) -> Result<()> {
    let mut wheels = BTreeMap::new();
    for path in check_wheel_set(artifacts, version)? {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("wheel path has no UTF-8 filename")?
            .to_owned();
        wheels.insert(filename, path);
    }
    let published = published_files(index, version)?;
    for (filename, expected_hash) in &published {
        let path = wheels
            .get(filename)
            .ok_or_else(|| format!("unexpected published file: {filename}"))?;
        let mut hash = Sha256::new();
        io::copy(&mut fs::File::open(path)?, &mut hash)?;
        if format!("{:x}", hash.finalize()) != *expected_hash {
            return Err(format!(
                "SHA256 mismatch for {filename}; changed binaries need a new version"
            )
            .into());
        }
    }

    // Validate every published file before staging any pending upload.
    fs::create_dir(destination)?;
    let mut count = 0;
    for (filename, path) in wheels {
        if !published.contains_key(&filename) {
            fs::copy(path, destination.join(filename))?;
            count += 1;
        }
    }
    if let Some(output) = env::var_os("GITHUB_OUTPUT").filter(|path| !path.is_empty()) {
        let mut output = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(output)?;
        writeln!(output, "has-files={}", count != 0)?;
    }
    println!("Staged {count} missing wheel(s) for {index}");
    Ok(())
}
