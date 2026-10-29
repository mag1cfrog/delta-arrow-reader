//! Release wheels keep their native code while separating installation ownership.

#![cfg(target_os = "linux")]

use std::{collections::BTreeSet, fs, path::Path, process::Command};

use serde_json::Value;
use sha2::{Digest, Sha256};

#[allow(dead_code)]
mod support;
use support::{Result, combined_wheel_files, wheel_filename, write_wheel};

fn split(source: &Path, root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command
        .arg("split-python-wheel")
        .arg(source)
        .args([
            "--expected-version",
            "1.2.3",
            "--platform-tag",
            "manylinux_2_28_x86_64",
            "--destination",
        ])
        .arg(root.join("wheels"))
        .arg("--symbols-directory")
        .arg(root.join("symbols"));
    command
}

fn members(path: &Path) -> Result<BTreeSet<String>> {
    let archive = rawzip::ZipArchive::from_slice(fs::read(path)?)?;
    let mut entries = archive.entries();
    let mut names = BTreeSet::new();
    while let Some(entry) = entries.next_entry()? {
        assert!(
            !entry.flags().has_data_descriptor(),
            "PyPI rejects ZIP data descriptors"
        );
        let local = archive.get_entry(entry.wayfinder())?;
        assert!(!local.local_header().flags().has_data_descriptor());
        // pip requires S_IFREG as well as execute bits; mode() supplies a missing type.
        assert_eq!((entry.external_attributes() >> 16) & 0o170000, 0o100000);
        names.insert(entry.file_path().try_normalize()?.as_str().to_owned());
    }
    Ok(names)
}

#[test]
fn splits_real_native_payloads_preserves_symbols_and_validates_both_distributions() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = root.path().join("native.rs");
    fs::write(&source, "fn main() { println!(\"dar 1.2.3\"); }")?;
    let native = root.path().join("native");
    let output = Command::new("rustc")
        .arg(&source)
        .arg("-o")
        .arg(&native)
        .output()?;
    assert!(output.status.success(), "{output:?}");
    let native_bytes = fs::read(native)?;
    let tag = "manylinux_2_28_x86_64";
    let mut files = combined_wheel_files(tag);
    for (name, data) in &mut files {
        if name.ends_with("/dar") || name.ends_with(".abi3.so") {
            *data = native_bytes.clone();
        }
    }
    let source = root.path().join(wheel_filename(tag));
    write_wheel(&source, &files, true)?;
    let input = fs::read(&source)?;
    let output = split(&source, root.path()).output()?;
    assert!(output.status.success(), "{output:?}");
    assert_eq!(fs::read(&source)?, input);
    let python = root.path().join("wheels").join(wheel_filename(tag));
    let cli = root
        .path()
        .join("wheels/delta_arrow_reader-1.2.3-py3-none-manylinux_2_28_x86_64.whl");
    let library_members = members(&python)?;
    let cli_members = members(&cli)?;
    assert!(!library_members.iter().any(|name| name.contains("/scripts/") || name.ends_with("cli-THIRD_PARTY_NOTICES.txt")));
    assert!(
        !cli_members
            .iter()
            .any(|name| name.starts_with("delta_arrow_reader/")
                || name.ends_with("python-THIRD_PARTY_NOTICES.txt"))
    );
    assert!(library_members.is_disjoint(&cli_members));
    assert_eq!(
        cli_members
            .iter()
            .filter(|name| name.contains("/scripts/"))
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["delta_arrow_reader-1.2.3.data/scripts/dar"]
    );
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("check-python-wheel")
        .args([&python, &cli])
        .args(["--expected-version", "1.2.3", "--platform-tag", tag])
        .output()?;
    assert!(output.status.success(), "{output:?}");
    let identities: Value =
        serde_json::from_slice(&fs::read(root.path().join("symbols/binaries.json"))?)?;
    assert_eq!(identities["version"], "1.2.3");
    for filename in ["dar", "delta_arrow_reader.abi3.so"] {
        assert_eq!(
            fs::read(root.path().join("symbols").join(filename))?,
            native_bytes
        );
        let entry = identities["binaries"]
            .as_array()
            .ok_or("missing identities")?
            .iter()
            .find(|entry| entry["file"] == filename)
            .ok_or("missing native identity")?;
        assert_eq!(
            entry["original_sha256"],
            format!("{:x}", Sha256::digest(&native_bytes))
        );
        assert_ne!(entry["installed_sha256"], entry["original_sha256"]);
    }
    assert!(!split(&source, root.path()).output()?.status.success());
    assert_eq!(fs::read(&source)?, input);
    Ok(())
}

#[test]
fn invalid_inputs_and_failed_stripping_leave_no_publishable_outputs() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = root.path().join(wheel_filename("manylinux_2_28_x86_64"));
    let mut files = combined_wheel_files("manylinux_2_28_x86_64");
    // The signature passes layout validation, but the payload cannot be stripped.
    write_wheel(&source, &files, true)?;
    let output = split(&source, root.path()).output()?;
    assert!(!output.status.success());
    assert!(!root.path().join("wheels").exists());
    assert!(!root.path().join("symbols").exists());
    files.retain(|(name, _)| !name.contains("/scripts/"));
    write_wheel(&source, &files, true)?;
    let output = split(&source, root.path()).output()?;
    assert!(String::from_utf8_lossy(&output.stderr).contains("missing required wheel file"));
    assert!(!root.path().join("wheels").exists());
    assert!(!root.path().join("symbols").exists());
    Ok(())
}
