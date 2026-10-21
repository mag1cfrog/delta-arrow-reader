//! Dependency notice generation failures preserve existing output.

#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, process::Command};

use serde_json::json;

#[test]
fn tool_failure_does_not_create_or_replace_output() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let rustup = temporary.path().join("rustup");
    fs::write(
        &rustup,
        "#!/bin/sh\necho 'simulated cargo-about failure' >&2\nexit 17\n",
    )?;
    fs::set_permissions(&rustup, fs::Permissions::from_mode(0o755))?;
    let output = temporary.path().join("notices.txt");
    for existing in [false, true] {
        if existing {
            fs::write(&output, "Previously validated notices\n")?;
        }
        let result = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args(["cli", "notices", "--architecture", "amd64", "--output"])
            .arg(&output)
            .env("PATH", temporary.path())
            .output()?;
        assert!(!result.status.success());
        let stderr = String::from_utf8(result.stderr)?;
        assert!(stderr.contains("simulated cargo-about failure"), "{stderr}");
        if existing {
            assert_eq!(
                fs::read_to_string(&output)?,
                "Previously validated notices\n"
            );
        } else {
            assert!(!output.exists());
        }
    }
    Ok(())
}

#[test]
fn cached_scans_recover_from_corruption_and_still_validate_original_files()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let workspace: toml::Value =
        toml::from_str(&fs::read_to_string(repository.join("Cargo.toml"))?)?;
    let version = workspace["workspace"]["metadata"]["cli-release"]["cargo-about-version"]
        .as_str()
        .unwrap();
    fs::write(root.join("version"), format!("cargo-about {version}\n"))?;
    let package = json!({
        "id": "local", "name": "delta-arrow-reader", "version": "0.7.0",
        "manifest_path": repository.join("crates/delta-arrow-reader-core/Cargo.toml"),
        "source": null,
    });
    let metadata = json!({
        "packages": [package.clone()],
        "resolve": {"root": "local", "nodes": [{"id": "local", "dependencies": []}]},
    });
    fs::write(root.join("metadata.json"), metadata.to_string())?;
    fs::write(root.join("report.json"), json!({
        "crates": [{"package": package.clone()}],
        "licenses": [{"id": "Apache-2.0", "text": "scan result", "used_by": [{"crate": package}]}],
    }).to_string())?;
    let docs = root.join("sysroot/share/doc/rust");
    fs::create_dir_all(docs.join("licenses"))?;
    for name in [
        "COPYRIGHT-library.html",
        "licenses/Apache-2.0.txt",
        "licenses/MIT.txt",
    ] {
        fs::write(docs.join(name), "original license")?;
    }
    let rustup = root.join("rustup");
    fs::write(
        &rustup,
        r#"#!/bin/sh
set -eu
directory=${0%/*}
shift 2
case "$1 $2" in
  'cargo metadata') cat "$directory/metadata.json" ;;
  'cargo about')
    if [ "$3" = --version ]; then
      cat "$directory/version"
    else
      printf 'scan\n' >> "$directory/scans"
      test ! -f "$directory/fail-scan"
      cat "$directory/report.json"
    fi ;;
  'rustc --print') printf '%s/sysroot\n' "$directory" ;;
  *) exit 99 ;;
esac
"#,
    )?;
    fs::set_permissions(&rustup, fs::Permissions::from_mode(0o755))?;
    let path = std::env::join_paths([root.to_path_buf()].into_iter().chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))?;
    let run = |name: &str| {
        Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args(["cli", "notices", "--architecture", "amd64", "--output"])
            .arg(root.join(name))
            .env("PATH", &path)
            .env("DAR_CI_LICENSE_REPORTS", root.join("cache"))
            .output()
    };
    fs::write(root.join("fail-scan"), "")?;
    assert!(!run("failed.txt")?.status.success());
    assert!(!root.join("failed.txt").exists());
    assert!(!root.join("cache").exists());
    fs::remove_file(root.join("fail-scan"))?;
    fs::remove_file(docs.join("licenses/MIT.txt"))?;
    assert!(!run("invalid-notices.txt")?.status.success());
    assert!(!root.join("invalid-notices.txt").exists());
    assert!(!root.join("cache").exists());
    fs::write(docs.join("licenses/MIT.txt"), "original license")?;
    let result = run("cold.txt")?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    fs::write(root.join("fail-scan"), "")?;
    let result = run("warm.txt")?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        fs::read(root.join("cold.txt"))?,
        fs::read(root.join("warm.txt"))?
    );
    assert_eq!(
        fs::read_to_string(root.join("scans"))?,
        "scan\nscan\nscan\n"
    );
    fs::remove_file(docs.join("licenses/MIT.txt"))?;
    assert!(!run("missing-license.txt")?.status.success());
    assert!(!root.join("missing-license.txt").exists());
    fs::write(docs.join("licenses/MIT.txt"), "original license")?;
    let cache = fs::read_dir(root.join("cache"))?.next().unwrap()?.path();
    fs::write(&cache, "truncated JSON")?;
    fs::remove_file(root.join("fail-scan"))?;
    assert!(run("recovered.txt")?.status.success());
    assert_eq!(
        fs::read_to_string(root.join("scans"))?,
        "scan\nscan\nscan\nscan\n"
    );
    fs::write(root.join("fail-scan"), "")?;
    let mut changed = metadata;
    changed["packages"][0]["version"] = "0.8.0".into();
    fs::write(root.join("metadata.json"), changed.to_string())?;
    assert!(!run("changed.txt")?.status.success());
    assert!(!root.join("changed.txt").exists());
    Ok(())
}
