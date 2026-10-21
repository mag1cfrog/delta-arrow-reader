#![cfg(unix)]

use std::{
    error::Error,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;
const PIN: &str = "41f1ce23377f088298a3ed196ad6c8e40cb9bfed";

#[path = "selective_read_build/python.rs"]
mod python;

fn write(root: &Path, path: &str, contents: &str) -> Result {
    let path = root.join(path);
    fs::create_dir_all(path.parent().unwrap())?;
    fs::write(path, contents)?;
    Ok(())
}

fn fixture() -> Result<tempfile::TempDir> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    for path in [
        "Cargo.toml",
        "Cargo.lock",
        "README.md",
        ".cargo/config.toml",
        "xtask/Cargo.toml",
        "xtask/src/main.rs",
        "xtask/src/selective_read_build.rs",
        "crates/delta-arrow-reader-core/Cargo.toml",
        "crates/delta-arrow-reader-core/src/lib.rs",
        "benches/selective_read/oracle_io.py",
        "benches/selective_read/rust-toolchain.toml",
        "benches/selective_read/runners/common.rs",
        "benches/selective_read/runners/dar/Cargo.toml",
        "benches/selective_read/runners/dar/Cargo.lock",
        "benches/selective_read/runners/delta-rs/Cargo.toml",
        "benches/selective_read/runners/delta-rs/Cargo.lock",
        "docs/content/benchmarks/selective-read-protocol.md",
        "docs/content/benchmarks/selective-read-large-workloads.md",
        "docs/content/benchmarks/selective-read-sampling.md",
        "docs/content/benchmarks/selective-read-production-workloads.md",
        "docs/content/benchmarks/selective-read-spark-matrix.md",
    ] {
        write(root, path, "original source\n")?;
    }
    let metadata = json!({
        "packages": [
            {"id": "deltalake", "name": "deltalake", "version": "1.0.0",
             "source": "registry+test", "manifest_path": root.join("registry/Cargo.toml")},
            {"id": "reader", "name": "reader", "version": "0.1.0", "source": null,
             "manifest_path": root.join("benches/selective_read/runners/dar/Cargo.toml")},
            {"id": "unused", "name": "unused", "version": "0.1.0", "source": null, "manifest_path": "unused"}
        ],
        "resolve": {"nodes": [
            {"id": "reader", "features": []},
            {"id": "deltalake", "features": ["datafusion", "s3"]}
        ]}
    });
    write(
        root,
        "metadata.json",
        &(serde_json::to_string(&metadata)? + "\n"),
    )?;
    write(
        root,
        "registry/.cargo_vcs_info.json",
        &json!({"git": {"sha1": PIN, "dirty": true}}).to_string(),
    )?;
    for (name, script) in [
        (
            "cargo",
            r#"
if [ "$1" = locate-project ]; then
    printf '%s/Cargo.toml\n' "$PWD"
    exit
fi
printf '%s\n' "$*" >> commands
test "$1" = +1.98.1
shift
test "$RUSTFLAGS" = '-C target-cpu=x86-64'
test "$CARGO_INCREMENTAL" = 0
test "$CARGO_TARGET_DIR" = "$PWD/target/selective-read-runners/$MOCK_READER"
test "${CARGO_ENCODED_RUSTFLAGS-unset}" = unset
test "${CARGO_MANIFEST_DIR-unset}" = unset
test "${CARGO_PKG_NAME-unset}" = unset
test "${MOCK_FAIL-}" != "$1" || exit 19
case "$1" in
build)
    directory="$CARGO_TARGET_DIR/x86_64-unknown-linux-gnu/release"
    mkdir -p "$directory"
    printf 'binary\n' > "$directory/selective-read-$MOCK_READER"
    chmod +x "$directory/selective-read-$MOCK_READER"
    if [ "${MOCK_MUTATE-}" = yes ]; then printf 'changed\n' >> README.md; fi
    ;;
metadata) cat metadata.json ;;
tree) printf 'dependency tree\n' ;;
--version) printf 'cargo 1.98.1\n' ;;
*) exit 20 ;;
esac
"#,
        ),
        (
            "rustc",
            "test \"$*\" = '+1.98.1 -vV'\nprintf 'rustc 1.98.1\\nhost: x86_64-unknown-linux-gnu\\n'",
        ),
        (
            "cc",
            "test \"$*\" = --version\nprintf 'cc fixture\\nCopyright\\n'",
        ),
        (
            "git",
            r#"
case "$*" in
'rev-parse HEAD') printf '0123456789012345678901234567890123456789\n' ;;
'status --porcelain') printf ' M README.md\n' ;;
*) exit 20 ;;
esac
"#,
        ),
    ] {
        write(
            root,
            &format!("bin/{name}"),
            &format!("#!/bin/sh\nset -eu\n{script}\n"),
        )?;
        fs::set_permissions(
            root.join("bin").join(name),
            fs::Permissions::from_mode(0o755),
        )?;
    }
    Ok(temporary)
}

fn run(root: &Path, reader: &str, extra: &[(&str, &str)]) -> Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_xtask"))
        .current_dir(root)
        .args([
            "build-selective-read",
            "--reader",
            reader,
            "--output",
            "output",
        ])
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", root.join("bin").display()),
        )
        .env("CARGO", root.join("bin/cargo"))
        .env("MOCK_READER", reader)
        .env("RUSTFLAGS", "wrong flags")
        .env("CARGO_INCREMENTAL", "1")
        .env("CARGO_TARGET_DIR", "/wrong-target")
        .env("CARGO_ENCODED_RUSTFLAGS", "wrong encoded flags")
        .env("CARGO_MANIFEST_DIR", "/wrong-manifest")
        .env("CARGO_PKG_NAME", "xtask")
        .envs(extra.iter().copied())
        .output()?)
}

fn hash(path: &Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

#[test]
fn builds_both_readers_with_pinned_commands_and_complete_provenance() -> Result {
    for reader in ["dar", "delta-rs"] {
        let temporary = fixture()?;
        let root = temporary.path();
        let result = run(root, reader, &[])?;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let output = root.join("output");
        let binary = output.join(format!("selective-read-{reader}"));
        assert_eq!(
            String::from_utf8(result.stdout)?.trim(),
            binary.to_str().unwrap()
        );
        assert_ne!(fs::metadata(&binary)?.permissions().mode() & 0o111, 0);
        let record: Value = serde_json::from_slice(&fs::read(output.join("build.json"))?)?;
        assert_eq!(record["format"], "selective-read-build-v1");
        assert_eq!(
            record["reader_id"],
            if reader == "dar" {
                "delta-arrow-reader"
            } else {
                reader
            }
        );
        assert_eq!(
            record["reader_git_commit"],
            if reader == "dar" {
                record["harness_git_commit"].as_str().unwrap()
            } else {
                PIN
            }
        );
        assert_eq!(record["published_vcs"]["deltalake"]["git"]["dirty"], true);
        assert_eq!(record["git_status"], " M README.md\n");
        assert_eq!(record["rustflags"], "-C target-cpu=x86-64");
        assert_eq!(record["cargo"], "cargo 1.98.1");
        assert_eq!(record["linker"], "cc fixture");
        assert_eq!(
            record["dependencies"],
            json!([
                {"name": "deltalake", "version": "1.0.0", "source": "registry+test", "features": ["datafusion", "s3"]},
                {"name": "reader", "version": "0.1.0", "source": null, "features": []}
            ])
        );
        for (key, name) in [
            ("executable_sha256", format!("selective-read-{reader}")),
            ("lockfile_sha256", "Cargo.lock".into()),
            ("metadata_sha256", "metadata.json".into()),
            ("cargo_tree_sha256", "cargo-tree.txt".into()),
        ] {
            assert_eq!(record[key], hash(&output.join(name))?);
        }
        assert_eq!(
            record["builder_executable_sha256"],
            hash(Path::new(env!("CARGO_BIN_EXE_xtask")))?
        );
        let sources = record["source_sha256"].as_object().unwrap();
        assert_eq!(sources.len(), 21);
        for (path, expected) in sources {
            assert_eq!(*expected, hash(&root.join(path))?);
        }
        let manifest = root.join(format!(
            "benches/selective_read/runners/{reader}/Cargo.toml"
        ));
        let manifest = manifest.display();
        let build = format!(
            "+1.98.1 build --release --locked --target x86_64-unknown-linux-gnu -j8 --manifest-path {manifest}"
        );
        assert_eq!(
            record["command"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<Vec<_>>()
                .join(" "),
            format!("cargo {build}")
        );
        assert_eq!(
            fs::read_to_string(root.join("commands"))?,
            format!(
                "{build}\n+1.98.1 metadata --locked --format-version=1 --filter-platform x86_64-unknown-linux-gnu --manifest-path {manifest}\n+1.98.1 tree --locked --target x86_64-unknown-linux-gnu --manifest-path {manifest} --edges normal,build,features\n+1.98.1 --version\n"
            )
        );
    }
    Ok(())
}

#[test]
fn source_changes_and_failed_commands_cannot_produce_a_build_record() -> Result {
    for (variable, value, message) in [
        ("MOCK_MUTATE", "yes", "source files changed"),
        ("MOCK_FAIL", "build", "19"),
        ("MOCK_FAIL", "metadata", "19"),
        ("MOCK_FAIL", "tree", "19"),
    ] {
        let temporary = fixture()?;
        let result = run(temporary.path(), "dar", &[(variable, value)])?;
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains(message));
        assert!(!temporary.path().join("output/build.json").exists());
    }
    Ok(())
}

#[test]
fn stale_or_missing_release_provenance_and_existing_outputs_are_rejected() -> Result {
    for vcs in [Some("{}"), None] {
        let temporary = fixture()?;
        let root = temporary.path();
        if let Some(value) = vcs {
            write(root, "registry/.cargo_vcs_info.json", value)?;
        } else {
            fs::remove_file(root.join("registry/.cargo_vcs_info.json"))?;
        }
        let result = run(root, "delta-rs", &[])?;
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("protocol pin"));
        assert!(!root.join("output/build.json").exists());
    }
    let temporary = fixture()?;
    let root = temporary.path();
    write(root, "output/keep", "untouched")?;
    assert!(!run(root, "dar", &[])?.status.success());
    assert_eq!(fs::read_to_string(root.join("output/keep"))?, "untouched");
    assert!(!root.join("commands").exists());
    Ok(())
}
