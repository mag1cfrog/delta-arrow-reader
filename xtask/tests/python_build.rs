#![cfg(unix)]

use std::{env, fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

use serde_json::json;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn executable(path: &Path, script: &str) -> Result {
    fs::write(path, format!("#!/bin/sh\nset -eu\n{script}"))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    Ok(())
}

#[test]
fn wheel_build_stops_at_each_failure_and_cleans_stale_payloads() -> Result {
    let fixture = tempfile::Builder::new()
        .prefix("Python wheel build ")
        .tempdir()?;
    let root = fixture.path();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let workspace: toml::Value =
        toml::from_str(&fs::read_to_string(repository.join("Cargo.toml"))?)?;
    let version = workspace["workspace"]["package"]["version"]
        .as_str()
        .unwrap();
    let rust_version = workspace["workspace"]["package"]["rust-version"]
        .as_str()
        .unwrap();
    let about_version = workspace["workspace"]["metadata"]["cli-release"]["cargo-about-version"]
        .as_str()
        .unwrap();
    let manifest = repository.join("crates/delta-arrow-reader-python/Cargo.toml");
    fs::write(
        root.join("pyproject.toml"),
        format!(
            "[tool.maturin]\nmanifest-path = {manifest:?}\nprofile = 'release'\neditable-profile = 'dev'\n"
        ),
    )?;
    fs::create_dir(root.join("bin"))?;
    executable(
        &root.join("bin/rustc"),
        &format!(
            "if [ \"$1\" = '-vV' ]; then\n  printf 'release: {rust_version}.0\\nhost: x86_64-unknown-linux-gnu\\n'\nelse\n  printf '%s/sysroot\\n' \"$TEST_ROOT\"\nfi\n"
        ),
    )?;
    executable(
        &root.join("native CLI"),
        &format!("printf 'delta-arrow-reader {version}\\n'\n"),
    )?;
    fs::write(
        root.join("cargo-artifact.json"),
        json!({
            "reason": "compiler-artifact", "target": {"name": "dar", "kind": ["bin"]},
            "executable": root.join("native CLI"),
        })
        .to_string()
            + "\n",
    )?;
    let package = json!({"id": "local", "name": "delta-arrow-reader", "version": version});
    let mut resolved = package.clone();
    resolved["manifest_path"] = json!(manifest);
    resolved["source"] = json!(null);
    resolved["license_file"] = json!(null);
    fs::write(root.join("licenses.json"), json!({
        "crates": [{"package": resolved}],
        "licenses": [{"id": "Apache-2.0", "text": "test license", "used_by": [{"crate": package}]}],
    }).to_string())?;
    let docs = root.join("sysroot/share/doc/rust");
    fs::create_dir_all(docs.join("licenses"))?;
    for file in [
        "COPYRIGHT-library.html",
        "licenses/Apache-2.0.txt",
        "licenses/MIT.txt",
    ] {
        fs::write(docs.join(file), "test license")?;
    }
    executable(
        &root.join("bin/cargo"),
        &format!(
            r#"
case "$1" in
  locate-project) printf '%s/Cargo.toml\n' "$TEST_WORKSPACE" ;;
  build)
    [ "$FAIL_STAGE" != build ] || exit 41
    printf '%s\n' "$@" > "$TEST_ROOT/cargo-arguments"
    cat "$TEST_ROOT/cargo-artifact.json" ;;
  about)
    [ "$CARGO_NET_OFFLINE" = "$EXPECTED_CARGO_NET_OFFLINE" ] || exit 44
    if [ "$2" = --version ]; then
      printf 'cargo-about {about_version}\n'
    else
      [ "$FAIL_STAGE" != notices ] || exit 42
      cat "$TEST_ROOT/licenses.json"
    fi ;;
  *) exit 99 ;;
esac
"#
        ),
    )?;
    executable(
        &root.join("bin/maturin"),
        r#"
printf '%s\n' "$@" > "$TEST_ROOT/maturin-arguments"
test -x delta_arrow_reader.data/scripts/delta-arrow-reader
test -f delta_arrow_reader.data/data/share/delta-arrow-reader/python-THIRD_PARTY_NOTICES.txt
test -f delta_arrow_reader.data/data/share/delta-arrow-reader/cli-THIRD_PARTY_NOTICES.txt
[ "$FAIL_STAGE" != maturin ] || exit 43
printf 'test wheel' > "$TEST_ROOT/assembled.whl"
printf '%s/assembled.whl\n' "$TEST_ROOT"
"#,
    )?;
    let search_path = env::join_paths(
        [root.join("bin")]
            .into_iter()
            .chain(env::split_paths(&env::var_os("PATH").unwrap_or_default())),
    )?;

    for (failure, network_flag, inherited_offline) in [
        ("build", None, "false"),
        ("notices", None, "false"),
        ("maturin", None, "false"),
        ("none", None, "false"),
        ("none", Some("--offline"), "false"),
        ("none", Some("--frozen"), "false"),
        ("none", None, "true"),
    ] {
        let data = root.join("delta_arrow_reader.data");
        fs::create_dir_all(data.join("scripts"))?;
        fs::write(
            data.join("scripts/delta-arrow-reader.exe"),
            "stale wrong platform CLI",
        )?;
        let output_directory = root.join(format!("wheel output {failure}"));
        let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args([
                "build-python-wheel",
                "--editable",
                "--profile",
                "release",
                "--target-dir",
                "build outputs",
            ])
            .args(network_flag)
            .arg("--project-directory")
            .arg(root)
            .arg("--wheel-directory")
            .arg(&output_directory)
            .env("PATH", &search_path)
            .env("CARGO", root.join("bin/cargo"))
            .env("RUSTC", root.join("bin/rustc"))
            .env("TEST_ROOT", root)
            .env("TEST_WORKSPACE", repository)
            .env("FAIL_STAGE", failure)
            .env("CARGO_NET_OFFLINE", inherited_offline)
            .env(
                "EXPECTED_CARGO_NET_OFFLINE",
                network_flag.map_or(inherited_offline, |_| "true"),
            )
            .env_remove("CARGO_BUILD_TARGET")
            .output()?;
        assert_eq!(
            output.status.success(),
            failure == "none",
            "{failure}, {network_flag:?}, {inherited_offline}: {output:?}"
        );
        assert!(!data.exists(), "{failure} retained installable data");
        assert_eq!(
            output_directory.join("assembled.whl").exists(),
            failure == "none"
        );
        assert_eq!(
            root.join("maturin-arguments").exists(),
            matches!(failure, "maturin" | "none")
        );
        assert!(!fs::read_dir(root)?.any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".python-wheel-")
        }));
        if failure == "none" {
            let cargo = fs::read_to_string(root.join("cargo-arguments"))?;
            let maturin = fs::read_to_string(root.join("maturin-arguments"))?;
            for arguments in [&cargo, &maturin] {
                assert!(arguments.contains("--profile\nrelease\n"));
                assert!(arguments.contains("--target\nx86_64-unknown-linux-gnu\n"));
                assert!(arguments.contains(&format!(
                    "--target-dir\n{}\n",
                    root.join("build outputs").display()
                )));
                for flag in ["--offline", "--frozen"] {
                    assert_eq!(
                        arguments.lines().any(|argument| argument == flag),
                        network_flag == Some(flag)
                    );
                }
            }
            assert!(maturin.contains("--editable\n"));
            assert_eq!(
                fs::read(output_directory.join("assembled.whl"))?,
                b"test wheel"
            );
        }
    }
    Ok(())
}

#[test]
fn source_archives_use_the_current_workspace_with_a_cached_xtask() -> Result {
    let fixture = tempfile::Builder::new()
        .prefix("Repacked Python source ")
        .tempdir()?;
    let root = fixture.path();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let workspace: toml::Value =
        toml::from_str(&fs::read_to_string(repository.join("Cargo.toml"))?)?;
    let version = workspace["workspace"]["package"]["version"]
        .as_str()
        .unwrap();
    fs::create_dir(root.join("bin"))?;
    executable(
        &root.join("bin/cargo"),
        "printf '%s/Cargo.toml\\n' \"$TEST_WORKSPACE\"\n",
    )?;
    // The same compiled xtask must package the active source, not its build directory.
    for (path, contents) in [
        ("Cargo.toml", fs::read(repository.join("Cargo.toml"))?),
        ("Cargo.lock", fs::read(repository.join("Cargo.lock"))?),
        ("LICENSE", b"test license".to_vec()),
        ("NOTICE", b"test notice".to_vec()),
        (".cargo/config.toml", Vec::new()),
        ("crates/delta-arrow-reader-cli/Cargo.toml", Vec::new()),
        ("crates/delta-arrow-reader-cli/Dockerfile", Vec::new()),
        ("crates/delta-arrow-reader-cli/src/main.rs", Vec::new()),
        (
            "crates/delta-arrow-reader-cli/release/about.toml",
            Vec::new(),
        ),
        ("xtask/Cargo.toml", Vec::new()),
        (
            "xtask/src/main.rs",
            b"source from the active workspace".to_vec(),
        ),
    ] {
        let destination = root.join(path);
        fs::create_dir_all(destination.parent().unwrap())?;
        fs::write(destination, contents)?;
    }
    let source = root.join("source.tar.gz");
    let destination = root.join("complete.tar.gz");
    let mut archive = tar::Builder::new(flate2::write::GzEncoder::new(
        fs::File::create(&source)?,
        flate2::Compression::default(),
    ));
    for file in [
        "Cargo.toml",
        "Cargo.lock",
        "pyproject.toml",
        "PKG-INFO",
        "build_backend.py",
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(0);
        header.set_cksum();
        archive.append_data(
            &mut header,
            format!("delta_arrow_reader-{version}/{file}"),
            std::io::empty(),
        )?;
    }
    archive.into_inner()?.finish()?;
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("complete-python-sdist")
        .arg(source)
        .arg(&destination)
        .current_dir(root)
        .env("CARGO", root.join("bin/cargo"))
        .env("TEST_WORKSPACE", root)
        .output()?;
    assert!(output.status.success(), "{output:?}");
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(fs::File::open(destination)?));
    let mut found = false;
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.path()?.ends_with("xtask/src/main.rs") {
            let mut source = String::new();
            std::io::Read::read_to_string(&mut entry, &mut source)?;
            assert_eq!(source, "source from the active workspace");
            found = true;
        }
    }
    assert!(found);
    Ok(())
}
