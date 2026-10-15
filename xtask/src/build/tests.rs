use super::*;
use std::collections::BTreeMap;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn git(repository: &Path, arguments: &[&str]) -> Result<String> {
    command_output(Command::new("git").current_dir(repository).args(arguments))
}

fn commit_fixture(repository: &Path) -> Result<String> {
    git(repository, &["add", "."])?;
    git(
        repository,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--quiet",
            "-m",
            "test: source fixture",
        ],
    )?;
    Ok(git(repository, &["rev-parse", "HEAD"])?.trim().into())
}

#[test]
fn source_must_be_the_exact_clean_commit() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let repository = temporary.path();
    git(repository, &["init", "--quiet"])?;
    fs::write(repository.join(".gitignore"), "/target/\n")?;
    let source = repository.join("source.rs");
    fs::write(&source, "original\n")?;
    let commit = commit_fixture(repository)?;
    check_source(repository, &commit)?;
    for incorrect in ["main", &commit[..12], &"f".repeat(40)] {
        assert!(check_source(repository, incorrect).is_err());
    }
    fs::write(&source, "modified\n")?;
    assert!(check_source(repository, &commit).is_err());
    fs::write(&source, "original\n")?;
    let extra = repository.join("untracked.rs");
    fs::write(&extra, "")?;
    assert!(check_source(repository, &commit).is_err());
    fs::remove_file(extra)?;
    fs::create_dir(repository.join("target"))?;
    fs::write(repository.join("target/build-info.json"), "{}")?;
    check_source(repository, &commit)?;
    Ok(())
}

#[test]
fn compiler_release_requires_the_declared_numeric_version() -> TestResult {
    let output = "rustc 1.94.1\nhost: x86_64-unknown-linux-gnu\nrelease: 1.94.1\n";
    assert_eq!(compiler_release(output, "1.94")?, "1.94.1");
    assert_eq!(compiler_release(output, "1.94.1")?, "1.94.1");
    assert_eq!(compiler_field(output, "host")?, "x86_64-unknown-linux-gnu");
    for declared in ["1.94.2", "1.95", "stable", "1", "1.94.1.0", "1..94"] {
        assert!(compiler_release(output, declared).is_err());
    }
    assert!(compiler_release("rustc 1.94.1", "1.94").is_err());
    Ok(())
}

#[test]
#[ignore = "requires Debian 12 and the exact Rust release installed through rustup"]
fn native_build_preserves_metadata_and_rejects_source_changes() -> TestResult {
    let compiler = command_output(Command::new("rustc").arg("-vV"))?;
    let config: WorkspaceManifest = read_toml(&repository_root().join("Cargo.toml"))?;
    let release = compiler_release(&compiler, &config.workspace.package.rust_version)?;
    let temporary = tempfile::tempdir()?;
    let repository = temporary.path();
    git(repository, &["init", "--quiet"])?;
    fs::write(repository.join(".gitignore"), "/target/\n")?;
    fs::write(
        repository.join("Cargo.toml"),
        r#"
[package]
name = "delta-arrow-reader-cli"
version.workspace = true
edition = "2024"
rust-version.workspace = true
[[bin]]
name = "dar"
path = "main.rs"
[workspace]
[workspace.package]
rust-version = "1.94"
version = "1.2.3"
[workspace.metadata.cli-release]
cargo-about-version = "0.9.2"
"#
        .replace(
            "rust-version = \"1.94\"",
            &format!("rust-version = \"{release}\""),
        ),
    )?;
    fs::write(
        repository.join("main.rs"),
        "fn main() { println!(\"dar 1.2.3\"); }\n",
    )?;
    fs::write(repository.join("build.rs"), "fn main() {}\n")?;
    run_command(
        Command::new("cargo")
            .args(["generate-lockfile", "--offline"])
            .current_dir(repository),
    )?;
    let architecture = match compiler_field(&compiler, "host")? {
        "x86_64-unknown-linux-gnu" => Architecture::Amd64,
        "aarch64-unknown-linux-gnu" => Architecture::Arm64,
        other => return Err(format!("unsupported test host {other}").into()),
    };
    let mut args = BuildArgs {
        source_commit: commit_fixture(repository)?,
        architecture,
        target_dir: repository.join("target"),
        rust_toolchain: Some(release.clone()),
    };
    let prepared = PrepareArgs {
        source_commit: args.source_commit.clone(),
        expected_version: Some("1.2.3".into()),
        rust_toolchain: Some(release.clone()),
    }
    .resolve(repository)?;
    assert_eq!(prepared["version"], "1.2.3");
    assert_eq!(prepared["source-commit"], args.source_commit);
    assert_eq!(prepared["rust-toolchain"], release);
    assert_eq!(prepared["about-version"], "0.9.2");
    assert_eq!(prepared["image"], DEBIAN_IMAGE);
    assert!(
        PrepareArgs {
            source_commit: args.source_commit.clone(),
            expected_version: Some("1.2.4".into()),
            rust_toolchain: Some(release.clone()),
        }
        .resolve(repository)
        .is_err()
    );
    let metadata = build_binary(repository, &args)?;
    assert_eq!(metadata.schema_version, 1);
    assert_eq!(metadata.identity.version, "1.2.3");
    assert_eq!(metadata.identity.source_commit, args.source_commit);
    assert_eq!(
        Some(&metadata.identity.rust_toolchain),
        args.rust_toolchain.as_ref()
    );
    assert_eq!(metadata.runtime_packages, RUNTIME_PACKAGES);
    let metadata_file = args
        .target_dir
        .join(architecture.rust_target())
        .join("build-info.json");
    let recorded: serde_json::Value = serde_json::from_slice(&fs::read(&metadata_file)?)?;
    assert_eq!(recorded, serde_json::to_value(&metadata)?);

    for mismatch in ["version", "rust_toolchain"] {
        let mut identity = metadata.identity.clone();
        if mismatch == "version" {
            identity.version = "1.2.4".into();
        } else {
            identity.rust_toolchain = "0.0.0".into();
        }
        let output = repository.join("target").join(mismatch);
        let result = BuildArchiveArgs {
            identity,
            architecture,
            target_dir: args.target_dir.clone(),
            output: output.clone(),
        }
        .build(repository);
        assert!(result.is_err());
        assert!(!output.exists());
    }
    let failed_notices = repository.join("target/failed-notices");
    let result = BuildArchiveArgs {
        identity: metadata.identity.clone(),
        architecture,
        target_dir: args.target_dir.clone(),
        output: failed_notices.clone(),
    }
    .build(repository);
    assert!(result.is_err());
    assert!(!failed_notices.exists());

    let binary = args
        .target_dir
        .join(architecture.rust_target())
        .join("release/dar");
    let original = fs::read(&binary)?;
    let notices = repository.join("target/notices.txt");
    fs::write(&notices, "Fixture dependency license and copyright.\n")?;
    package_validated_build(
        &binary,
        &notices,
        &repository.join("target/valid-package"),
        architecture,
        &metadata,
    )?;
    fs::write(&binary, "replacement binary")?;
    let error = package_validated_build(
        &binary,
        &notices,
        &repository.join("target/replaced-package"),
        architecture,
        &metadata,
    )
    .err()
    .ok_or("replaced binary was accepted")?;
    assert!(
        error
            .to_string()
            .contains("differs from the validated build"),
        "{error}"
    );
    fs::write(&binary, original)?;

    fs::write(
        repository.join("build.rs"),
        "fn main() { std::fs::write(\"untracked\", \"changed during build\").unwrap(); }\n",
    )?;
    args.source_commit = commit_fixture(repository)?;
    let error = build_binary(repository, &args)
        .err()
        .ok_or("changed source was accepted")?;
    assert!(
        error.to_string().contains("modified or untracked"),
        "{error}"
    );
    assert!(
        !metadata_file.exists(),
        "stale successful metadata must be removed"
    );
    Ok(())
}

#[test]
fn child_commands_use_build_toolchain_and_baseline_flags() {
    for key in [
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS",
        "CFLAGS_aarch64_unknown_linux_gnu",
        "RUSTC",
        "RUSTC_WRAPPER",
        "CARGO_PROFILE_RELEASE_OPT_LEVEL",
        "CARGO_BUILD_TARGET",
        "RUSTUP_TOOLCHAIN",
        "GITHUB_TOKEN",
    ] {
        assert!(!keep_build_variable(OsStr::new(key)), "must discard {key}");
    }
    for (architecture, cpu) in [
        (Architecture::Amd64, "x86-64"),
        (Architecture::Arm64, "generic"),
    ] {
        let command = rustup_command("1.94.1", architecture, Path::new("."));
        let environment: BTreeMap<_, _> = command.get_envs().collect();
        assert_eq!(
            environment[OsStr::new("RUSTUP_TOOLCHAIN")],
            Some(OsStr::new("1.94.1"))
        );
        assert_eq!(
            environment[OsStr::new("RUSTFLAGS")],
            Some(OsStr::new(&format!("-Ctarget-cpu={cpu}")))
        );
        assert_eq!(command.get_args().collect::<Vec<_>>(), ["run", "1.94.1"]);
    }
}
