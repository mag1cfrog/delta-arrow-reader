use super::*;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

const LINUX_TARGET: &str = "x86_64-unknown-linux-gnu";

#[test]
fn requires_a_supported_native_target_and_selects_its_filename() -> TestResult {
    let targets = [
        (LINUX_TARGET, "delta-arrow-reader"),
        ("aarch64-apple-darwin", "delta-arrow-reader"),
        ("x86_64-pc-windows-msvc", "delta-arrow-reader.exe"),
    ];
    for (target, name) in targets {
        assert_eq!(executable_name(target)?, name);
        for (host, _) in targets {
            assert_eq!(check_native_target(target, host).is_ok(), target == host);
        }
    }
    for target in [
        "",
        "x86_64-unknown-linux-musl",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "x86_64-pc-windows-gnu",
    ] {
        assert!(executable_name(target).is_err());
        assert!(check_native_target(target, target).is_err());
    }
    Ok(())
}

#[test]
fn selects_one_binary_from_cargo_output_without_guessing_cache_paths() -> TestResult {
    let artifact = serde_json::json!({
        "reason": "compiler-artifact",
        "target": {"name": "dar", "kind": ["bin"]},
        "executable": "/cache with spaces/custom-target/dar",
        "fresh": true
    })
    .to_string();
    let other = r#"{"reason":"compiler-artifact","target":{"name":"dependency","kind":["lib"]},"executable":null}"#;
    let finished = r#"{"reason":"build-finished","success":true}"#;
    assert_eq!(
        built_executable(&format!("{other}\n{artifact}\n{finished}\n"))?,
        PathBuf::from("/cache with spaces/custom-target/dar")
    );
    for output in [
        String::new(),
        other.into(),
        finished.into(),
        artifact.replace("[\"bin\"]", "[\"example\"]"),
        artifact.replace("\"dar\"", "\"other-command\""),
        format!("{artifact}\n{artifact}\n"),
        format!("{artifact}\ninvalid JSON\n"),
    ] {
        assert!(built_executable(&output).is_err(), "accepted {output}");
    }
    Ok(())
}

#[test]
fn failed_builds_and_wrong_targets_remove_only_reserved_outputs() -> TestResult {
    let temporary = tempfile::Builder::new().prefix("wheel data ").tempdir()?;
    let destination = temporary.path();
    let scripts = destination.join("scripts");
    fs::create_dir(&scripts)?;
    let unrelated = scripts.join("another-command");
    fs::write(&unrelated, b"keep this")?;
    for target in [LINUX_TARGET, "aarch64-apple-darwin", "unsupported"] {
        for name in EXECUTABLE_NAMES {
            fs::write(scripts.join(name), b"old executable")?;
        }
        let result = stage_cli(destination, target, || {
            check_native_target(target, LINUX_TARGET)?;
            InvalidInputSnafu {
                message: "build failed",
            }
            .fail()
        });
        assert!(result.is_err());
        for name in EXECUTABLE_NAMES {
            assert!(!scripts.join(name).exists());
        }
        assert_eq!(fs::read(&unrelated)?, b"keep this");
    }
    Ok(())
}

#[test]
fn copy_failures_leave_no_payload_and_never_delete_directories() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path();
    let scripts = destination.join("scripts");
    assert!(
        stage_cli(destination, LINUX_TARGET, || {
            Ok((destination.join("missing binary"), "1.2.3".into()))
        })
        .is_err()
    );
    assert_eq!(fs::read_dir(destination)?.count(), 1);
    assert_eq!(fs::read_dir(&scripts)?.count(), 0);

    let reserved_directory = scripts.join("delta-arrow-reader");
    fs::create_dir(&reserved_directory)?;
    let unrelated = reserved_directory.join("keep.txt");
    fs::write(&unrelated, b"keep this")?;
    assert!(
        stage_cli(destination, LINUX_TARGET, || panic!(
            "must fail before build"
        ))
        .is_err()
    );
    assert_eq!(fs::read(&unrelated)?, b"keep this");
    Ok(())
}

#[cfg(unix)]
#[test]
fn rejects_directory_symlinks_without_following_them() -> TestResult {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir()?;
    let outside = temporary.path().join("outside");
    fs::create_dir(&outside)?;
    let unrelated = outside.join("delta-arrow-reader");
    fs::write(&unrelated, b"keep this")?;
    let destination = temporary.path().join("wheel data");
    symlink(&outside, &destination)?;
    for path in [&destination, &destination.join(""), &destination.join(".")] {
        assert!(stage_cli(path, LINUX_TARGET, || panic!("must reject symlink")).is_err());
    }
    fs::remove_file(&destination)?;
    fs::create_dir(&destination)?;
    symlink(&outside, destination.join("scripts"))?;
    assert!(stage_cli(&destination, LINUX_TARGET, || panic!("must reject symlink")).is_err());
    assert_eq!(fs::read(unrelated)?, b"keep this");
    Ok(())
}

#[cfg(unix)]
#[test]
fn stages_only_a_verified_executable_and_preserves_its_permissions() -> TestResult {
    use std::os::unix::fs::{PermissionsExt, symlink};

    const CHILD_PROCESS_ENV: &str = "DAR_TEST_CLI_VERIFICATION_CHILD";
    if env::var_os(CHILD_PROCESS_ENV).is_none() {
        // Isolate executable writes from forks started by parallel tests.
        let test_thread = std::thread::current();
        let output = output_with_timeout(
            Command::new(env::current_exe()?)
                .arg("--exact")
                .arg(test_thread.name().expect("test thread must be named"))
                .env(CHILD_PROCESS_ENV, "1"),
            Duration::from_secs(90),
        )?;
        assert!(output.status.success(), "{output:?}");
        return Ok(());
    }

    let temporary = tempfile::Builder::new().prefix("CLI staging ").tempdir()?;
    let destination = temporary.path().join("wheel data");
    let scripts = destination.join("scripts");
    fs::create_dir_all(&scripts)?;
    let built = temporary.path().join("built executable");
    fs::write(&built, "#!/bin/sh\nprintf 'delta-arrow-reader 1.2.3\\n'\n")?;
    fs::set_permissions(&built, fs::Permissions::from_mode(0o751))?;
    // Replacing a reserved file symlink must leave its target untouched.
    symlink(&built, scripts.join("delta-arrow-reader"))?;
    fs::write(scripts.join("delta-arrow-reader.exe"), b"old Windows build")?;
    let executable = stage_cli(&destination, LINUX_TARGET, || {
        Ok((built.clone(), "1.2.3".into()))
    })?;
    assert_eq!(executable, scripts.join("delta-arrow-reader"));
    assert_eq!(fs::read(&executable)?, fs::read(&built)?);
    assert_eq!(
        fs::metadata(&executable)?.permissions().mode() & 0o777,
        0o751
    );
    assert!(fs::symlink_metadata(&executable)?.is_file());
    assert_eq!(fs::read_dir(&scripts)?.count(), 1);
    assert_eq!(fs::read_dir(&destination)?.count(), 1);

    for body in [
        "printf 'delta-arrow-reader 1.2.2\\n'",
        "printf 'dar 1.2.3\\n'",
        "printf 'delta-arrow-reader 1.2.3\\n'; printf 'warning\\n' >&2",
        "printf 'delta-arrow-reader 1.2.3\\n'; exit 1",
    ] {
        fs::write(&built, format!("#!/bin/sh\n{body}\n"))?;
        fs::write(&executable, b"stale payload")?;
        assert!(
            stage_cli(&destination, LINUX_TARGET, || {
                Ok((built.clone(), "1.2.3".into()))
            })
            .is_err(),
            "accepted {body}"
        );
        assert!(!executable.exists());
        assert_eq!(fs::read_dir(&destination)?.count(), 1);
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn interrupted_staging_can_be_retried_without_polluting_wheel_data() -> TestResult {
    use std::os::unix::{fs::PermissionsExt, process::ExitStatusExt};

    const CHILD_BINARY: &str = "DAR_TEST_INTERRUPTED_STAGING_BINARY";
    if let Some(built) = env::var_os(CHILD_BINARY) {
        stage_cli(Path::new("."), LINUX_TARGET, || {
            Ok((built.into(), "1.2.3".into()))
        })?;
        return Ok(());
    }

    let temporary = tempfile::Builder::new()
        .prefix("interrupted CLI ")
        .tempdir()?;
    let destination = temporary.path().join("wheel data");
    fs::create_dir(&destination)?;
    let built = temporary.path().join("built executable");
    // Kill only the child test process while it is verifying the staged executable.
    fs::write(&built, "#!/bin/sh\nkill -KILL \"$PPID\"\n")?;
    fs::set_permissions(&built, fs::Permissions::from_mode(0o755))?;
    let mut child = Command::new(env::current_exe()?);
    child
        .args([
            "--exact",
            "python_cli::tests::interrupted_staging_can_be_retried_without_polluting_wheel_data",
        ])
        .env(CHILD_BINARY, &built)
        .current_dir(&destination);
    let interrupted = output_with_timeout(&mut child, Duration::from_secs(30))?;
    assert_eq!(interrupted.status.signal(), Some(9), "{interrupted:?}");
    let executable = destination.join("scripts/delta-arrow-reader");
    assert!(!executable.exists());

    fs::write(&built, "#!/bin/sh\nprintf 'delta-arrow-reader 1.2.3\\n'\n")?;
    let retried = output_with_timeout(&mut child, Duration::from_secs(30))?;
    assert!(retried.status.success(), "{retried:?}");
    assert_eq!(fs::read(&executable)?, fs::read(&built)?);
    assert_eq!(fs::read_dir(&destination)?.count(), 1);
    assert_eq!(fs::read_dir(destination.join("scripts"))?.count(), 1);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "builds the native CLI; run explicitly with the workspace Rust toolchain"]
fn native_cli_stages_and_reads_a_fixture_outside_the_checkout() -> TestResult {
    use crate::scan::ipc::{IpcTable, check_scan_output};

    let temporary = tempfile::Builder::new().prefix("native CLI ").tempdir()?;
    let destination = temporary.path().join("wheel data");
    let repository = repository_root();
    let manifest: WorkspaceManifest = read_toml(&repository.join("Cargo.toml"))?;
    StagePythonCliArgs {
        destination: destination.clone(),
        target: LINUX_TARGET.into(),
        profile: "dev".into(),
    }
    .run()?;
    let executable = destination.join("scripts/delta-arrow-reader");
    let output = output_with_timeout(
        Command::new(&executable)
            .arg("--version")
            .current_dir(temporary.path()),
        Duration::from_secs(15),
    )?;
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        output.stdout,
        format!(
            "delta-arrow-reader {}\n",
            manifest.workspace.package.version
        )
        .as_bytes()
    );

    let fixture = repository.join(
        "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned",
    );
    let output = output_with_timeout(
        Command::new(&executable)
            .arg("inspect")
            .arg(fixture.join("table"))
            .current_dir(temporary.path()),
        Duration::from_secs(30),
    )?;
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let inspection: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(inspection["table_version"], "0");
    let expected = IpcTable::read_file(&fixture.join("expected.arrow"))?;
    let fields = inspection["schema"]["fields"].as_array().unwrap();
    assert_eq!(fields.len(), expected.schema.fields().len());
    for (actual, expected) in fields.iter().zip(expected.schema.fields()) {
        assert_eq!(actual["name"], expected.name().as_str());
    }

    let output = output_with_timeout(
        Command::new(executable)
            .arg("scan")
            .arg(fixture.join("table"))
            .current_dir(temporary.path()),
        Duration::from_secs(30),
    )?;
    check_scan_output(&output)?.check_matches(&expected, None)?;
    Ok(())
}
