//! CLI artifact manifests, packaging, and invalid build identity checks.

#![cfg(unix)]

use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

use serde_json::Value;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn artifact_command(task: &str, directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command
        .args(["cli", "artifacts", task])
        .arg(directory)
        .args([
            "--version",
            "1.2.3",
            "--source-commit",
            &"a".repeat(40),
            "--rust-toolchain",
            "1.94.0",
        ]);
    command
}

fn manifest(output: Output) -> TestResult<Value> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[test]
fn commands_return_verified_manifests_and_preserve_packaged_bytes() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let inputs = root.join("inputs");
    let notices = root.join("notices.txt");
    fs::write(&notices, "Test redistribution terms\n")?;
    for architecture in ["amd64", "arm64"] {
        let binary = root.join(format!("dar-{architecture}"));
        fs::write(&binary, format!("test {architecture} binary\n"))?;
        let directory = inputs.join(architecture);
        let packaged = manifest(
            artifact_command("package", &directory)
                .arg("--binary")
                .arg(&binary)
                .arg("--dependency-notices")
                .arg(&notices)
                .args([
                    "--architecture",
                    architecture,
                    "--runtime-dependency",
                    "libc.so.6",
                ])
                .output()?,
        )?;
        assert_eq!(packaged["targets"][0]["architecture"], architecture);
        assert_eq!(
            packaged,
            serde_json::from_slice::<Value>(&fs::read(directory.join("cli-artifacts.json"))?)?
        );
        let verified = manifest(
            artifact_command("verify", &directory)
                .args(["--architecture", architecture])
                .output()?,
        )?;
        assert_eq!(verified, packaged);
        let destination = root.join(format!("extracted-{architecture}"));
        let extracted = manifest(
            artifact_command("extract", &directory)
                .args(["--architecture", architecture])
                .arg("--destination")
                .arg(&destination)
                .output()?,
        )?;
        assert_eq!(extracted, packaged);
        assert_eq!(fs::read(destination.join("dar"))?, fs::read(&binary)?);
    }
    let complete = root.join("complete");
    let assembled = manifest(
        artifact_command("assemble", &complete)
            .arg("--inputs")
            .arg(&inputs)
            .output()?,
    )?;
    assert_eq!(
        assembled["targets"]
            .as_array()
            .ok_or("missing targets")?
            .len(),
        2
    );
    assert_eq!(
        manifest(artifact_command("verify", &complete).output()?)?,
        assembled
    );
    assert!(
        !artifact_command("verify", &complete)
            .args(["--architecture", "amd64"])
            .output()?
            .status
            .success()
    );
    Ok(())
}

#[test]
fn invalid_identity_fails_before_touching_artifacts() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let absent = temporary.path().join("missing");
    for (version, source, toolchain) in [
        ("../1.2.3", "a".repeat(40), "1.94.0"),
        ("1.2.3", "main".into(), "1.94.0"),
        ("1.2.3", "a".repeat(40), "stable"),
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args(["cli", "artifacts", "verify"])
            .arg(&absent)
            .args([
                "--version",
                version,
                "--source-commit",
                &source,
                "--rust-toolchain",
                toolchain,
            ])
            .output()?;
        assert_eq!(result.status.code(), Some(1));
        assert!(result.stdout.is_empty());
        assert!(String::from_utf8(result.stderr)?.contains("invalid"));
        assert!(!absent.exists());
    }
    Ok(())
}
