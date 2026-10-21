//! Container image build inputs, metadata, and cleanup checks.

#![cfg(target_os = "linux")]

use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[path = "images/artifacts.rs"]
mod artifacts;

struct ImageFixture {
    root: tempfile::TempDir,
    artifacts: PathBuf,
    binary: PathBuf,
    architecture: &'static str,
}

fn assert_success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

impl ImageFixture {
    fn new(with_libgcc: bool) -> TestResult<Self> {
        let root = tempfile::tempdir()?;
        let binary = root.path().join("dar");
        let source = root.path().join("fixture.c");
        fs::write(&source, "int main(void) { return 0; }\n")?;
        let mut compiler = Command::new("cc");
        compiler.arg(&source).arg("-o").arg(&binary);
        if with_libgcc {
            compiler.args(["-Wl,--no-as-needed", "-lgcc_s"]);
        }
        assert_success(compiler.output()?);
        // Match the existing ELF fixtures by removing distro-specific compiler notes.
        assert_success(
            Command::new("objcopy")
                .args([
                    "--remove-section=.gnu.build.attributes",
                    "--remove-section=.note.gnu.property",
                ])
                .arg(&binary)
                .output()?,
        );
        let architecture = match env::consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            other => return Err(format!("unsupported test architecture: {other}").into()),
        };
        let fixture = Self {
            artifacts: root.path().join("artifacts"),
            root,
            binary,
            architecture,
        };
        let notices = fixture.root.path().join("notices.txt");
        fs::write(&notices, "Fixture dependency redistribution terms\n")?;
        let inputs = fixture.root.path().join("inputs");
        for platform in ["amd64", "arm64"] {
            let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
            command
                .args(["cli", "artifacts", "package"])
                .arg(inputs.join(platform))
                .arg("--binary")
                .arg(&fixture.binary)
                .arg("--dependency-notices")
                .arg(&notices)
                .args([
                    "--architecture",
                    platform,
                    "--runtime-dependency",
                    "libc.so.6",
                ]);
            if with_libgcc {
                command.args(["--runtime-dependency", "libgcc_s.so.1"]);
            }
            assert_success(Self::with_identity(command).output()?);
        }
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        command
            .args(["cli", "artifacts", "assemble"])
            .arg(&fixture.artifacts)
            .arg("--inputs")
            .arg(&inputs);
        assert_success(Self::with_identity(command).output()?);

        let tools = fixture.root.path().join("tools");
        fs::create_dir(&tools)?;
        let docker = tools.join("docker");
        fs::write(
            &docker,
            "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$@\" > \"$IMAGE_TEST_ROOT/arguments\"\nfor argument in \"$@\"; do context=\"$argument\"; done\nprintf '%s' \"$context\" > \"$IMAGE_TEST_ROOT/context-path\"\nmkdir \"$IMAGE_TEST_ROOT/context-copy\"\ncp -a \"$context/.\" \"$IMAGE_TEST_ROOT/context-copy/\"\nprintf 'container build output\\n'\nexit \"${IMAGE_TEST_EXIT_CODE:-0}\"\n",
        )?;
        fs::set_permissions(&docker, fs::Permissions::from_mode(0o755))?;
        Ok(fixture)
    }

    fn with_identity(mut command: Command) -> Command {
        command.args([
            "--version",
            "1.2.3",
            "--source-commit",
            &"a".repeat(40),
            "--rust-toolchain",
            "1.94.0",
        ]);
        command
    }

    fn build_command(&self) -> TestResult<Command> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        command
            .args(["cli", "build-image"])
            .arg(&self.artifacts)
            .args(["--architecture", self.architecture, "--tag", "dar:fixture"])
            .env("IMAGE_TEST_ROOT", self.root.path());
        let mut paths = vec![self.root.path().join("tools")];
        paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
        command.env("PATH", env::join_paths(paths)?);
        Ok(Self::with_identity(command))
    }

    fn assert_context_removed(&self) -> TestResult {
        let context = PathBuf::from(fs::read_to_string(self.root.path().join("context-path"))?);
        assert!(
            !context
                .parent()
                .ok_or("missing temporary directory")?
                .exists()
        );
        Ok(())
    }
}

#[test]
fn image_build_preserves_validated_files_and_uses_only_required_packages() -> TestResult {
    for with_libgcc in [false, true] {
        let fixture = ImageFixture::new(with_libgcc)?;
        let output = assert_success(fixture.build_command()?.output()?);
        assert_eq!(output.stdout, b"Built CLI image dar:fixture\n");
        assert_eq!(output.stderr, b"container build output\n");
        let context = fixture.root.path().join("context-copy");
        let mut files = fs::read_dir(&context)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        files.sort();
        assert_eq!(
            files,
            [
                "Dockerfile",
                "LICENSE",
                "NOTICE",
                "dar",
                "runtime-packages.txt"
            ]
        );
        assert_eq!(fs::read(context.join("dar"))?, fs::read(&fixture.binary)?);
        assert_eq!(
            fs::read(context.join("LICENSE"))?,
            include_bytes!("../../LICENSE")
        );
        assert!(
            fs::read_to_string(context.join("NOTICE"))?
                .ends_with("Fixture dependency redistribution terms\n")
        );
        for (name, mode) in [("dar", 0o755), ("LICENSE", 0o644), ("NOTICE", 0o644)] {
            assert_eq!(
                fs::metadata(context.join(name))?.permissions().mode() & 0o7777,
                mode
            );
        }
        assert_eq!(
            fs::read_to_string(context.join("runtime-packages.txt"))?,
            if with_libgcc {
                "ca-certificates\nlibc6\nlibgcc-s1\n"
            } else {
                "ca-certificates\nlibc6\n"
            }
        );
        let arguments = fs::read_to_string(fixture.root.path().join("arguments"))?;
        let arguments: Vec<_> = arguments.lines().collect();
        assert_eq!(
            &arguments[..5],
            [
                "build",
                "--platform",
                &format!("linux/{}", fixture.architecture),
                "--tag",
                "dar:fixture"
            ]
        );
        assert_eq!(
            &arguments[5..9],
            [
                "--build-arg",
                "CLI_VERSION=1.2.3",
                "--build-arg",
                &format!("CLI_SOURCE_COMMIT={}", "a".repeat(40))
            ]
        );
        assert_eq!(arguments.len(), 10);
        fixture.assert_context_removed()?;
    }
    Ok(())
}

#[test]
fn incomplete_or_changed_artifacts_never_start_docker() -> TestResult {
    for case in [
        "missing-architecture",
        "checksums",
        "archive",
        "source",
        "version",
        "elf-dependencies",
    ] {
        let fixture = ImageFixture::new(false)?;
        let mut command = fixture.build_command()?;
        match case {
            "missing-architecture" => {
                let other = if fixture.architecture == "amd64" {
                    "arm64"
                } else {
                    "amd64"
                };
                fs::remove_file(
                    fixture
                        .artifacts
                        .join(format!("dar-v1.2.3-linux-{other}-gnu.tar.gz")),
                )?;
            }
            "checksums" => fs::write(fixture.artifacts.join("SHA256SUMS"), "wrong\n")?,
            "archive" => fs::write(
                fixture.artifacts.join("dar-v1.2.3-linux-amd64-gnu.tar.gz"),
                "broken",
            )?,
            "source" | "version" => {
                let path = fixture.artifacts.join("cli-artifacts.json");
                let mut manifest: Value = serde_json::from_slice(&fs::read(&path)?)?;
                if case == "source" {
                    manifest["source_commit"] = "b".repeat(40).into();
                } else {
                    manifest["version"] = "1.2.4".into();
                }
                fs::write(path, serde_json::to_vec(&manifest)?)?;
            }
            "elf-dependencies" => {
                let path = fixture.artifacts.join("cli-artifacts.json");
                let mut manifest: Value = serde_json::from_slice(&fs::read(&path)?)?;
                for target in manifest["targets"]
                    .as_array_mut()
                    .ok_or("missing targets")?
                {
                    target["runtime_dependencies"] =
                        serde_json::json!(["libc.so.6", "libgcc_s.so.1"]);
                }
                fs::write(path, serde_json::to_vec(&manifest)?)?;
                let mut checksums = String::new();
                for name in [
                    "dar-v1.2.3-linux-amd64-gnu.tar.gz",
                    "dar-v1.2.3-linux-arm64-gnu.tar.gz",
                    "cli-artifacts.json",
                ] {
                    checksums.push_str(&format!(
                        "{:x}  {name}\n",
                        Sha256::digest(fs::read(fixture.artifacts.join(name))?)
                    ));
                }
                fs::write(fixture.artifacts.join("SHA256SUMS"), checksums)?;
            }
            _ => unreachable!(),
        }
        let output = command.output()?;
        assert_eq!(output.status.code(), Some(1), "{case}: {output:?}");
        assert!(output.stdout.is_empty());
        if case == "elf-dependencies" {
            assert!(String::from_utf8_lossy(&output.stderr).contains("ELF metadata differs"));
        } else if matches!(case, "source" | "version") {
            assert!(
                String::from_utf8_lossy(&output.stderr).contains("manifest build identity differs")
            );
        }
        assert!(!fixture.root.path().join("arguments").exists(), "{case}");
    }
    Ok(())
}

#[test]
fn docker_failure_is_reported_and_temporary_files_are_removed() -> TestResult {
    let fixture = ImageFixture::new(false)?;
    let output = fixture
        .build_command()?
        .env("IMAGE_TEST_EXIT_CODE", "17")
        .output()?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("docker failed"));
    fixture.assert_context_removed()?;
    Ok(())
}
