use std::{os::unix::fs::symlink, path::Path};

use serde_json::json;

use super::*;

fn artifact_fixture() -> TestResult<ImageFixture> {
    let fixture = ImageFixture::new(false)?;
    let root = fixture.root.path();
    let base_image = include_str!("../../../crates/delta-arrow-reader-cli/Dockerfile")
        .lines()
        .find_map(|line| line.strip_prefix("ARG BASE_IMAGE="))
        .ok_or("missing base image")?;
    for architecture in ["amd64", "arm64"] {
        fs::write(
            root.join(format!("config-{architecture}.json")),
            serde_json::to_vec(&json!({
                "os": "linux", "architecture": architecture,
                "config": {
                    "User": "65532:65532", "WorkingDir": "/work",
                    "Entrypoint": ["/usr/local/bin/dar"], "Cmd": ["--help"],
                    "Volumes": null, "ExposedPorts": null, "OnBuild": null,
                    "Env": ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],
                    "Labels": {
                        "org.opencontainers.image.source": "https://github.com/mag1cfrog/delta-arrow-reader",
                        "org.opencontainers.image.licenses": "Apache-2.0",
                        "org.opencontainers.image.revision": "a".repeat(40),
                        "org.opencontainers.image.version": "1.2.3",
                        "org.opencontainers.image.base.name": base_image
                    }
                }
            }))?,
        )?;
    }
    let manifest = serde_json::to_vec(&json!({
        "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"
    }))?;
    fs::write(root.join("oci-manifest.json"), &manifest)?;
    fs::write(
        root.join("digest"),
        format!("sha256:{:x}", Sha256::digest(&manifest)),
    )?;
    let files = root.join("image-files");
    fs::create_dir_all(files.join("usr/local/bin"))?;
    fs::create_dir_all(files.join("usr/share/doc/dar"))?;
    fs::copy(&fixture.binary, files.join("usr/local/bin/dar"))?;
    fs::write(
        files.join("usr/share/doc/dar/LICENSE"),
        include_bytes!("../../../LICENSE"),
    )?;
    fs::write(
        files.join("usr/share/doc/dar/NOTICE"),
        [
            include_bytes!("../../../NOTICE").as_slice(),
            b"\n",
            b"Fixture dependency redistribution terms\n",
        ]
        .concat(),
    )?;
    for (name, mode) in [
        ("usr/local/bin/dar", 0o755),
        ("usr/share/doc/dar/LICENSE", 0o644),
        ("usr/share/doc/dar/NOTICE", 0o644),
    ] {
        fs::set_permissions(files.join(name), fs::Permissions::from_mode(mode))?;
    }
    for (name, script) in [
        (
            "docker",
            r#"#!/bin/sh
set -eu
[ "$1" = save ] && [ "$2" = --output ] && [ "$4" = -- ]
printf '%s' "$3" > "$IMAGE_TEST_ROOT/saved-path"
printf '%s' "$5" > "$3"
[ "${IMAGE_TEST_FAIL:-}" != save ]
"#,
        ),
        (
            "skopeo",
            r#"#!/bin/sh
set -eu
case "$1:$2" in
  copy:--format)
    [ "$3" = oci ]
    destination=${5#oci-archive:}; destination=${destination%:dar}
    cp "${4#docker-archive:}" "$destination"
    [ "${IMAGE_TEST_FAIL:-}" != export ]
    ;;
  copy:--all)
    [ "$3" = --preserve-digests ] && [ "$4" = --digestfile ]
    source=${6#oci-archive:}
    case "$source" in
      *linux-amd64.oci.tar) architecture=amd64 ;;
      *linux-arm64.oci.tar) architecture=arm64 ;;
      *) exit 12 ;;
    esac
    layout=${7#oci:}; layout=${layout%:dar}
    mkdir "$layout"
    cp "$IMAGE_TEST_ROOT/config-$architecture.json" "$layout/config.json"
    cp "$IMAGE_TEST_ROOT/digest" "$5"
    printf '%s' "$layout" > "$IMAGE_TEST_ROOT/layout-path"
    [ "${IMAGE_TEST_FAIL:-}" != import ]
    ;;
  inspect:--raw) cat "$IMAGE_TEST_ROOT/oci-manifest.json" ;;
  inspect:--config)
    layout=${3#oci:}; layout=${layout%:dar}
    cat "$layout/config.json"
    ;;
  *) exit 13 ;;
esac
"#,
        ),
        (
            "umoci",
            r#"#!/bin/sh
set -eu
[ "$1" = unpack ] && [ "$2" = --rootless ] && [ "$3" = --image ]
mkdir "$5"
cp -a "$IMAGE_TEST_ROOT/image-files" "$5/rootfs"
[ "${IMAGE_TEST_FAIL:-}" != unpack ]
"#,
        ),
    ] {
        let path = root.join("tools").join(name);
        fs::write(&path, script)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    }
    Ok(fixture)
}

fn artifact_command(
    fixture: &ImageFixture,
    operation: &str,
    directory: &Path,
) -> TestResult<Command> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command
        .args(["cli", "images", operation])
        .arg(directory)
        .arg("--binary-artifacts")
        .arg(&fixture.artifacts)
        .env("IMAGE_TEST_ROOT", fixture.root.path());
    let mut paths = vec![fixture.root.path().join("tools")];
    paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
    command.env("PATH", env::join_paths(paths)?);
    Ok(ImageFixture::with_identity(command))
}

fn export_command(
    fixture: &ImageFixture,
    directory: &Path,
    architecture: &str,
) -> TestResult<Command> {
    let mut command = artifact_command(fixture, "export", directory)?;
    command.args([
        "--architecture",
        architecture,
        "--image",
        &format!("dar:{architecture}"),
    ]);
    Ok(command)
}

fn assert_failure(output: Output, message: &str) {
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(message),
        "{output:?}"
    );
}

#[test]
fn exported_images_assemble_byte_for_byte_and_verify_again() -> TestResult {
    let fixture = artifact_fixture()?;
    let inputs = fixture.root.path().join("image inputs");
    for architecture in ["amd64", "arm64"] {
        let directory = inputs.join(architecture);
        let output = assert_success(export_command(&fixture, &directory, architecture)?.output()?);
        let manifest: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(manifest["schema_version"], 1);
        assert_eq!(manifest["source_commit"], "a".repeat(40));
        assert_eq!(manifest["targets"][0]["architecture"], architecture);
        assert_eq!(
            manifest["targets"][0]["manifest_digest"],
            fs::read_to_string(fixture.root.path().join("digest"))?
        );
        assert_eq!(
            manifest["targets"][0]["binary_archive_sha256"],
            format!(
                "{:x}",
                Sha256::digest(fs::read(
                    fixture
                        .artifacts
                        .join(format!("dar-v1.2.3-linux-{architecture}-gnu.tar.gz"))
                )?)
            )
        );
        assert_eq!(fs::read_dir(&directory)?.count(), 2);
        assert_success(
            artifact_command(&fixture, "verify", &directory)?
                .args(["--architecture", architecture])
                .output()?,
        );
    }
    let complete = fixture.root.path().join("complete");
    let mut assemble = artifact_command(&fixture, "assemble", &complete)?;
    assemble.arg("--inputs").arg(&inputs);
    assert_success(assemble.output()?);
    assert_success(artifact_command(&fixture, "verify", &complete)?.output()?);
    let before = fs::read(complete.join("cli-images.json"))?;
    assert_eq!(fs::read_dir(&complete)?.count(), 3);
    for architecture in ["amd64", "arm64"] {
        let name = format!("dar-v1.2.3-linux-{architecture}.oci.tar");
        assert_eq!(
            fs::read(inputs.join(architecture).join(&name))?,
            fs::read(complete.join(&name))?
        );
    }
    assert_failure(assemble.output()?, "Could not access");
    assert_eq!(fs::read(complete.join("cli-images.json"))?, before);
    for name in ["saved-path", "layout-path"] {
        let path = PathBuf::from(fs::read_to_string(fixture.root.path().join(name))?);
        assert!(!path.parent().ok_or("missing parent")?.exists());
    }
    Ok(())
}

#[test]
fn changed_handoff_records_and_files_are_rejected() -> TestResult {
    let fixture = artifact_fixture()?;
    let directory = fixture.root.path().join("image");
    assert_success(export_command(&fixture, &directory, "amd64")?.output()?);
    let path = directory.join("cli-images.json");
    let original: Value = serde_json::from_slice(&fs::read(&path)?)?;
    for (pointer, value, message) in [
        ("/schema_version", json!(2), "build identity differs"),
        (
            "/source_commit",
            json!("b".repeat(40)),
            "build identity differs",
        ),
        ("/version", json!("1.2.4"), "build identity differs"),
        ("/rust_toolchain", json!("1.94.1"), "build identity differs"),
        (
            "/base_image",
            json!("debian:latest"),
            "build identity differs",
        ),
        (
            "/targets/0/archive",
            json!("../outside.oci.tar"),
            "identity or hash differs",
        ),
        (
            "/targets/0/architecture",
            json!("arm64"),
            "identity or hash differs",
        ),
        (
            "/targets/0/binary_archive_sha256",
            json!("0".repeat(64)),
            "identity or hash differs",
        ),
        (
            "/targets/0/archive_sha256",
            json!("0".repeat(64)),
            "identity or hash differs",
        ),
        (
            "/targets/0/manifest_digest",
            json!("sha256:wrong"),
            "manifest digest differ",
        ),
        (
            "/targets/0/dar_sha256",
            json!("0".repeat(64)),
            "manifest digest differ",
        ),
        (
            "/targets/0/configuration/User",
            json!("0:0"),
            "manifest digest differ",
        ),
        ("/targets", json!([]), "incomplete image target list"),
    ] {
        let mut changed = original.clone();
        *changed.pointer_mut(pointer).ok_or("missing test field")? = value;
        fs::write(&path, serde_json::to_vec(&changed)?)?;
        assert_failure(
            artifact_command(&fixture, "verify", &directory)?
                .args(["--architecture", "amd64"])
                .output()?,
            message,
        );
    }
    fs::write(&path, serde_json::to_vec(&original)?)?;
    let archive = directory.join("dar-v1.2.3-linux-amd64.oci.tar");
    fs::write(&archive, "changed")?;
    assert_failure(
        artifact_command(&fixture, "verify", &directory)?
            .args(["--architecture", "amd64"])
            .output()?,
        "identity or hash differs",
    );
    fs::remove_file(&archive)?;
    symlink(&fixture.binary, &archive)?;
    assert_failure(
        artifact_command(&fixture, "verify", &directory)?
            .args(["--architecture", "amd64"])
            .output()?,
        "expected a regular file",
    );
    Ok(())
}

#[test]
fn invalid_configuration_never_produces_a_handoff_manifest() -> TestResult {
    let fixture = artifact_fixture()?;
    let path = fixture.root.path().join("config-amd64.json");
    let original: Value = serde_json::from_slice(&fs::read(&path)?)?;
    for (index, (pointer, value, message)) in [
        ("/os", json!("windows"), "platform differs"),
        ("/architecture", json!("arm64"), "platform differs"),
        ("/config/User", json!("0:0"), "configuration: User"),
        (
            "/config/WorkingDir",
            json!("/"),
            "configuration: WorkingDir",
        ),
        (
            "/config/Entrypoint",
            json!(["sh", "-c", "dar"]),
            "configuration: Entrypoint",
        ),
        ("/config/Cmd", json!([]), "configuration: Cmd"),
        ("/config/Env", json!(["SECRET=value"]), "configuration: Env"),
        (
            "/config/Volumes",
            json!({"/work": {}}),
            "configuration: Volumes",
        ),
        (
            "/config/ExposedPorts",
            json!({"80/tcp": {}}),
            "configuration: ExposedPorts",
        ),
        (
            "/config/OnBuild",
            json!(["RUN true"]),
            "configuration: OnBuild",
        ),
        (
            "/config/Labels/org.opencontainers.image.revision",
            json!("b".repeat(40)),
            "revision label differs",
        ),
        (
            "/config/Labels/org.opencontainers.image.version",
            json!("1.2.4"),
            "version label differs",
        ),
        (
            "/config/Labels/org.opencontainers.image.base.name",
            json!("debian:latest"),
            "base.name label differs",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut changed = original.clone();
        *changed.pointer_mut(pointer).ok_or("missing test field")? = value;
        fs::write(&path, serde_json::to_vec(&changed)?)?;
        let directory = fixture.root.path().join(format!("image-{index}"));
        assert_failure(
            export_command(&fixture, &directory, "amd64")?.output()?,
            message,
        );
        assert!(!directory.join("cli-images.json").exists());
    }
    Ok(())
}

#[test]
fn damaged_image_files_and_container_tool_failures_are_rejected() -> TestResult {
    for case in [
        "save",
        "export",
        "import",
        "unpack",
        "digest",
        "index",
        "binary",
        "notice",
        "permissions",
        "symlink",
        "parent-symlink",
    ] {
        let fixture = artifact_fixture()?;
        let root = fixture.root.path();
        let binary = root.join("image-files/usr/local/bin/dar");
        match case {
            "digest" => fs::write(root.join("digest"), "sha256:wrong")?,
            "index" => {
                let index = serde_json::to_vec(
                    &json!({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json"}),
                )?;
                fs::write(root.join("oci-manifest.json"), &index)?;
                fs::write(
                    root.join("digest"),
                    format!("sha256:{:x}", Sha256::digest(&index)),
                )?;
            }
            "binary" => fs::write(&binary, "changed")?,
            "notice" => fs::write(root.join("image-files/usr/share/doc/dar/NOTICE"), "changed")?,
            "permissions" => fs::set_permissions(&binary, fs::Permissions::from_mode(0o777))?,
            "symlink" => {
                fs::remove_file(&binary)?;
                symlink(&fixture.binary, &binary)?;
            }
            "parent-symlink" => {
                let bin = binary.parent().ok_or("missing bin directory")?;
                let outside = root.join("outside-bin");
                fs::rename(bin, &outside)?;
                symlink(&outside, bin)?;
            }
            _ => {}
        }
        let directory = root.join("image");
        let output = export_command(&fixture, &directory, "amd64")?
            .env("IMAGE_TEST_FAIL", case)
            .output()?;
        assert_failure(
            output,
            match case {
                "save" => "docker failed",
                "export" | "import" => "skopeo failed",
                "unpack" => "umoci failed",
                "digest" | "index" => "single OCI platform manifest",
                "binary" => "image dar differs",
                "notice" => "image NOTICE differs",
                "permissions" => "permissions differ",
                "symlink" => "expected a regular file",
                "parent-symlink" => "escapes the root filesystem",
                _ => unreachable!(),
            },
        );
        assert!(!directory.join("cli-images.json").exists(), "{case}");
        for name in ["saved-path", "layout-path"] {
            if let Ok(path) = fs::read_to_string(root.join(name)) {
                assert!(
                    !Path::new(&path).parent().ok_or("missing parent")?.exists(),
                    "{case}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn incomplete_sets_and_extra_files_cannot_be_used_for_handoff() -> TestResult {
    let fixture = artifact_fixture()?;
    let inputs = fixture.root.path().join("inputs-images");
    let directory = inputs.join("amd64");
    assert_success(export_command(&fixture, &directory, "amd64")?.output()?);
    assert_failure(
        artifact_command(&fixture, "verify", &directory)?.output()?,
        "expected",
    );
    let complete = fixture.root.path().join("complete");
    assert_failure(
        artifact_command(&fixture, "assemble", &complete)?
            .arg("--inputs")
            .arg(&inputs)
            .output()?,
        "expected",
    );
    assert!(!complete.exists());
    fs::write(directory.join("extra.txt"), "unexpected")?;
    assert_failure(
        artifact_command(&fixture, "verify", &directory)?
            .args(["--architecture", "amd64"])
            .output()?,
        "expected",
    );
    fs::remove_file(directory.join("extra.txt"))?;
    let path = directory.join("cli-images.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&path)?)?;
    manifest["unknown"] = true.into();
    fs::write(path, serde_json::to_vec(&manifest)?)?;
    assert_failure(
        artifact_command(&fixture, "verify", &directory)?
            .args(["--architecture", "amd64"])
            .output()?,
        "unknown field",
    );
    fs::remove_file(fixture.artifacts.join("dar-v1.2.3-linux-arm64-gnu.tar.gz"))?;
    let destination = fixture.root.path().join("missing-binary");
    fs::remove_file(fixture.root.path().join("saved-path"))?;
    assert_failure(
        export_command(&fixture, &destination, "amd64")?.output()?,
        "expected",
    );
    assert!(!fixture.root.path().join("saved-path").exists());
    assert!(!destination.exists());
    Ok(())
}
