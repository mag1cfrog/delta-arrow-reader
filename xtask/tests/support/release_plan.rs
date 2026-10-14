use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    process::{Command, Output},
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::release_selection_fixture::{ARTIFACTS, Fixture};
use crate::support::{Result, write_zip};

pub struct PlanFixture {
    pub selection: Fixture,
    pub binaries: Vec<(String, Vec<u8>)>,
    pub images: Vec<(String, Vec<u8>)>,
}

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn tar_files(files: &[(&str, &[u8], u32)]) -> Result<Vec<u8>> {
    let mut archive = tar::Builder::new(Vec::new());
    for (name, bytes, mode) in files {
        let mut header = tar::Header::new_ustar();
        header.set_size(bytes.len() as u64);
        header.set_mode(*mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_entry_type(tar::EntryType::Regular);
        archive.append_data(&mut header, name, *bytes)?;
    }
    Ok(archive.into_inner()?)
}

impl PlanFixture {
    pub fn new(changed_image_binary: bool) -> Result<Self> {
        let selection = Fixture::new()?;
        let root = selection.directory.path();
        let binary = b"Fixture dar bytes; verification must never execute this file.\n";
        fs::write(root.join("dar"), binary)?;
        fs::write(
            root.join("notices"),
            "Fixture dependency redistribution terms\n",
        )?;
        for architecture in ["amd64", "arm64"] {
            let mut command = Self::xtask();
            command
                .args(["cli", "artifacts", "package"])
                .arg(root.join("inputs").join(architecture))
                .arg("--binary")
                .arg(root.join("dar"))
                .arg("--dependency-notices")
                .arg(root.join("notices"))
                .args([
                    "--architecture",
                    architecture,
                    "--runtime-dependency",
                    "libc.so.6",
                ]);
            Self::identity(&mut command);
            Self::success(command.output()?);
        }
        let mut command = Self::xtask();
        command
            .args(["cli", "artifacts", "assemble"])
            .arg(root.join("binaries"))
            .arg("--inputs")
            .arg(root.join("inputs"));
        Self::identity(&mut command);
        Self::success(command.output()?);
        let binaries = fs::read_dir(root.join("binaries"))?
            .map(|entry| {
                let entry = entry?;
                Ok((
                    entry.file_name().into_string().unwrap(),
                    fs::read(entry.path())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let notice = [
            include_bytes!("../../../NOTICE").as_slice(),
            b"\nFixture dependency redistribution terms\n",
        ]
        .concat();
        let image_binary = if changed_image_binary {
            b"Altered executable".as_slice()
        } else {
            binary.as_slice()
        };
        let base_image = include_str!("../../../crates/delta-arrow-reader-cli/Dockerfile")
            .lines()
            .find_map(|line| line.strip_prefix("ARG BASE_IMAGE="))
            .unwrap();
        let mut targets = Vec::new();
        let mut images = Vec::new();
        for architecture in ["amd64", "arm64"] {
            let files = [
                ("usr/local/bin/dar", image_binary, 0o755),
                (
                    "usr/share/doc/dar/LICENSE",
                    include_bytes!("../../../LICENSE").as_slice(),
                    0o644,
                ),
                ("usr/share/doc/dar/NOTICE", notice.as_slice(), 0o644),
            ];
            let layer = tar_files(&files)?;
            let layer_hash = sha256(&layer);
            let config = json!({
                "os":"linux", "architecture": architecture,
                "rootfs": {"type":"layers", "diff_ids":[format!("sha256:{layer_hash}")]},
                "config": {
                    "User":"65532:65532", "WorkingDir":"/work", "Entrypoint":["/usr/local/bin/dar"], "Cmd":["--help"],
                    "Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],
                    "Labels": {
                        "org.opencontainers.image.source":"https://github.com/mag1cfrog/delta-arrow-reader",
                        "org.opencontainers.image.licenses":"Apache-2.0", "org.opencontainers.image.revision":"a".repeat(40),
                        "org.opencontainers.image.version":"1.2.3", "org.opencontainers.image.base.name":base_image,
                    }
                }
            });
            let config_bytes = serde_json::to_vec(&config)?;
            let config_hash = sha256(&config_bytes);
            let manifest = serde_json::to_vec(&json!({
                "schemaVersion":2, "mediaType":"application/vnd.oci.image.manifest.v1+json",
                "config":{"mediaType":"application/vnd.oci.image.config.v1+json", "size":config_bytes.len(), "digest":format!("sha256:{config_hash}")},
                "layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar", "size":layer.len(), "digest":format!("sha256:{layer_hash}")}],
            }))?;
            let manifest_hash = sha256(&manifest);
            let index = serde_json::to_vec(&json!({
                "schemaVersion":2, "manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json",
                    "size":manifest.len(), "digest":format!("sha256:{manifest_hash}"), "annotations":{"org.opencontainers.image.ref.name":"dar"}}],
            }))?;
            let archive = tar_files(&[
                ("oci-layout", b"{\"imageLayoutVersion\":\"1.0.0\"}", 0o644),
                ("index.json", &index, 0o644),
                (&format!("blobs/sha256/{manifest_hash}"), &manifest, 0o644),
                (&format!("blobs/sha256/{config_hash}"), &config_bytes, 0o644),
                (&format!("blobs/sha256/{layer_hash}"), &layer, 0o644),
            ])?;
            let name = format!("dar-v1.2.3-linux-{architecture}.oci.tar");
            let binary_archive = binaries
                .iter()
                .find(|(name, _)| name == &format!("dar-v1.2.3-linux-{architecture}-gnu.tar.gz"))
                .unwrap();
            targets.push(json!({
                "os":"linux", "architecture":architecture, "binary_archive_sha256":sha256(&binary_archive.1),
                "dar_sha256":sha256(binary), "archive":name, "archive_sha256":sha256(&archive),
                "manifest_digest":format!("sha256:{manifest_hash}"), "configuration":config["config"],
            }));
            images.push((name, archive));
            fs::write(
                root.join(format!("config-{architecture}.json")),
                config_bytes,
            )?;
            fs::write(
                root.join(format!("manifest-{architecture}.json")),
                &manifest,
            )?;
            fs::write(
                root.join(format!("digest-{architecture}")),
                format!("sha256:{manifest_hash}"),
            )?;
            for (name, bytes, mode) in files {
                let path = root.join(format!("rootfs-{architecture}")).join(name);
                fs::create_dir_all(path.parent().unwrap())?;
                fs::write(&path, bytes)?;
                fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
            }
        }
        images.push(("cli-images.json".into(), serde_json::to_vec(&json!({
            "schema_version":1, "version":"1.2.3", "source_commit":"a".repeat(40), "rust_toolchain":"1.94.1", "base_image":base_image, "targets":targets,
        }))?));
        for (name, script) in [
            (
                "skopeo",
                r#"#!/bin/sh
set -eu
printf '%s\n' "skopeo $*" >> "$CLI_TEST_API/container-calls"
[ "${CLI_PLAN_TOOL_FAILURE:-}" != skopeo ] || exit 90
case "$1:$2" in
  copy:--all)
    [ "$3" = --preserve-digests ] && [ "$4" = --digestfile ]
    source=${6#oci-archive:}; destination=${7#oci:}; destination=${destination%:dar}
    case "$source" in *linux-amd64.oci.tar) arch=amd64 ;; *linux-arm64.oci.tar) arch=arm64 ;; *) exit 91 ;; esac
    mkdir "$destination"
    cp "$CLI_TEST_API/config-$arch.json" "$destination/config.json"
    cp "$CLI_TEST_API/manifest-$arch.json" "$destination/manifest.json"
    cp "$CLI_TEST_API/digest-$arch" "$5"
    printf '%s' "$arch" > "$destination/architecture"
    ;;
  inspect:--raw)
    case "$3" in
      oci-archive:*linux-amd64.oci.tar) cat "$CLI_TEST_API/manifest-amd64.json" ;;
      oci-archive:*linux-arm64.oci.tar) cat "$CLI_TEST_API/manifest-arm64.json" ;;
      oci:*) layout=${3#oci:}; layout=${layout%:dar}; cat "$layout/manifest.json" ;;
      *) exit 92 ;;
    esac ;;
  inspect:--config) layout=${3#oci:}; layout=${layout%:dar}; cat "$layout/config.json" ;;
  *) exit 93 ;;
esac
"#,
            ),
            (
                "umoci",
                r#"#!/bin/sh
set -eu
printf '%s\n' "umoci $*" >> "$CLI_TEST_API/container-calls"
[ "$1" = unpack ] && [ "$2" = --rootless ] && [ "$3" = --image ]
[ "${CLI_PLAN_TOOL_FAILURE:-}" != umoci ] || exit 94
layout=${4%:dar}; arch=$(cat "$layout/architecture")
mkdir "$5"
cp -a "$CLI_TEST_API/rootfs-$arch" "$5/rootfs"
"#,
            ),
        ] {
            let path = root.join("bin").join(name);
            fs::write(&path, script)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
        }
        let mut fixture = Self {
            selection,
            binaries,
            images,
        };
        fixture.update_downloads()?;
        Ok(fixture)
    }

    fn xtask() -> Command {
        Command::new(env!("CARGO_BIN_EXE_xtask"))
    }
    fn identity(command: &mut Command) {
        command.args([
            "--version",
            "1.2.3",
            "--source-commit",
            &"a".repeat(40),
            "--rust-toolchain",
            "1.94.1",
        ]);
    }
    pub fn success(output: Output) {
        assert!(output.status.success(), "{output:?}");
    }

    pub fn update_downloads(&mut self) -> Result<()> {
        for (index, id, manifest, field, files) in [
            (
                1,
                101,
                "cli-artifacts.json",
                "binary_artifact",
                &self.binaries,
            ),
            (2, 102, "cli-images.json", "image_artifact", &self.images),
        ] {
            let path = self.selection.directory.path().join(format!("{id}.zip"));
            write_zip(&path, files, index == 1)?;
            let bytes = fs::read(path)?;
            let metadata =
                &mut self.selection.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"][index];
            metadata["digest"] = json!(format!("sha256:{}", sha256(&bytes)));
            metadata["size_in_bytes"] = json!(bytes.len());
            self.selection
                .raw
                .insert(format!("actions/artifacts/{id}/zip"), bytes);
            if let Some((_, bytes)) = files.iter().find(|(name, _)| name == manifest) {
                self.selection.record[field]["manifest_sha256"] = json!(sha256(bytes));
            }
        }
        self.selection.pack_record(true)
    }

    pub fn command(&self) -> Result<Command> {
        self.selection.command_for("plan-release", "plan", true)
    }

    pub fn rejected(&self, output: Output, message: &str) -> Result<()> {
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{message}: {output:?}"
        );
        assert!(!self.selection.directory.path().join("plan").exists());
        assert!(
            !fs::read_dir(self.selection.directory.path())?.any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".dar-release-"))
        );
        Ok(())
    }

    pub fn check_plan(&self) -> Result<()> {
        let directory = self.selection.directory.path().join("plan");
        let plan: Value = serde_json::from_slice(&fs::read(directory.join("release-plan.json"))?)?;
        for (folder, files) in [("binaries", &self.binaries), ("images", &self.images)] {
            for (name, bytes) in files {
                assert_eq!(fs::read(directory.join(folder).join(name))?, *bytes);
            }
        }
        let assets = plan["github_assets"].as_array().unwrap();
        assert_eq!(assets.len(), 4);
        for asset in assets {
            let bytes = fs::read(directory.join(asset["path"].as_str().unwrap()))?;
            assert_eq!(asset["sha256"], sha256(&bytes));
            assert_eq!(asset["size"], bytes.len());
        }
        assert_eq!(plan["selection"]["release"]["commit"], "b".repeat(40));
        assert_eq!(
            plan["selection"]["validation_record"],
            self.selection.record
        );
        assert_eq!(
            plan["container_image"]["reference"],
            "ghcr.io/mag1cfrog/delta-arrow-reader:v1.2.3"
        );
        let bytes = fs::read(directory.join("image-index.json"))?;
        assert_eq!(
            plan["container_image"]["index_digest"],
            format!("sha256:{}", sha256(&bytes))
        );
        let index: Value = serde_json::from_slice(&bytes)?;
        assert_eq!(index["schemaVersion"], 2);
        assert_eq!(
            index["mediaType"],
            "application/vnd.oci.image.index.v1+json"
        );
        assert_eq!(index["manifests"].as_array().unwrap().len(), 2);
        for (i, architecture) in ["amd64", "arm64"].iter().enumerate() {
            let raw = fs::read(
                self.selection
                    .directory
                    .path()
                    .join(format!("manifest-{architecture}.json")),
            )?;
            assert_eq!(
                index["manifests"][i]["platform"],
                json!({"os":"linux", "architecture":architecture})
            );
            assert_eq!(
                index["manifests"][i]["digest"],
                format!("sha256:{}", sha256(&raw))
            );
            assert_eq!(index["manifests"][i]["size"], raw.len());
            let platform = &plan["container_image"]["platforms"][i];
            assert_eq!(platform["architecture"], *architecture);
            assert_eq!(
                platform["reference"],
                format!("ghcr.io/mag1cfrog/delta-arrow-reader:v1.2.3-linux-{architecture}")
            );
            assert_eq!(platform["manifest_digest"], index["manifests"][i]["digest"]);
            assert_eq!(
                platform["archive_sha256"],
                sha256(&fs::read(
                    directory.join(platform["archive_path"].as_str().unwrap())
                )?)
            );
        }
        Ok(())
    }
}
