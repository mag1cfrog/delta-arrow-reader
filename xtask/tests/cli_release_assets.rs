#![cfg(target_os = "linux")]

use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    process::{Command, Output},
};

use serde_json::{Value, json};

#[allow(dead_code)]
mod support;
use support::Result;
#[allow(dead_code)]
#[path = "support/release_plan.rs"]
mod release_plan_fixture;
#[allow(dead_code)]
#[path = "support/release_selection.rs"]
mod release_selection_fixture;
use release_plan_fixture::{PlanFixture, sha256};
use release_selection_fixture::{ARTIFACTS, REPOSITORY};

struct UploadFixture {
    plan: PlanFixture,
    plan_hash: String,
}

impl UploadFixture {
    fn new() -> Result<Self> {
        let plan = PlanFixture::new(false)?;
        PlanFixture::success(plan.command()?.output()?);
        let root = plan.selection.directory.path();
        let plan_hash = sha256(&fs::read(root.join("plan/release-plan.json"))?);
        fs::create_dir(root.join("remote"))?;
        fs::write(
            root.join("remote/assets.json"),
            serde_json::to_vec(&json!([
                {"id":9000, "name":"existing-python.whl", "state":"uploaded", "size":17, "digest":null}
            ]))?,
        )?;
        fs::write(
            root.join("remote/title-and-body"),
            "Release managed by release-plz\nPython and Rust changelog\n",
        )?;
        fs::rename(root.join("bin/gh"), root.join("bin/gh-readonly"))?;
        fs::write(
            root.join("bin/gh"),
            r#"#!/bin/sh
set -eu
[ "$1" = api ] && [ "$2" = --hostname ] && [ "$3" = github.com ] && [ "$4" = --method ] || exit 97
method=$5
endpoint=
input=
next_input=false
for arg do
  if [ "$next_input" = true ]; then input=$arg; next_input=false; fi
  case "$arg" in
    repos/mag1cfrog/delta-arrow-reader/*|https://uploads.github.com/repos/mag1cfrog/delta-arrow-reader/*) endpoint=$arg ;;
    --input) next_input=true ;;
  esac
done
remote=$CLI_TEST_API/remote
case "$method:$endpoint" in
  'GET:repos/mag1cfrog/delta-arrow-reader/releases/7/assets?per_page=100')
    printf '%s\n' "$*" >> "$remote/requests"
    [ "${CLI_UPLOAD_READ_FAILURE:-}" != list ] || exit 89
    case "$*" in *'--paginate --slurp'*) ;; *) exit 96 ;; esac
    if [ "${CLI_UPLOAD_CHANGE_LOCAL:-}" = true ]; then printf changed > "$CLI_TEST_API/plan/binaries/dar-v1.2.3-linux-amd64-gnu.tar.gz"; fi
    jq '[[], .]' "$remote/assets.json"
    ;;
  GET:repos/mag1cfrog/delta-arrow-reader/releases/assets/*)
    printf '%s\n' "$*" >> "$remote/requests"
    case "$*" in *'Accept: application/octet-stream'*) ;; *) exit 96 ;; esac
    [ "${CLI_UPLOAD_READ_FAILURE:-}" != download ] || exit 89
    id=${endpoint##*/}
    cat "$remote/$id"
    ;;
  POST:https://uploads.github.com/repos/mag1cfrog/delta-arrow-reader/releases/7/assets\?name=*)
    printf '%s\n' "$*" >> "$remote/requests"
    case "$*" in *'Content-Type: application/octet-stream'*) ;; *) exit 96 ;; esac
    name=${endpoint##*name=}
    printf '%s\n' "$name" >> "$remote/uploads"
    [ "$name" != "${CLI_UPLOAD_FAILURE:-}" ] || exit 88
    jq -e --arg name "$name" 'all(.[]; .name != $name)' "$remote/assets.json" > /dev/null || exit 87
    id=$(jq 'length + 1000' "$remote/assets.json")
    hash=$(sha256sum "$input"); hash=${hash%% *}
    size=$(wc -c < "$input")
    cp "$input" "$remote/$id"
    if [ "${CLI_UPLOAD_CORRUPT_DELIVERY:-}" = true ]; then printf 'altered' > "$remote/$id"; fi
    jq -n --arg name "$name" --arg hash "$hash" --argjson id "$id" --argjson size "$size" \
      '{id:$id,name:$name,state:"uploaded",size:$size,digest:("sha256:" + $hash)}' > "$remote/response.json"
    jq --slurpfile asset "$remote/response.json" '. + $asset' "$remote/assets.json" > "$remote/next.json"
    mv "$remote/next.json" "$remote/assets.json"
    if [ "${CLI_UPLOAD_DISAPPEAR:-}" = true ]; then
      jq --argjson id "$id" 'map(select(.id != $id))' "$remote/assets.json" > "$remote/next.json"
      mv "$remote/next.json" "$remote/assets.json"
    fi
    if [ "${CLI_UPLOAD_BAD_RESPONSE:-}" = true ]; then
      jq '.name = "unexpected"' "$remote/response.json" > "$remote/next.json"
      mv "$remote/next.json" "$remote/response.json"
    fi
    if [ "${CLI_UPLOAD_MOVE_TAG:-}" = true ]; then
      printf '%s' '{"object":{"type":"commit","sha":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}}' > "$CLI_TEST_API/responses/repos/mag1cfrog/delta-arrow-reader/git/ref/tags/v1.2.3.body"
    fi
    cat "$remote/response.json"
    ;;
  GET:*) exec "$CLI_TEST_API/bin/gh-readonly" "$@" ;;
  *) printf '%s\n' "Forbidden request: $*" >&2; exit 95 ;;
esac
"#,
        )?;
        fs::set_permissions(root.join("bin/gh"), fs::Permissions::from_mode(0o755))?;
        fs::remove_file(root.join("container-calls"))?;
        fs::remove_file(root.join("calls"))?;
        for name in ["skopeo", "umoci", "docker", "cargo"] {
            fs::write(
                root.join("bin").join(name),
                "#!/bin/sh\nprintf forbidden > \"$CLI_TEST_API/forbidden-tool\"\nexit 96\n",
            )?;
            fs::set_permissions(
                root.join("bin").join(name),
                fs::Permissions::from_mode(0o755),
            )?;
        }
        Ok(Self { plan, plan_hash })
    }

    fn command(&self) -> Result<Command> {
        let mut command = self.plan.selection.xtask()?;
        command
            .args([
                "cli",
                "upload-release-assets",
                "plan",
                "--plan-sha256",
                &self.plan_hash,
            ])
            .env_remove("GITHUB_ACTIONS")
            .env_remove("GITHUB_EVENT_NAME");
        Ok(command)
    }

    fn remote(&self) -> Result<Value> {
        Ok(serde_json::from_slice(&fs::read(
            self.plan
                .selection
                .directory
                .path()
                .join("remote/assets.json"),
        )?)?)
    }

    fn set_remote(&self, value: &Value) -> Result<()> {
        Ok(fs::write(
            self.plan
                .selection
                .directory
                .path()
                .join("remote/assets.json"),
            serde_json::to_vec(value)?,
        )?)
    }

    fn add_existing(&self, index: usize) -> Result<()> {
        let (name, bytes) = &self.plan.binaries[index];
        let mut assets = self.remote()?;
        let id = 2000 + index;
        assets.as_array_mut().unwrap().push(json!({"id":id, "name":name, "state":"uploaded", "size":bytes.len(), "digest":format!("sha256:{}", sha256(bytes))}));
        self.set_remote(&assets)?;
        Ok(fs::write(
            self.plan
                .selection
                .directory
                .path()
                .join(format!("remote/{id}")),
            bytes,
        )?)
    }

    fn uploads(&self) -> Result<Vec<String>> {
        let path = self.plan.selection.directory.path().join("remote/uploads");
        Ok(if path.exists() {
            fs::read_to_string(path)?
                .lines()
                .map(str::to_owned)
                .collect()
        } else {
            Vec::new()
        })
    }

    fn rejected(&self, output: Output, message: &str) -> Result<()> {
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{message}: {output:?}"
        );
        assert!(self.uploads()?.is_empty());
        Ok(())
    }

    fn check_published(&self) -> Result<()> {
        let root = self.plan.selection.directory.path();
        let assets = self.remote()?;
        assert_eq!(assets.as_array().unwrap().len(), 5);
        assert_eq!(
            assets[0],
            json!({"id":9000,"name":"existing-python.whl","state":"uploaded","size":17,"digest":null})
        );
        for (name, bytes) in &self.plan.binaries {
            let asset = assets
                .as_array()
                .unwrap()
                .iter()
                .find(|asset| asset["name"] == *name)
                .unwrap();
            assert_eq!(
                fs::read(root.join(format!("remote/{}", asset["id"])))?,
                *bytes
            );
        }
        assert_eq!(
            fs::read_to_string(root.join("remote/title-and-body"))?,
            "Release managed by release-plz\nPython and Rust changelog\n"
        );
        assert!(!root.join("forbidden-tool").exists());
        assert!(!root.join("container-calls").exists());
        let reads = fs::read_to_string(root.join("calls"))?;
        for id in [101, 102] {
            assert!(!reads.contains(&format!("actions/artifacts/{id}/zip")));
        }
        assert!(!fs::read_to_string(root.join("remote/requests"))?.contains("DELETE"));
        Ok(())
    }
}

#[test]
fn publishes_exact_files_and_repeating_the_same_plan_is_a_no_op() -> Result<()> {
    let fixture = UploadFixture::new()?;
    PlanFixture::success(fixture.command()?.output()?);
    fixture.check_published()?;
    assert_eq!(fixture.uploads()?.len(), 4);
    PlanFixture::success(fixture.command()?.output()?);
    assert_eq!(fixture.uploads()?.len(), 4);
    fixture.check_published()
}

#[test]
fn delivery_requires_all_assets_and_checks_the_downloaded_bytes() -> Result<()> {
    let fixture = UploadFixture::new()?;
    let verify = || -> Result<Command> {
        let mut command = fixture.plan.selection.xtask()?;
        command.args([
            "cli",
            "verify-release-assets",
            "plan",
            "--plan-sha256",
            &fixture.plan_hash,
        ]);
        Ok(command)
    };
    fixture.rejected(verify()?.output()?, "missing CLI assets")?;
    PlanFixture::success(fixture.command()?.output()?);
    PlanFixture::success(verify()?.output()?);
    let assets = fixture.remote()?;
    let id = assets[1]["id"].as_u64().unwrap();
    fs::write(
        fixture
            .plan
            .selection
            .directory
            .path()
            .join(format!("remote/{id}")),
        "corrupt bytes despite unchanged metadata",
    )?;
    let output = verify()?.output()?;
    assert!(!output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("different bytes"));
    assert_eq!(fixture.uploads()?.len(), 4);
    Ok(())
}

#[test]
fn read_only_rehearsal_uses_the_same_remote_conflict_checks() -> Result<()> {
    let fixture = UploadFixture::new()?;
    fixture.add_existing(0)?;
    let output = fixture.command()?.arg("--dry-run").output()?;
    assert!(String::from_utf8_lossy(&output.stdout).contains("Would upload"));
    PlanFixture::success(output);
    assert!(fixture.uploads()?.is_empty());
    let mut assets = fixture.remote()?;
    assets[1]["size"] = json!(0);
    fixture.set_remote(&assets)?;
    fixture.rejected(
        fixture.command()?.arg("--dry-run").output()?,
        "conflicting or incomplete",
    )
}

#[test]
fn uploaded_bytes_come_from_the_private_verified_snapshot() -> Result<()> {
    let fixture = UploadFixture::new()?;
    PlanFixture::success(
        fixture
            .command()?
            .env("CLI_UPLOAD_CHANGE_LOCAL", "true")
            .output()?,
    );
    fixture.check_published()
}

#[test]
fn retries_a_partial_upload_with_the_original_set() -> Result<()> {
    let fixture = UploadFixture::new()?;
    let output = fixture
        .command()?
        .env("CLI_UPLOAD_FAILURE", "cli-artifacts.json")
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("gh failed"));
    assert_eq!(fixture.remote()?.as_array().unwrap().len(), 3);
    PlanFixture::success(fixture.command()?.output()?);
    assert_eq!(
        fixture.uploads()?,
        [
            "dar-v1.2.3-linux-amd64-gnu.tar.gz",
            "dar-v1.2.3-linux-arm64-gnu.tar.gz",
            "cli-artifacts.json",
            "cli-artifacts.json",
            "SHA256SUMS"
        ]
    );
    fixture.check_published()
}

#[test]
fn checks_all_existing_content_before_uploading_any_missing_file() -> Result<()> {
    for (field, value) in [
        ("digest", json!(format!("sha256:{}", "0".repeat(64)))),
        ("size", json!(0)),
        ("state", json!("starter")),
    ] {
        let fixture = UploadFixture::new()?;
        fixture.add_existing(0)?;
        let mut remote = fixture.remote()?;
        remote[1][field] = value;
        fixture.set_remote(&remote)?;
        fixture.rejected(fixture.command()?.output()?, "conflicting or incomplete")?;
    }
    let fixture = UploadFixture::new()?;
    for index in 0..fixture.plan.binaries.len() {
        fixture.add_existing(index)?;
    }
    let mut remote = fixture.remote()?;
    for asset in remote.as_array_mut().unwrap() {
        asset["digest"] = Value::Null;
    }
    fixture.set_remote(&remote)?;
    PlanFixture::success(fixture.command()?.output()?);
    assert!(fixture.uploads()?.is_empty());
    fixture.check_published()
}

#[test]
fn rejects_remote_duplicates_and_corruption_even_with_a_matching_metadata_digest() -> Result<()> {
    let fixture = UploadFixture::new()?;
    fixture.add_existing(0)?;
    let mut remote = fixture.remote()?;
    let duplicate = remote[1].clone();
    remote.as_array_mut().unwrap().push(duplicate);
    fixture.set_remote(&remote)?;
    fixture.rejected(fixture.command()?.output()?, "duplicate release asset")?;
    remote.as_array_mut().unwrap().pop();
    fixture.set_remote(&remote)?;
    fs::write(
        fixture.plan.selection.directory.path().join("remote/2000"),
        b"corrupt",
    )?;
    fixture.rejected(fixture.command()?.output()?, "different bytes")
}

#[test]
fn refuses_changed_plan_files_and_expired_or_different_provenance() -> Result<()> {
    for path in [
        "release-plan.json",
        "binaries/SHA256SUMS",
        "images/cli-images.json",
        "images/dar-v1.2.3-linux-arm64.oci.tar",
        "image-index.json",
    ] {
        let fixture = UploadFixture::new()?;
        fs::write(
            fixture
                .plan
                .selection
                .directory
                .path()
                .join("plan")
                .join(path),
            b"changed",
        )?;
        fixture.rejected(
            fixture.command()?.output()?,
            if path == "release-plan.json" {
                "trusted verification handoff"
            } else if path.starts_with("binaries/") {
                "different bytes"
            } else {
                "differs"
            },
        )?;
    }
    let mut fixture = UploadFixture::new()?;
    fixture.plan.selection.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"][2]["expired"] =
        json!(true);
    fixture.rejected(fixture.command()?.output()?, "expired")?;
    let mut fixture = UploadFixture::new()?;
    fixture.plan.selection.record["source_commit"] = json!("e".repeat(40));
    fixture.plan.selection.pack_record(true)?;
    fixture.rejected(fixture.command()?.output()?, "differs")
}

#[test]
fn rejects_symlink_inputs_and_release_identity_changes_before_mutation() -> Result<()> {
    let fixture = UploadFixture::new()?;
    let root = fixture.plan.selection.directory.path();
    fs::rename(
        root.join("plan/binaries/SHA256SUMS"),
        root.join("saved-checksums"),
    )?;
    symlink(
        root.join("saved-checksums"),
        root.join("plan/binaries/SHA256SUMS"),
    )?;
    fixture.rejected(fixture.command()?.output()?, "regular file")?;
    let fixture = UploadFixture::new()?;
    let mut command = fixture.command()?;
    fs::write(
        fixture
            .plan
            .selection
            .directory
            .path()
            .join("responses/repos/mag1cfrog/delta-arrow-reader/git/ref/tags/v1.2.3.body.next"),
        serde_json::to_vec(&json!({"object":{"type":"commit","sha":"e".repeat(40)}}))?,
    )?;
    fixture.rejected(command.output()?, "changed during")
}

#[test]
fn download_and_upload_failures_cannot_report_complete_publication() -> Result<()> {
    for operation in ["list", "download"] {
        let fixture = UploadFixture::new()?;
        fixture.add_existing(0)?;
        fixture.rejected(
            fixture
                .command()?
                .env("CLI_UPLOAD_READ_FAILURE", operation)
                .output()?,
            "gh failed",
        )?;
    }
    for (option, message) in [
        ("CLI_UPLOAD_CORRUPT_DELIVERY", "different bytes"),
        ("CLI_UPLOAD_MOVE_TAG", "changed during"),
        ("CLI_UPLOAD_DISAPPEAR", "disappeared during publication"),
        ("CLI_UPLOAD_BAD_RESPONSE", "conflicting or incomplete"),
    ] {
        let fixture = UploadFixture::new()?;
        let output = fixture.command()?.env(option, "true").output()?;
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{output:?}"
        );
        assert_eq!(fixture.uploads()?.len(), 1);
    }
    Ok(())
}

#[test]
fn rejects_malformed_handoff_shapes_even_when_the_supplied_hash_matches() -> Result<()> {
    for (pointer, value, message) in [
        (
            "/schema_version",
            json!(2),
            "unsupported CLI release plan schema",
        ),
        (
            "/selection/release/commit",
            json!("e".repeat(40)),
            "provenance differs",
        ),
        (
            "/github_assets/0/path",
            json!("../secret"),
            "asset name or path",
        ),
        (
            "/github_assets/0/name",
            json!("other"),
            "asset name or path",
        ),
        ("/github_assets", json!([]), "exactly four"),
        (
            "/container_image/reference",
            json!("ghcr.io/other/project:v1.2.3"),
            "image destination",
        ),
        (
            "/container_image/platforms/0/archive_path",
            json!("../../secret"),
            "image identity",
        ),
        ("/container_image/platforms", json!([]), "image destination"),
    ] {
        let mut fixture = UploadFixture::new()?;
        let path = fixture
            .plan
            .selection
            .directory
            .path()
            .join("plan/release-plan.json");
        let mut plan: Value = serde_json::from_slice(&fs::read(&path)?)?;
        *plan.pointer_mut(pointer).unwrap() = value;
        let bytes = serde_json::to_vec(&plan)?;
        fixture.plan_hash = sha256(&bytes);
        fs::write(path, bytes)?;
        fixture.rejected(fixture.command()?.output()?, message)?;
    }
    Ok(())
}

#[test]
fn actions_publication_accepts_only_the_upstream_release_or_main_recovery_workflow() -> Result<()> {
    for (event, reference) in [
        ("release", "refs/tags/v1.2.3"),
        ("workflow_dispatch", "refs/heads/main"),
    ] {
        let fixture = UploadFixture::new()?;
        let mut command = fixture.command()?;
        command
            .env("GITHUB_ACTIONS", "true")
            .env("GITHUB_EVENT_NAME", event)
            .env("GITHUB_REF", reference)
            .env("GITHUB_SHA", "b".repeat(40))
            .env(
                "GITHUB_WORKFLOW_REF",
                format!("{REPOSITORY}/.github/workflows/cli-release.yml@{reference}"),
            );
        for (key, invalid) in [
            ("GITHUB_EVENT_NAME", "pull_request"),
            ("GITHUB_EVENT_NAME", "pull_request_target"),
            ("GITHUB_EVENT_NAME", "push"),
            ("GITHUB_REF", "refs/heads/feature"),
            ("GITHUB_WORKFLOW_REF", "untrusted/workflow@refs/heads/main"),
        ] {
            let original = command
                .get_envs()
                .find(|(name, _)| *name == key)
                .unwrap()
                .1
                .unwrap()
                .to_owned();
            fixture.rejected(command.env(key, invalid).output()?, "CLI publication")?;
            command.env(key, original);
        }
        if event == "release" {
            fixture.rejected(
                command.env("GITHUB_SHA", "e".repeat(40)).output()?,
                "event commit differs",
            )?;
            command.env("GITHUB_SHA", "b".repeat(40));
        }
        PlanFixture::success(command.output()?);
        fixture.check_published()?;
    }
    Ok(())
}
