#![cfg(target_os = "linux")]

use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    process::{Command, Output},
};

use serde_json::json;

#[allow(dead_code)]
#[path = "support/common.rs"]
mod support;
use support::Result;
#[allow(dead_code)]
#[path = "support/release_plan.rs"]
mod release_plan_fixture;
#[allow(dead_code)]
#[path = "support/release_selection.rs"]
mod release_selection_fixture;
use release_plan_fixture::{PlanFixture, sha256};
use release_selection_fixture::ARTIFACTS;

const TAGS: [&str; 3] = ["v1.2.3-linux-amd64", "v1.2.3-linux-arm64", "v1.2.3"];

struct ImageFixture {
    plan: PlanFixture,
    plan_hash: String,
}

impl ImageFixture {
    fn new() -> Result<Self> {
        let plan = PlanFixture::new(false)?;
        PlanFixture::success(plan.command()?.output()?);
        let root = plan.selection.directory.path();
        let plan_hash = sha256(&fs::read(root.join("plan/release-plan.json"))?);
        fs::create_dir(root.join("remote"))?;
        fs::create_dir(root.join("expected"))?;
        for (name, bytes) in &plan.images {
            fs::write(root.join("expected").join(name), bytes)?;
        }
        fs::copy(
            root.join("plan/image-index.json"),
            root.join("expected/index.json"),
        )?;
        fs::write(
            root.join("publisher.json"),
            "{\"hosts\":{\"ghcr.io\":{\"user\":\"publisher\",\"pass\":\"fixture\"}}}",
        )?;
        write_tool(
            &root.join("bin/regctl"),
            r#"#!/bin/sh
set -eu
reference=$3
case "$reference" in
  ghcr.io/mag1cfrog/delta-arrow-reader:v1.2.3-linux-amd64) tag=v1.2.3-linux-amd64; arch=amd64 ;;
  ghcr.io/mag1cfrog/delta-arrow-reader:v1.2.3-linux-arm64) tag=v1.2.3-linux-arm64; arch=arm64 ;;
  ghcr.io/mag1cfrog/delta-arrow-reader:v1.2.3) tag=v1.2.3; arch= ;;
  ghcr.io/mag1cfrog/delta-arrow-reader@sha256:*)
    digest=${reference##*@}
    if [ "$digest" = "$(cat "$CLI_TEST_API/digest-amd64")" ]; then arch=amd64;
    elif [ "$digest" = "$(cat "$CLI_TEST_API/digest-arm64")" ]; then arch=arm64;
    else exit 96; fi
    tag=v1.2.3-linux-$arch ;;
  *) exit 96 ;;
esac
remote=$CLI_TEST_API/remote
anonymous=false
if [ "$REGCTL_CONFIG" != "$CLI_TEST_API/publisher.json" ]; then
  jq -e '. == {version:1,incDockerCred:false,incDockerCert:false,hosts:{}}' "$REGCTL_CONFIG" > /dev/null
  anonymous=true
fi
printf '%s\n' "$anonymous $*" >> "$remote/requests"
if [ "$anonymous" = true ] && [ "${CLI_REG_PRIVATE:-}" = true ]; then
  printf '%s\n' "failed to get manifest $reference: request failed: unauthorized [http 403]" >&2
  exit 1
fi
case "$1:$2" in
  manifest:get)
    [ "$4" = --format ] && [ "$5" = raw-body ]
    if [ -n "${CLI_REG_READ_FAILURE:-}" ]; then
      printf '%s\n' "failed to get manifest $reference: $CLI_REG_READ_FAILURE" >&2
      exit 1
    fi
    if [ "${CLI_REG_CHANGE_INPUTS:-}" = true ]; then
      printf altered > "$CLI_TEST_API/plan/images/dar-v1.2.3-linux-amd64.oci.tar"
      printf altered > "$CLI_TEST_API/plan/image-index.json"
    fi
    if [ -f "$remote/$tag.json" ]; then cat "$remote/$tag.json"; else
      printf '%s\n' "failed to get manifest $reference: request failed: not found [http 404]: {\"errors\":[{\"code\":\"MANIFEST_UNKNOWN\"}]}" >&2
      exit 1
    fi ;;
  image:import|manifest:put)
    [ "$anonymous" = false ]
    printf '%s\n' "$tag" >> "$remote/writes"
    [ "$tag" != "${CLI_REG_WRITE_FAILURE:-}" ] || exit 88
    [ ! -f "$remote/$tag.json" ] || exit 87
    if [ "$1" = image ]; then
      cmp "$4" "$CLI_TEST_API/expected/dar-v1.2.3-linux-$arch.oci.tar"
      cp "$4" "$remote/$arch.tar"
      cp "$CLI_TEST_API/manifest-$arch.json" "$remote/$tag.json"
    else
      [ "$4" = --content-type ] && [ "$5" = application/vnd.oci.image.index.v1+json ]
      test -f "$remote/v1.2.3-linux-amd64.json"
      test -f "$remote/v1.2.3-linux-arm64.json"
      cat > "$remote/$tag.json"
      cmp "$remote/$tag.json" "$CLI_TEST_API/expected/index.json"
    fi
    if [ "${CLI_REG_CORRUPT:-}" = true ]; then printf changed > "$remote/$tag.json"; fi
    if [ "${CLI_REG_DISAPPEAR:-}" = true ]; then rm "$remote/$tag.json"; fi
    if [ "${CLI_REG_MOVE_TAG:-}" = true ]; then
      printf '%s' '{"object":{"type":"commit","sha":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}}' > "$CLI_TEST_API/responses/repos/mag1cfrog/delta-arrow-reader/git/ref/tags/v1.2.3.body"
    fi
    if [ "${CLI_REG_LATE_CONFLICT:-}" = true ]; then printf changed > "$remote/v1.2.3.json"; fi
    ;;
  image:export)
    [ "$anonymous" = true ]
    [ "${CLI_REG_EXPORT_FAILURE:-}" != true ] || exit 89
    cp "$remote/$arch.tar" "$4"
    ;;
  *) exit 95 ;;
esac
"#,
        )?;
        for name in ["cargo", "docker"] {
            write_tool(
                &root.join("bin").join(name),
                "#!/bin/sh\nprintf forbidden > \"$CLI_TEST_API/forbidden-tool\"\nexit 96\n",
            )?;
        }
        Ok(Self { plan, plan_hash })
    }

    fn root(&self) -> &Path {
        self.plan.selection.directory.path()
    }

    fn command(&self, task: &str) -> Result<Command> {
        let mut command = self.plan.selection.xtask()?;
        command
            .args(["cli", task, "plan", "--plan-sha256", &self.plan_hash])
            .env("REGCTL_CONFIG", self.root().join("publisher.json"))
            .env_remove("GITHUB_ACTIONS");
        Ok(command)
    }

    fn upload(&self) -> Result<Command> {
        self.command("upload-release-images")
    }
    fn verify(&self) -> Result<Command> {
        self.command("verify-release-images")
    }

    fn writes(&self) -> Result<Vec<String>> {
        let path = self.root().join("remote/writes");
        Ok(if path.exists() {
            fs::read_to_string(path)?
                .lines()
                .map(str::to_owned)
                .collect()
        } else {
            Vec::new()
        })
    }

    fn rejected(&self, output: Output, message: &str, writes: &[&str]) -> Result<()> {
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{message}: {output:?}"
        );
        assert_eq!(self.writes()?, writes);
        assert!(!self.root().join("forbidden-tool").exists());
        Ok(())
    }
}

fn write_tool(path: &Path, script: &str) -> Result<()> {
    fs::write(path, script)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    Ok(())
}

#[test]
fn publishes_platforms_before_the_index_and_reuses_identical_content() -> Result<()> {
    let fixture = ImageFixture::new()?;
    PlanFixture::success(fixture.upload()?.output()?);
    assert_eq!(fixture.writes()?, TAGS);
    PlanFixture::success(fixture.upload()?.output()?);
    assert_eq!(fixture.writes()?, TAGS);
    PlanFixture::success(fixture.verify()?.output()?);
    let calls = fs::read_to_string(fixture.root().join("remote/requests"))?;
    assert_eq!(
        calls
            .lines()
            .filter(|line| line.starts_with("true image export"))
            .count(),
        2
    );
    assert!(!fixture.root().join("forbidden-tool").exists());
    Ok(())
}

#[test]
fn rehearses_without_writes_and_rejects_all_conflicts_before_any_upload() -> Result<()> {
    let fixture = ImageFixture::new()?;
    let output = fixture.upload()?.arg("--dry-run").output()?;
    assert_eq!(
        String::from_utf8_lossy(&output.stdout)
            .matches("Would publish")
            .count(),
        3
    );
    PlanFixture::success(output);
    assert!(fixture.writes()?.is_empty());
    for tag in TAGS {
        let path = fixture.root().join(format!("remote/{tag}.json"));
        fs::write(&path, b"conflicting version or provenance")?;
        for dry in [false, true] {
            let mut command = fixture.upload()?;
            if dry {
                command.arg("--dry-run");
            }
            fixture.rejected(command.output()?, "conflicting content", &[])?;
        }
        fs::remove_file(path)?;
    }
    Ok(())
}

#[test]
fn retries_partial_publication_using_only_the_original_missing_images() -> Result<()> {
    for (index, tag) in TAGS.iter().enumerate() {
        let fixture = ImageFixture::new()?;
        fixture.rejected(
            fixture
                .upload()?
                .env("CLI_REG_WRITE_FAILURE", tag)
                .output()?,
            "regctl failed",
            &TAGS[..=index],
        )?;
        assert!(!fixture.root().join("remote/v1.2.3.json").exists());
        PlanFixture::success(fixture.upload()?.output()?);
        let expected: Vec<_> = TAGS[..=index]
            .iter()
            .chain(TAGS[index..].iter())
            .map(|tag| tag.to_string())
            .collect();
        assert_eq!(fixture.writes()?, expected);
        PlanFixture::success(fixture.verify()?.output()?);
    }
    Ok(())
}

#[test]
fn fails_on_private_images_and_retries_after_visibility_is_fixed_without_writes() -> Result<()> {
    let fixture = ImageFixture::new()?;
    fixture.rejected(
        fixture.upload()?.env("CLI_REG_PRIVATE", "true").output()?,
        "package must be public",
        &TAGS,
    )?;
    fixture.rejected(
        fixture.verify()?.env("CLI_REG_PRIVATE", "true").output()?,
        "unauthorized",
        &TAGS,
    )?;
    PlanFixture::success(fixture.upload()?.output()?);
    PlanFixture::success(fixture.verify()?.output()?);
    assert_eq!(fixture.writes()?, TAGS);
    Ok(())
}

#[test]
fn does_not_treat_lookup_errors_as_missing_tags() -> Result<()> {
    let fixture = ImageFixture::new()?;
    for error in [
        "request failed: unauthorized [http 401]",
        "request failed: unauthorized [http 403]",
        "request failed: rate limit exceeded [http 429]",
        "request failed: unexpected http status: Internal Server Error [http 500]",
        "dial tcp: no such host",
        "TLS handshake failure",
        "file not found",
        "unrecognized error mentioning [http 404]",
        "failed to acquire token: not found [http 404]",
    ] {
        fixture.rejected(
            fixture
                .upload()?
                .env("CLI_REG_READ_FAILURE", error)
                .output()?,
            "regctl failed",
            &[],
        )?;
    }
    Ok(())
}

#[test]
fn stages_all_original_inputs_before_remote_requests() -> Result<()> {
    let fixture = ImageFixture::new()?;
    PlanFixture::success(
        fixture
            .upload()?
            .env("CLI_REG_CHANGE_INPUTS", "true")
            .output()?,
    );
    assert_eq!(fixture.writes()?, TAGS);
    assert_eq!(
        fs::read(fixture.root().join("remote/v1.2.3.json"))?,
        fs::read(fixture.root().join("expected/index.json"))?
    );
    Ok(())
}

#[test]
fn rejects_changed_handoffs_expired_artifacts_and_untrusted_actions() -> Result<()> {
    for kind in [
        "plan", "archive", "index", "symlink", "expired", "tag", "actions",
    ] {
        let mut fixture = ImageFixture::new()?;
        let message = match kind {
            "plan" => {
                fs::write(fixture.root().join("plan/release-plan.json"), "{}")?;
                "handoff"
            }
            "archive" => {
                fs::write(
                    fixture
                        .root()
                        .join("plan/images/dar-v1.2.3-linux-arm64.oci.tar"),
                    "changed",
                )?;
                "file hash differs"
            }
            "index" => {
                fs::write(fixture.root().join("plan/image-index.json"), "changed")?;
                "index SHA256 differs"
            }
            "symlink" => {
                let path = fixture.root().join("plan/image-index.json");
                fs::remove_file(&path)?;
                symlink(fixture.root().join("expected/index.json"), path)?;
                "regular file"
            }
            "expired" => {
                fixture.plan.selection.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"][2]["expired"] =
                    json!(true);
                "expired"
            }
            "tag" => {
                fs::write(fixture.root().join("responses/repos/mag1cfrog/delta-arrow-reader/git/ref/tags/v1.2.3.body.next"),
                    serde_json::to_vec(&json!({"object":{"type":"commit","sha":"e".repeat(40)}}))?)?;
                "release or tag changed"
            }
            "actions" => "requires a release event",
            _ => unreachable!(),
        };
        let mut command = fixture.upload()?;
        if kind == "actions" {
            command
                .env("GITHUB_ACTIONS", "true")
                .env("GITHUB_EVENT_NAME", "pull_request_target");
        }
        fixture.rejected(command.output()?, message, &[])?;
        if kind != "tag" {
            assert!(!fixture.root().join("remote/requests").exists());
        }
    }
    Ok(())
}

#[test]
fn stops_after_corruption_disappearance_new_conflicts_or_a_moved_release_tag() -> Result<()> {
    for (variable, message) in [
        ("CLI_REG_CORRUPT", "conflicting content"),
        ("CLI_REG_DISAPPEAR", "reference is missing"),
        ("CLI_REG_LATE_CONFLICT", "conflicting content"),
        ("CLI_REG_MOVE_TAG", "release or tag changed"),
    ] {
        let fixture = ImageFixture::new()?;
        fixture.rejected(
            fixture.upload()?.env(variable, "true").output()?,
            message,
            &TAGS[..1],
        )?;
    }
    Ok(())
}

#[test]
fn delivery_checks_downloads_and_binary_identity_without_executing_images() -> Result<()> {
    let fixture = ImageFixture::new()?;
    PlanFixture::success(fixture.upload()?.output()?);
    fixture.rejected(
        fixture
            .verify()?
            .env("CLI_REG_EXPORT_FAILURE", "true")
            .output()?,
        "regctl failed",
        &TAGS,
    )?;
    fs::write(
        fixture.root().join("rootfs-arm64/usr/local/bin/dar"),
        "changed executable",
    )?;
    fixture.rejected(fixture.verify()?.output()?, "differ", &TAGS)?;
    Ok(())
}

#[test]
#[ignore = "requires regctl v0.9.2, skopeo, umoci, and Docker; release image validation runs this test"]
fn real_registry_preserves_manifests_and_published_binary_contents() -> Result<()> {
    let fixture = ImageFixture::new()?;
    // The wrapper rewrites every approved GHCR reference to this disposable local registry.
    let output = Command::new("docker")
        .args([
            "run",
            "--detach",
            "--rm",
            "--publish",
            "127.0.0.1::5000",
            "registry:2.8.3",
        ])
        .output()?;
    let id = String::from_utf8(output.stdout.clone())?.trim().to_owned();
    PlanFixture::success(output);
    struct Registry(String);
    impl Drop for Registry {
        fn drop(&mut self) {
            let _ = Command::new("docker")
                .args(["rm", "--force", &self.0])
                .output();
        }
    }
    let registry = Registry(id);
    let output = Command::new("docker")
        .args(["port", &registry.0, "5000/tcp"])
        .output()?;
    let address = String::from_utf8(output.stdout.clone())?.trim().to_owned();
    PlanFixture::success(output);
    let output = Command::new("sh")
        .args(["-c", "command -v regctl"])
        .output()?;
    let regctl = String::from_utf8(output.stdout.clone())?.trim().to_owned();
    PlanFixture::success(output);
    for name in ["skopeo", "umoci", "docker"] {
        fs::remove_file(fixture.root().join("bin").join(name))?;
    }
    write_tool(
        &fixture.root().join("bin/regctl"),
        r#"#!/bin/sh
set -eu
case "$3" in ghcr.io/mag1cfrog/delta-arrow-reader:*|ghcr.io/mag1cfrog/delta-arrow-reader@sha256:*) ;; *) exit 96 ;; esac
first=$1; second=$2; suffix=${3#ghcr.io/mag1cfrog/delta-arrow-reader}; shift 3
stderr=$(mktemp)
trap 'rm -f "$stderr"' EXIT
if "$CLI_REAL_REGCTL" "$first" "$second" "$CLI_REGISTRY/dar$suffix" "$@" --host "reg=$CLI_REGISTRY,tls=disabled" 2> "$stderr"; then status=0; else status=$?; fi
sed "s|$CLI_REGISTRY/dar|ghcr.io/mag1cfrog/delta-arrow-reader|g" "$stderr" >&2
exit "$status"
"#,
    )?;
    let run = |mut command: Command| -> Result<Output> {
        Ok(command
            .env("CLI_REAL_REGCTL", &regctl)
            .env("CLI_REGISTRY", &address)
            .output()?)
    };
    // No real GitHub requests or GHCR writes are possible through either test transport.
    let mut dry = fixture.upload()?;
    dry.arg("--dry-run");
    PlanFixture::success(run(dry)?);
    PlanFixture::success(run(fixture.upload()?)?);
    PlanFixture::success(run(fixture.upload()?)?);
    PlanFixture::success(run(fixture.verify()?)?);
    for tag in TAGS {
        let output = Command::new(&regctl)
            .args([
                "manifest",
                "get",
                &format!("{address}/dar:{tag}"),
                "--format",
                "raw-body",
                "--host",
                &format!("reg={address},tls=disabled"),
            ])
            .output()?;
        PlanFixture::success(output.clone());
        let expected = if tag == "v1.2.3" {
            "expected/index.json".into()
        } else {
            format!(
                "manifest-{}.json",
                tag.strip_prefix("v1.2.3-linux-").unwrap()
            )
        };
        assert_eq!(output.stdout, fs::read(fixture.root().join(expected))?);
    }
    assert!(!fixture.root().join("forbidden-tool").exists());
    Ok(())
}
