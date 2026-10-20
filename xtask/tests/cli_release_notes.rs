#![cfg(target_os = "linux")]

use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

use serde_json::{Value, json};

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

const START: &str = "<!-- dar-cli:start -->";
const END: &str = "<!-- dar-cli:end -->";

struct NotesFixture {
    plan: PlanFixture,
    plan_hash: String,
}

impl NotesFixture {
    fn new() -> Result<Self> {
        let plan = PlanFixture::new(false)?;
        PlanFixture::success(plan.command()?.output()?);
        let root = plan.selection.directory.path();
        let plan_hash = sha256(&fs::read(root.join("plan/release-plan.json"))?);
        fs::create_dir(root.join("remote"))?;
        fs::write(
            root.join("remote/release.json"),
            serde_json::to_vec(&json!({
                "name":"Original release title", "body":"Rust changelog\r\n\r\nPython wheel notes\n"
            }))?,
        )?;
        fs::rename(root.join("bin/gh"), root.join("bin/gh-readonly"))?;
        for (name, script) in [
            (
                "gh",
                r#"#!/bin/sh
set -eu
[ "$1" = api ] && [ "$2" = --hostname ] && [ "$3" = github.com ] && [ "$4" = --method ] || exit 97
method=$5
endpoint=
input=
next=false
for arg do
  if [ "$next" = true ]; then input=$arg; next=false; fi
  case "$arg" in repos/mag1cfrog/delta-arrow-reader/*) endpoint=$arg ;; --input) next=true ;; esac
done
remote=$CLI_TEST_API/remote
case "$method:$endpoint" in
  GET:repos/mag1cfrog/delta-arrow-reader/releases/7)
    cat "$remote/release.json"
    if [ "${CLI_NOTES_EDIT_BODY:-}" = true ]; then
      jq '.body += "Another maintainer edit\n"' "$remote/release.json" > "$remote/next.json"
      mv "$remote/next.json" "$remote/release.json"
    fi
    if [ "${CLI_NOTES_MOVE_TAG:-}" = true ]; then
      printf '%s' '{"object":{"type":"commit","sha":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}}' > "$CLI_TEST_API/responses/repos/mag1cfrog/delta-arrow-reader/git/ref/tags/v1.2.3.body"
    fi
    ;;
  'GET:repos/mag1cfrog/delta-arrow-reader/releases/7/assets?per_page=100')
    if [ "${CLI_NOTES_ASSET_EXISTS:-}" = true ]; then
      jq '[[.github_assets[0] | {id:2000, name:.name, size:.size, digest:("sha256:" + .sha256), state:"uploaded"}]]' "$CLI_TEST_API/plan/release-plan.json"
    else printf '[[]]'; fi ;;
  GET:repos/mag1cfrog/delta-arrow-reader/releases/assets/2000)
    cat "$CLI_TEST_API/plan/binaries/dar-v1.2.3-linux-amd64-gnu.tar.gz" ;;
  PATCH:repos/mag1cfrog/delta-arrow-reader/releases/7)
    jq -e 'keys == ["body"]' "$input" > /dev/null
    printf patch\\n >> "$remote/writes"
    [ "${CLI_NOTES_WRITE_FAILURE:-}" != true ] || exit 88
    jq --slurpfile request "$input" '.body = $request[0].body' "$remote/release.json" > "$remote/next.json"
    mv "$remote/next.json" "$remote/release.json"
    if [ "${CLI_NOTES_BAD_RESPONSE:-}" = true ]; then printf '{"body":"incorrect"}'; else cat "$remote/release.json"; fi ;;
  GET:*) exec "$CLI_TEST_API/bin/gh-readonly" "$@" ;;
  *) exit 96 ;;
esac
"#,
            ),
            (
                "regctl",
                r#"#!/bin/sh
set -eu
[ "$1" = manifest ] && [ "$2" = get ] && [ "$4" = --format ] && [ "$5" = raw-body ] || exit 96
reference=$3
if [ "${CLI_NOTES_IMAGE_EXISTS:-}" = true ] && [ "$reference" = ghcr.io/mag1cfrog/delta-arrow-reader:v1.2.3-linux-arm64 ]; then
  cat "$CLI_TEST_API/manifest-arm64.json"; exit 0
fi
if [ "${CLI_NOTES_REGISTRY_FAILURE:-}" = true ]; then printf 'registry unavailable\n' >&2; else
  printf '%s\n' "failed to get manifest $reference: request failed: not found [http 404]" >&2
fi
exit 1
"#,
            ),
        ] {
            let path = root.join("bin").join(name);
            fs::write(&path, script)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
        }
        Ok(Self { plan, plan_hash })
    }
    fn root(&self) -> &Path {
        self.plan.selection.directory.path()
    }
    fn command(&self, status: &str) -> Result<Command> {
        let mut command = self.plan.selection.xtask()?;
        command
            .args([
                "cli",
                "update-release-notes",
                "plan",
                "--plan-sha256",
                &self.plan_hash,
                "--status",
                status,
            ])
            .env_remove("GITHUB_ACTIONS");
        Ok(command)
    }
    fn body(&self) -> Result<String> {
        let release: Value =
            serde_json::from_slice(&fs::read(self.root().join("remote/release.json"))?)?;
        assert_eq!(release["name"], "Original release title");
        Ok(release["body"].as_str().unwrap_or_default().into())
    }
    fn set_body(&self, body: Value) -> Result<()> {
        Ok(fs::write(
            self.root().join("remote/release.json"),
            serde_json::to_vec(&json!({"name":"Original release title","body":body}))?,
        )?)
    }
    fn writes(&self) -> Result<usize> {
        let path = self.root().join("remote/writes");
        Ok(if path.exists() {
            fs::read_to_string(path)?.lines().count()
        } else {
            0
        })
    }
    fn old_section(&self, status: &str) -> Result<()> {
        self.set_body(json!(format!("Original prose\n{START}\n<!-- dar-cli-state {} -->\nOld CLI section\n{END}\nOriginal ending\n",
            json!({"status":status,"release_commit":"b".repeat(40),"validation_run_id":700,"record_artifact_id":99}))))
    }
}

#[test]
fn preserves_other_notes_and_title_and_updates_only_the_cli_section() -> Result<()> {
    let fixture = NotesFixture::new()?;
    let original = fixture.body()?;
    PlanFixture::success(fixture.command("pending")?.output()?);
    let pending = fixture.body()?;
    assert!(pending.starts_with(&original));
    assert!(pending.contains("publication is incomplete"));
    assert_eq!(pending.matches(START).count(), 1);
    for text in [
        "v1.2.3",
        "glibc 2.36",
        "linux/amd64",
        "linux/arm64",
        "actions/runs/701",
        "record artifact `901`",
        &"a".repeat(40),
        &"b".repeat(40),
    ] {
        assert!(pending.contains(text), "missing {text}");
    }
    for (name, bytes) in &fixture.plan.binaries {
        assert!(pending.contains(name));
        assert!(pending.contains(&sha256(bytes)));
    }
    PlanFixture::success(fixture.command("pending")?.output()?);
    assert_eq!(fixture.writes()?, 1);
    fixture.set_body(json!(format!(
        "{pending}More Python notes after the CLI section\r\n"
    )))?;
    PlanFixture::success(fixture.command("complete")?.output()?);
    let completed = fixture.body()?;
    assert!(completed.starts_with(&original));
    assert!(completed.ends_with("More Python notes after the CLI section\r\n"));
    assert!(completed.contains("passed download verification"));
    assert!(!completed.contains("publication is incomplete"));
    for status in ["complete", "pending"] {
        PlanFixture::success(fixture.command(status)?.output()?);
    }
    assert_eq!(fixture.writes()?, 2);
    assert_eq!(fixture.body()?, completed);
    Ok(())
}

#[test]
fn dry_run_and_empty_bodies_do_not_replace_other_release_fields() -> Result<()> {
    let fixture = NotesFixture::new()?;
    PlanFixture::success(fixture.command("pending")?.arg("--dry-run").output()?);
    assert_eq!(fixture.writes()?, 0);
    fixture.set_body(Value::Null)?;
    PlanFixture::success(fixture.command("pending")?.output()?);
    assert!(fixture.body()?.starts_with(START));
    Ok(())
}

#[test]
fn untrusted_contexts_and_changed_plans_cannot_edit_notes() -> Result<()> {
    for kind in ["workflow", "plan"] {
        let fixture = NotesFixture::new()?;
        let original = fixture.body()?;
        let mut command = fixture.command("pending")?;
        if kind == "workflow" {
            command
                .env("GITHUB_ACTIONS", "true")
                .env("GITHUB_EVENT_NAME", "pull_request");
        } else {
            fs::write(fixture.root().join("plan/release-plan.json"), "{}")?;
        }
        let output = command.output()?;
        assert!(!output.status.success(), "{kind}: {output:?}");
        assert_eq!(fixture.writes()?, 0);
        assert_eq!(fixture.body()?, original);
    }
    Ok(())
}

#[test]
fn malformed_or_foreign_sections_fail_without_changing_the_release() -> Result<()> {
    let fixture = NotesFixture::new()?;
    for body in [
        format!("{START} orphan"),
        format!("{END}{START}"),
        format!("{START}{START}{END}"),
        format!("{START} no identity {END}"),
        format!("{START}\n<!-- dar-cli-state invalid -->\n{END}"),
        format!(
            "{START}\n<!-- dar-cli-state {} -->\n{END}",
            json!({"status":"pending","release_commit":"e".repeat(40),"validation_run_id":701,"record_artifact_id":100})
        ),
    ] {
        fixture.set_body(json!(body))?;
        let output = fixture.command("pending")?.output()?;
        assert!(!output.status.success(), "{output:?}");
        assert_eq!(fixture.body()?, body);
        assert_eq!(fixture.writes()?, 0);
    }
    Ok(())
}

#[test]
fn a_new_record_can_replace_a_pending_selection_only_before_any_bytes_are_published() -> Result<()>
{
    for (variable, expected) in [
        ("none", true),
        ("CLI_NOTES_ASSET_EXISTS", false),
        ("CLI_NOTES_IMAGE_EXISTS", false),
        ("CLI_NOTES_REGISTRY_FAILURE", false),
    ] {
        let fixture = NotesFixture::new()?;
        fixture.old_section("pending")?;
        let old = fixture.body()?;
        let output = fixture.command("pending")?.env(variable, "true").output()?;
        assert_eq!(output.status.success(), expected, "{variable}: {output:?}");
        assert_eq!(fixture.writes()?, usize::from(expected));
        if !expected {
            assert_eq!(fixture.body()?, old);
        }
    }
    let fixture = NotesFixture::new()?;
    fixture.old_section("complete")?;
    let old = fixture.body()?;
    for status in ["pending", "complete"] {
        let output = fixture.command(status)?.output()?;
        assert!(!output.status.success(), "{output:?}");
        assert_eq!(fixture.body()?, old);
        assert_eq!(fixture.writes()?, 0);
    }
    Ok(())
}

#[test]
fn concurrent_body_edits_tag_moves_and_write_errors_cannot_report_success() -> Result<()> {
    for (variable, writes) in [
        ("CLI_NOTES_EDIT_BODY", 0),
        ("CLI_NOTES_MOVE_TAG", 0),
        ("CLI_NOTES_WRITE_FAILURE", 1),
        ("CLI_NOTES_BAD_RESPONSE", 1),
    ] {
        let fixture = NotesFixture::new()?;
        let output = fixture.command("pending")?.env(variable, "true").output()?;
        assert!(!output.status.success(), "{variable}: {output:?}");
        assert_eq!(fixture.writes()?, writes);
        if variable == "CLI_NOTES_EDIT_BODY" {
            assert!(fixture.body()?.contains("Another maintainer edit"));
        }
    }
    Ok(())
}
