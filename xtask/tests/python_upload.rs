//! Python package upload validation against a controlled index fixture.

#![allow(clippy::unwrap_used)]
#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output},
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

mod support;
use support::{PLATFORMS, Result, WheelFixture, cli_wheel_files, write_wheel};

const EXISTING_OUTPUT: &str = "existing=kept\n";

struct UploadFixture {
    fixture: WheelFixture,
    artifacts: PathBuf,
    wheel_paths: Vec<PathBuf>,
}

impl UploadFixture {
    fn new(name: &str) -> Result<Self> {
        let fixture = WheelFixture::new(name)?;
        let (artifacts, wheel_paths) = fixture.create_artifacts()?;
        let curl = fixture.directory.join("curl");
        fs::write(
            &curl,
            r#"#!/bin/sh
printf '%s\0' "$@" >> "$CURL_ARGUMENTS"
case "$*" in
  */pypi/delta-arrow-reader/*) [ "${CURL_CLI_EXIT:-0}" = 0 ] || exit "$CURL_CLI_EXIT"; printf '%s\n%s' "$CURL_CLI_BODY" "${CURL_CLI_STATUS:-404}"; exit 0 ;;
esac
[ "$CURL_EXIT" = 0 ] || exit "$CURL_EXIT"
printf '%s\n%s' "$CURL_BODY" "$CURL_STATUS"
"#,
        )?;
        fs::set_permissions(curl, fs::Permissions::from_mode(0o755))?;
        fs::write(fixture.directory.join("output"), EXISTING_OUTPUT)?;
        Ok(Self {
            fixture,
            artifacts,
            wheel_paths,
        })
    }

    fn command(&self, index: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        command
            .arg("stage-python-wheels")
            .arg(&self.artifacts)
            .args([
                "--expected-version",
                "1.2.3",
                "--index",
                index,
                "--destination",
            ])
            .arg(self.fixture.directory.join("dist"))
            .env("PATH", &self.fixture.directory)
            .env(
                "CURL_ARGUMENTS",
                self.fixture.directory.join("curl-arguments"),
            )
            .env("CURL_EXIT", "0")
            .env("CURL_STATUS", "200")
            .env("CURL_CLI_STATUS", "404")
            .env("CURL_CLI_BODY", "not published")
            .env(
                "CURL_BODY",
                self.index_response(0, false).unwrap().to_string(),
            )
            .env("GITHUB_OUTPUT", self.fixture.directory.join("output"));
        command
    }

    fn index_response(&self, published_mask: usize, cli: bool) -> Result<Value> {
        let mut files = Vec::new();
        // Each bit selects one platform wheel already present on the index.
        for (index, path) in self.wheel_paths.iter().enumerate() {
            if path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("delta_arrow_reader-")
                != cli
            {
                continue;
            }
            if published_mask & (1 << index) != 0 {
                files.push(json!({
                    "filename": path.file_name().unwrap().to_str().unwrap(),
                    "digests": {"sha256": format!("{:x}", Sha256::digest(fs::read(path)?))},
                    "yanked": false,
                }));
            }
        }
        Ok(json!({
            "info": {"name": if cli { "delta-arrow-reader" } else { "delta-arrow-reader-py" }, "version": "1.2.3"},
            "urls": files,
        }))
    }

    fn assert_staged(&self, published_mask: usize) -> Result<()> {
        let destination = self.fixture.directory.join("dist");
        let mut expected = Vec::new();
        for (index, path) in self.wheel_paths.iter().enumerate() {
            let source = fs::read(path)?;
            if published_mask & (1 << index) == 0 {
                let name = path.file_name().unwrap();
                expected.push(name.to_owned());
                assert_eq!(fs::read(destination.join(name))?, source);
            }
        }
        expected.sort();
        let mut actual = fs::read_dir(destination)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        actual.sort();
        assert_eq!(actual, expected);
        assert_eq!(
            fs::read_to_string(self.fixture.directory.join("output"))?,
            format!("{EXISTING_OUTPUT}has-files={}\n", !expected.is_empty()),
        );
        Ok(())
    }

    fn assert_rejected(&self, output: Output, message: &str) -> Result<()> {
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{output:?}",
        );
        assert!(!self.fixture.directory.join("dist").exists());
        assert_eq!(
            fs::read_to_string(self.fixture.directory.join("output"))?,
            EXISTING_OUTPUT,
        );
        Ok(())
    }
}

#[test]
fn stages_exactly_the_missing_wheels_for_every_published_subset() -> Result<()> {
    for index in ["pypi", "testpypi"] {
        for published_mask in 0..(1 << (2 * PLATFORMS.len())) {
            let upload = UploadFixture::new(&format!("upload-{index}-{published_mask}"))?;
            let mut release = upload.index_response(published_mask, false)?;
            release["info"]["name"] = json!("Delta._-Arrow.Reader.Py");
            let output = upload
                .command(index)
                .env("CURL_BODY", release.to_string())
                .env("CURL_CLI_STATUS", "200")
                .env(
                    "CURL_CLI_BODY",
                    upload.index_response(published_mask, true)?.to_string(),
                )
                .output()?;
            assert!(output.status.success(), "{output:?}");
            upload.assert_staged(published_mask)?;
        }
    }
    Ok(())
}

#[test]
fn missing_release_queries_the_selected_index_and_stages_all_wheels() -> Result<()> {
    for (index, host) in [("pypi", "pypi.org"), ("testpypi", "test.pypi.org")] {
        let upload = UploadFixture::new(&format!("upload-missing-{index}"))?;
        let output = upload
            .command(index)
            .env("CURL_STATUS", "404")
            .env("CURL_BODY", "not JSON")
            .output()?;
        assert!(output.status.success(), "{output:?}");
        upload.assert_staged(0)?;
        let arguments = fs::read_to_string(upload.fixture.directory.join("curl-arguments"))?;
        let arguments: Vec<_> = arguments.split_terminator('\0').collect();
        assert_eq!(arguments[0], "--disable");
        for pair in [
            [
                "--url",
                &format!("https://{host}/pypi/delta-arrow-reader-py/1.2.3/json"),
            ],
            ["--max-time", "30"],
            ["--proto", "=https"],
            ["--proto-redir", "=https"],
            ["--header", "Accept: application/json"],
            ["--header", "Cache-Control: no-cache"],
        ] {
            assert!(
                arguments.windows(2).any(|args| args == pair),
                "{arguments:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn changed_or_unexpected_published_files_stop_before_staging() -> Result<()> {
    let upload = UploadFixture::new("upload-changed")?;
    for index in 0..PLATFORMS.len() {
        let mut release = upload.index_response((1 << upload.wheel_paths.len()) - 1, false)?;
        release["urls"][index]["digests"]["sha256"] = json!("0".repeat(64));
        upload.assert_rejected(
            upload
                .command("pypi")
                .env("CURL_BODY", release.to_string())
                .output()?,
            "changed binaries need a new version",
        )?;
    }
    let mut release = upload.index_response(1, false)?;
    release["urls"][0]["filename"] = json!("extra.whl");
    upload.assert_rejected(
        upload
            .command("pypi")
            .env("CURL_BODY", release.to_string())
            .output()?,
        "unexpected published file",
    )?;
    Ok(())
}

#[test]
fn invalid_index_metadata_never_enables_an_upload() -> Result<()> {
    let upload = UploadFixture::new("upload-invalid-metadata")?;
    let valid = upload.index_response(1, false)?;
    let mut invalid = vec![json!(null), json!([]), json!({})];
    for (pointer, value) in [
        ("/info", json!(null)),
        ("/info/name", json!("other-package")),
        ("/info/name", json!("-delta-arrow-reader")),
        ("/info/version", json!("1.2.4")),
        ("/urls", json!({})),
        ("/urls", json!(null)),
        ("/urls", json!([valid["urls"][0], valid["urls"][0]])),
        ("/urls/0", json!({})),
        ("/urls/0/filename", json!(null)),
        ("/urls/0/filename", json!("")),
        ("/urls/0/digests", json!({})),
        ("/urls/0/digests/sha256", json!("invalid")),
        ("/urls/0/digests/sha256", json!(null)),
        ("/urls/0/digests/sha256", json!("A".repeat(64))),
        ("/urls/0/yanked", json!(true)),
        ("/urls/0/yanked", json!(null)),
        ("/urls/0/yanked", json!(0)),
    ] {
        let mut release = valid.clone();
        *release.pointer_mut(pointer).unwrap() = value;
        invalid.push(release);
    }
    for release in invalid {
        upload.assert_rejected(
            upload
                .command("pypi")
                .env("CURL_BODY", release.to_string())
                .output()?,
            "",
        )?;
    }
    Ok(())
}

#[test]
fn index_and_transport_failures_are_not_treated_as_missing_releases() -> Result<()> {
    let upload = UploadFixture::new("upload-network-errors")?;
    for status in ["000", "204", "301", "403", "429", "503"] {
        upload.assert_rejected(
            upload.command("pypi").env("CURL_STATUS", status).output()?,
            &format!("package index returned HTTP {status}"),
        )?;
    }
    for exit in ["6", "28"] {
        upload.assert_rejected(
            upload
                .command("pypi")
                .env("CURL_EXIT", exit)
                .env("CURL_STATUS", "404")
                .output()?,
            "curl failed",
        )?;
    }
    for body in ["not JSON", "", "{\"info\":"] {
        upload.assert_rejected(upload.command("pypi").env("CURL_BODY", body).output()?, "")?;
    }
    Ok(())
}

#[test]
fn invalid_local_inputs_stop_before_contacting_the_index() -> Result<()> {
    let upload = UploadFixture::new("upload-local-inputs")?;
    upload.assert_rejected(
        upload.command("another-index").output()?,
        "index must be pypi or testpypi",
    )?;
    fs::remove_file(&upload.wheel_paths[0])?;
    upload.assert_rejected(
        upload.command("pypi").output()?,
        "expected Python and CLI wheels",
    )?;
    assert!(!upload.fixture.directory.join("curl-arguments").exists());
    Ok(())
}

#[test]
fn cli_index_failures_do_not_stage_either_distribution() -> Result<()> {
    let upload = UploadFixture::new("upload-cli-index-errors")?;
    for position in 0..PLATFORMS.len() {
        let mut release = upload.index_response(63, true)?;
        release["urls"][position]["digests"]["sha256"] = json!("0".repeat(64));
        upload.assert_rejected(
            upload
                .command("pypi")
                .env("CURL_CLI_STATUS", "200")
                .env("CURL_CLI_BODY", release.to_string())
                .output()?,
            "changed binaries need a new version",
        )?;
    }
    for status in ["403", "503"] {
        upload.assert_rejected(
            upload
                .command("pypi")
                .env("CURL_CLI_STATUS", status)
                .output()?,
            &format!("package index returned HTTP {status}"),
        )?;
    }
    upload.assert_rejected(
        upload.command("pypi").env("CURL_CLI_EXIT", "28").output()?,
        "curl failed",
    )?;
    let python_metadata = upload.index_response(0, false)?;
    upload.assert_rejected(
        upload
            .command("pypi")
            .env("CURL_CLI_STATUS", "200")
            .env("CURL_CLI_BODY", python_metadata.to_string())
            .output()?,
        "index response does not match the requested package and version",
    )?;
    Ok(())
}

#[test]
fn wheels_without_a_cli_cannot_reach_either_package_index() -> Result<()> {
    for (position, (platform, tag)) in PLATFORMS.into_iter().enumerate() {
        for index in ["pypi", "testpypi"] {
            let upload = UploadFixture::new(&format!("upload-missing-cli-{platform}-{index}"))?;
            let mut files = cli_wheel_files(tag);
            files.retain(|(name, _)| !name.contains(".data/scripts/"));
            write_wheel(
                &upload.wheel_paths[position + PLATFORMS.len()],
                &files,
                true,
            )?;
            upload.assert_rejected(
                upload.command(index).output()?,
                "missing required wheel file: delta_arrow_reader-1.2.3.data/scripts/dar",
            )?;
            assert!(!upload.fixture.directory.join("curl-arguments").exists());
        }
    }
    Ok(())
}

#[test]
fn an_existing_destination_is_preserved_and_does_not_enable_uploads() -> Result<()> {
    let upload = UploadFixture::new("upload-existing-destination")?;
    let destination = upload.fixture.directory.join("dist");
    fs::create_dir(&destination)?;
    let stale = destination.join("unvalidated.whl");
    fs::write(&stale, "stale")?;
    let output = upload.command("pypi").output()?;
    assert!(!output.status.success(), "{output:?}");
    assert_eq!(fs::read_dir(destination)?.count(), 1);
    assert_eq!(fs::read_to_string(stale)?, "stale");
    assert_eq!(
        fs::read_to_string(upload.fixture.directory.join("output"))?,
        EXISTING_OUTPUT
    );
    Ok(())
}
