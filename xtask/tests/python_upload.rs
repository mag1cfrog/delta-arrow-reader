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
use support::{Result, Wheels};

const EXISTING_OUTPUT: &str = "existing=kept\n";

struct Upload {
    directory: Wheels,
    artifacts: PathBuf,
    wheels: Vec<PathBuf>,
}

impl Upload {
    fn new(name: &str) -> Result<Self> {
        let directory = Wheels::new(name)?;
        let (artifacts, wheels) = directory.artifact_set()?;
        let curl = directory.0.join("curl");
        fs::write(
            &curl,
            r#"#!/bin/sh
printf '%s\0' "$@" > "$CURL_ARGUMENTS"
[ "$CURL_EXIT" = 0 ] || exit "$CURL_EXIT"
printf '%s\n%s' "$CURL_BODY" "$CURL_STATUS"
"#,
        )?;
        fs::set_permissions(curl, fs::Permissions::from_mode(0o755))?;
        fs::write(directory.0.join("output"), EXISTING_OUTPUT)?;
        Ok(Self {
            directory,
            artifacts,
            wheels,
        })
    }

    fn command(&self, index: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        command
            .arg("prepare-python-upload")
            .arg(&self.artifacts)
            .args([
                "--expected-version",
                "1.2.3",
                "--index",
                index,
                "--destination",
            ])
            .arg(self.directory.0.join("dist"))
            .env("PATH", &self.directory.0)
            .env("CURL_ARGUMENTS", self.directory.0.join("curl-arguments"))
            .env("CURL_EXIT", "0")
            .env("CURL_STATUS", "200")
            .env("CURL_BODY", self.release(0).unwrap().to_string())
            .env("GITHUB_OUTPUT", self.directory.0.join("output"));
        command
    }

    fn release(&self, published: usize) -> Result<Value> {
        let mut files = Vec::new();
        for (index, path) in self.wheels.iter().enumerate() {
            if published & (1 << index) != 0 {
                files.push(json!({
                    "filename": path.file_name().unwrap().to_str().unwrap(),
                    "digests": {"sha256": format!("{:x}", Sha256::digest(fs::read(path)?))},
                    "yanked": false,
                }));
            }
        }
        Ok(json!({
            "info": {"name": "delta-arrow-reader", "version": "1.2.3"},
            "urls": files,
        }))
    }

    fn assert_staged(&self, published: usize) -> Result<()> {
        let destination = self.directory.0.join("dist");
        let mut expected = Vec::new();
        for (index, path) in self.wheels.iter().enumerate() {
            let source = fs::read(path)?;
            if published & (1 << index) == 0 {
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
            fs::read_to_string(self.directory.0.join("output"))?,
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
        assert!(!self.directory.0.join("dist").exists());
        assert_eq!(
            fs::read_to_string(self.directory.0.join("output"))?,
            EXISTING_OUTPUT,
        );
        Ok(())
    }
}

#[test]
fn stages_exactly_the_missing_wheels_for_every_published_subset() -> Result<()> {
    for index in ["pypi", "testpypi"] {
        for published in 0..16 {
            let upload = Upload::new(&format!("upload-{index}-{published}"))?;
            let mut release = upload.release(published)?;
            release["info"]["name"] = json!("Delta._-Arrow.Reader");
            let output = upload
                .command(index)
                .env("CURL_BODY", release.to_string())
                .output()?;
            assert!(output.status.success(), "{output:?}");
            upload.assert_staged(published)?;
        }
    }
    Ok(())
}

#[test]
fn missing_release_queries_the_selected_index_and_stages_all_wheels() -> Result<()> {
    for (index, host) in [("pypi", "pypi.org"), ("testpypi", "test.pypi.org")] {
        let upload = Upload::new(&format!("upload-missing-{index}"))?;
        let output = upload
            .command(index)
            .env("CURL_STATUS", "404")
            .env("CURL_BODY", "not JSON")
            .output()?;
        assert!(output.status.success(), "{output:?}");
        upload.assert_staged(0)?;
        let arguments = fs::read_to_string(upload.directory.0.join("curl-arguments"))?;
        let arguments: Vec<_> = arguments.split_terminator('\0').collect();
        assert_eq!(arguments[0], "--disable");
        for pair in [
            [
                "--url",
                &format!("https://{host}/pypi/delta-arrow-reader/1.2.3/json"),
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
    let upload = Upload::new("upload-changed")?;
    for index in 0..4 {
        let mut release = upload.release(15)?;
        release["urls"][index]["digests"]["sha256"] = json!("0".repeat(64));
        upload.assert_rejected(
            upload
                .command("pypi")
                .env("CURL_BODY", release.to_string())
                .output()?,
            "changed binaries need a new version",
        )?;
    }
    let mut release = upload.release(1)?;
    release["urls"][0]["filename"] = json!("extra.whl");
    upload.assert_rejected(
        upload
            .command("pypi")
            .env("CURL_BODY", release.to_string())
            .output()?,
        "unexpected published file: extra.whl",
    )?;
    Ok(())
}

#[test]
fn invalid_index_metadata_never_enables_an_upload() -> Result<()> {
    let upload = Upload::new("upload-invalid-metadata")?;
    let valid = upload.release(1)?;
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
    let upload = Upload::new("upload-network-errors")?;
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
    let upload = Upload::new("upload-local-inputs")?;
    upload.assert_rejected(
        upload.command("another-index").output()?,
        "index must be pypi or testpypi",
    )?;
    fs::remove_file(&upload.wheels[0])?;
    upload.assert_rejected(upload.command("pypi").output()?, "expected one wheel")?;
    assert!(!upload.directory.0.join("curl-arguments").exists());
    Ok(())
}

#[test]
fn an_existing_destination_is_preserved_and_does_not_enable_uploads() -> Result<()> {
    let upload = Upload::new("upload-existing-destination")?;
    let destination = upload.directory.0.join("dist");
    fs::create_dir(&destination)?;
    let stale = destination.join("unvalidated.whl");
    fs::write(&stale, "stale")?;
    let output = upload.command("pypi").output()?;
    assert!(!output.status.success(), "{output:?}");
    assert_eq!(fs::read_dir(destination)?.count(), 1);
    assert_eq!(fs::read_to_string(stale)?, "stale");
    assert_eq!(
        fs::read_to_string(upload.directory.0.join("output"))?,
        EXISTING_OUTPUT
    );
    Ok(())
}
