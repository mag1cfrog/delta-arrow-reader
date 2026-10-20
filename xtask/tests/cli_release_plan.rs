#![cfg(target_os = "linux")]

use serde_json::{Value, json};
use std::fs;

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
use release_selection_fixture::ARTIFACTS;

#[test]
fn plans_exact_validated_bytes_and_version_pinned_destinations() -> Result<()> {
    let fixture = PlanFixture::new(false)?;
    let output = fixture.command()?.output()?;
    let plan = fs::read(
        fixture
            .selection
            .directory
            .path()
            .join("plan/release-plan.json"),
    )?;
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains(&format!("Release plan SHA256: {}", sha256(&plan)))
    );
    PlanFixture::success(output);
    fixture.check_plan()?;
    let calls = fs::read_to_string(fixture.selection.directory.path().join("calls"))?;
    assert_eq!(calls.matches("actions/artifacts/101/zip").count(), 1);
    assert_eq!(calls.matches("actions/artifacts/102/zip").count(), 1);
    assert!(calls.lines().all(|line| line.contains("--method GET")));
    let calls = fs::read_to_string(fixture.selection.directory.path().join("container-calls"))?;
    assert!(!calls.contains("docker:") && !calls.contains("build") && !calls.contains("push"));
    Ok(())
}

#[test]
fn rejects_changed_downloads_and_manifest_hashes_before_oci_tools() -> Result<()> {
    for id in [101, 102] {
        let mut fixture = PlanFixture::new(false)?;
        fixture
            .selection
            .raw
            .get_mut(&format!("actions/artifacts/{id}/zip"))
            .unwrap()[0] ^= 1;
        fixture.rejected(fixture.command()?.output()?, "SHA256 differs")?;
        assert!(
            !fixture
                .selection
                .directory
                .path()
                .join("container-calls")
                .exists()
        );
    }
    for field in ["binary_artifact", "image_artifact"] {
        let mut fixture = PlanFixture::new(false)?;
        fixture.selection.record[field]["manifest_sha256"] = json!("0".repeat(64));
        fixture.selection.pack_record(true)?;
        fixture.rejected(fixture.command()?.output()?, "approved validation record")?;
    }
    Ok(())
}

#[test]
fn rejects_missing_extra_duplicate_and_unsafe_zip_entries() -> Result<()> {
    for (case, message) in [
        ("missing", "incomplete or extra file inventory"),
        ("extra", "incomplete or extra file inventory"),
        ("duplicate", "unexpected, duplicate"),
        ("unsafe", "unexpected, duplicate"),
        ("archive", "unexpected end of file"),
    ] {
        let mut fixture = PlanFixture::new(false)?;
        match case {
            "missing" => {
                fixture.binaries.pop();
            }
            "extra" => fixture.binaries.push(("unexpected".into(), vec![0])),
            "duplicate" => fixture.binaries[1] = fixture.binaries[0].clone(),
            "unsafe" => fixture.binaries[0].0 = "../escaped".into(),
            "archive" => fixture
                .binaries
                .iter_mut()
                .find(|(name, _)| name.ends_with(".tar.gz"))
                .unwrap()
                .1
                .push(1),
            _ => unreachable!(),
        }
        fixture.update_downloads()?;
        fixture.rejected(fixture.command()?.output()?, message)?;
        assert!(!fixture.selection.directory.path().join("escaped").exists());
    }
    Ok(())
}

#[test]
fn rejects_zip_links_encryption_inconsistent_headers_and_bad_crc() -> Result<()> {
    for (case, message) in [
        ("symlink", "non-regular entry"),
        ("encrypted", "encrypted"),
        ("local name", "local header differs"),
        ("crc", "Invalid checksum"),
    ] {
        let mut fixture = PlanFixture::new(false)?;
        let bytes = fixture
            .selection
            .raw
            .get_mut("actions/artifacts/102/zip")
            .unwrap();
        let central = bytes
            .windows(4)
            .position(|value| value == b"PK\x01\x02")
            .unwrap();
        match case {
            "symlink" => {
                bytes[central + 5] = 3; // Unix creator, with symlink type in external attributes.
                bytes[central + 38..central + 42]
                    .copy_from_slice(&(0o120777u32 << 16).to_le_bytes());
            }
            "encrypted" => bytes[central + 8] |= 1,
            "local name" => bytes[30] ^= 1,
            "crc" => {
                let name_length = u16::from_le_bytes(bytes[26..28].try_into()?) as usize;
                let extra_length = u16::from_le_bytes(bytes[28..30].try_into()?) as usize;
                bytes[30 + name_length + extra_length] ^= 1;
            }
            _ => unreachable!(),
        }
        // Authenticate this malformed ZIP so the extraction checks must reject it.
        fixture.selection.responses.get_mut(ARTIFACTS).unwrap()[1]["artifacts"][2]["digest"] =
            json!(format!("sha256:{}", sha256(bytes)));
        fixture.rejected(fixture.command()?.output()?, message)?;
    }
    Ok(())
}

#[test]
fn rejects_mixed_manifests_and_damaged_image_executables() -> Result<()> {
    for (field, value) in [
        ("source_commit", json!("e".repeat(40))),
        ("version", json!("1.2.4")),
        ("rust_toolchain", json!("1.94.0")),
        ("targets", json!([])),
    ] {
        let mut fixture = PlanFixture::new(false)?;
        let bytes = &mut fixture
            .images
            .iter_mut()
            .find(|(name, _)| name == "cli-images.json")
            .unwrap()
            .1;
        let mut manifest: Value = serde_json::from_slice(bytes)?;
        manifest[field] = value;
        *bytes = serde_json::to_vec(&manifest)?;
        fixture.update_downloads()?;
        fixture.rejected(
            fixture.command()?.output()?,
            if field == "targets" {
                "incomplete image target list"
            } else {
                "image manifest build identity differs"
            },
        )?;
    }
    let fixture = PlanFixture::new(true)?;
    fixture.rejected(fixture.command()?.output()?, "image dar differs")?;
    Ok(())
}

#[test]
fn propagates_tool_and_download_errors_without_a_completed_plan() -> Result<()> {
    for tool in ["skopeo", "umoci"] {
        let fixture = PlanFixture::new(false)?;
        fixture.rejected(
            fixture
                .command()?
                .env("CLI_PLAN_TOOL_FAILURE", tool)
                .output()?,
            &format!("{tool} failed"),
        )?;
    }
    let fixture = PlanFixture::new(false)?;
    fixture.rejected(
        fixture
            .command()?
            .env(
                "CLI_TEST_FAIL_ENDPOINT",
                "repos/mag1cfrog/delta-arrow-reader/actions/artifacts/102/zip",
            )
            .output()?,
        "gh failed",
    )?;
    Ok(())
}

#[test]
fn refuses_existing_output_and_a_tag_changed_during_verification() -> Result<()> {
    let fixture = PlanFixture::new(false)?;
    let mut command = fixture.command()?;
    let tag = fixture
        .selection
        .directory
        .path()
        .join("responses/repos/mag1cfrog/delta-arrow-reader/git/ref/tags/v1.2.3.body.next");
    fs::write(
        tag,
        serde_json::to_vec(&json!({"object":{"type":"commit", "sha":"e".repeat(40)}}))?,
    )?;
    fixture.rejected(command.output()?, "changed during")?;
    let directory = fixture.selection.directory.path().join("plan");
    fs::create_dir(&directory)?;
    fs::write(directory.join("keep"), "existing content")?;
    let output = fixture.command()?.output()?;
    assert!(!output.status.success());
    assert_eq!(
        fs::read_to_string(directory.join("keep"))?,
        "existing content"
    );
    Ok(())
}

#[test]
#[ignore = "requires installed skopeo and umoci; release image validation runs this test"]
fn real_oci_tools_preserve_platform_digests_and_detect_changed_executables() -> Result<()> {
    for changed in [false, true] {
        let fixture = PlanFixture::new(changed)?;
        for name in ["skopeo", "umoci"] {
            fs::remove_file(fixture.selection.directory.path().join("bin").join(name))?;
        }
        let output = fixture.command()?.output()?;
        if changed {
            fixture.rejected(output, "image dar differs")?;
        } else {
            PlanFixture::success(output);
            fixture.check_plan()?;
        }
    }
    Ok(())
}
