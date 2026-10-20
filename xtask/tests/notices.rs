//! Dependency notice generation failures preserve existing output.

#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, process::Command};

#[test]
fn tool_failure_does_not_create_or_replace_output() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let rustup = temporary.path().join("rustup");
    fs::write(
        &rustup,
        "#!/bin/sh\necho 'simulated cargo-about failure' >&2\nexit 17\n",
    )?;
    fs::set_permissions(&rustup, fs::Permissions::from_mode(0o755))?;
    let output = temporary.path().join("notices.txt");
    for existing in [false, true] {
        if existing {
            fs::write(&output, "Previously validated notices\n")?;
        }
        let result = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args(["cli", "notices", "--architecture", "amd64", "--output"])
            .arg(&output)
            .env("PATH", temporary.path())
            .output()?;
        assert!(!result.status.success());
        let stderr = String::from_utf8(result.stderr)?;
        assert!(stderr.contains("simulated cargo-about failure"), "{stderr}");
        if existing {
            assert_eq!(
                fs::read_to_string(&output)?,
                "Previously validated notices\n"
            );
        } else {
            assert!(!output.exists());
        }
    }
    Ok(())
}
