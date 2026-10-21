#![cfg(target_os = "linux")]

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Stdio},
};

#[test]
fn smoke_generation_keeps_resource_limits_and_identifies_an_unlinked_executable()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let executable = temporary.path().join("fixture-cli");
    fs::copy(env!("CARGO_BIN_EXE_selective-read-fixtures"), &executable)?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))?;
    let mut hash = Sha256::new();
    let mut source = fs::File::open(&executable)?;
    let mut buffer = [0; 64 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    let expected: String = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    // CI keeps this fixture for the independent oracle's subsequent checks.
    let output = std::env::var_os("SELECTIVE_READ_FIXTURES")
        .map(PathBuf::from)
        .unwrap_or_else(|| temporary.path().join("smoke"));
    let mut child = Command::new(&executable)
        .args(["--profile", "smoke", "--sort-memory-mib", "16", "--output"])
        .arg(&output)
        .stdout(Stdio::null())
        .spawn()?;
    fs::remove_file(&executable)?;
    assert!(child.wait()?.success());
    let manifest: Value = serde_json::from_reader(fs::File::open(output.join("manifest.json"))?)?;
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["generator"]["executable_sha256"], expected);
    Ok(())
}
