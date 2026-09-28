#![cfg(target_os = "linux")]

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::ExitStatusExt;
use std::process::Command;

#[test]
fn preflight_and_deadline_cannot_leave_a_completed_fixture() {
    let root = tempfile::tempdir().unwrap();
    let output = root.path().join("output");
    let binary = env!("CARGO_BIN_EXE_selective-read-fixtures");
    let rejected = Command::new(binary)
        .args([
            "--profile",
            "large",
            "--scale-factor",
            "300",
            "--fixture",
            "wide.clustered",
            "--disk-limit-mib",
            "1",
            "--elapsed-limit-seconds",
            "10",
            "--preflight",
            "--output",
        ])
        .arg(&output)
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    let plan: serde_json::Value = serde_json::from_slice(&rejected.stdout).unwrap();
    assert_eq!(plan["fits_budget"], false);
    assert!(!output.exists());

    // A source manifest blocked in read() tests the hard wall-clock limit without
    // generating a large dataset or depending on machine throughput/free space.
    let fifo = CString::new(root.path().join("manifest.json").as_os_str().as_bytes()).unwrap();
    // SAFETY: the owned NUL-terminated path remains valid throughout mkfifo.
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let mut child = Command::new(binary)
        .args([
            "--profile",
            "large",
            "--scale-factor",
            "1",
            "--fixture",
            "source",
            "--disk-limit-mib",
            "1",
            "--elapsed-limit-seconds",
            "1",
            "--source-from",
        ])
        .arg(root.path())
        .arg("--output")
        .arg(&output)
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed().as_secs() >= 5 {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("preparation did not enforce its one-second deadline");
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    assert_eq!(status.signal(), Some(libc::SIGALRM));
    assert!(!output.exists());
}
