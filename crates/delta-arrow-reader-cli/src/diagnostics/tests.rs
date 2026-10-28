use std::{
    fmt, io,
    process::{Command, Stdio},
    sync::Barrier,
    time::{Duration, Instant},
};

use arrow_schema::ArrowError;
use serde_json::Value;

use super::{DiagnosticFormat, Error};

struct PanicOnFormatError;

impl fmt::Debug for PanicOnFormatError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("secret source Debug was called")
    }
}

impl fmt::Display for PanicOnFormatError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("secret source Display was called")
    }
}

impl std::error::Error for PanicOnFormatError {}

// Run through the production panic hook without adding a CLI fault-injection flag.
#[test]
fn run_diagnostic_subprocess_scenario() {
    let Ok(scenario) = std::env::var("DAR_DIAGNOSTIC_TEST_SCENARIO") else {
        return;
    };
    let diagnostic_format = DiagnosticFormat::Json;
    super::init_tracing(diagnostic_format);
    crate::runtime::install_panic_hook(diagnostic_format);
    tracing::error!(target: "delta_arrow_reader", "secret core trace");
    tracing::error!(target: "reqwest", "secret dependency trace");
    tracing::error!(target: "delta_arrow_reader_cli", "secret CLI trace");
    let error = match scenario.as_str() {
        "main_panic" => panic!("secret panic payload"),
        "locked_terminal_panic" => {
            let _guard = super::TERMINAL_RECORD_STARTED.lock().unwrap();
            panic!("secret panic while a terminal writer owns the lock");
        }
        "worker_panics" => {
            let barrier = Barrier::new(3);
            std::thread::scope(|scope| {
                for _ in 0..2 {
                    scope.spawn(|| {
                        barrier.wait();
                        std::panic::panic_any(PanicOnFormatError);
                    });
                }
                barrier.wait();
            });
            unreachable!()
        }
        "source" => Error::Runtime {
            source: io::Error::other("fake child stdout=secret-token\nstderr=secret-password"),
            reason: "runtime_build_failed",
        },
        "unprintable" => Error::ArrowIpc {
            source: ArrowError::ExternalError(Box::new(PanicOnFormatError)),
        },
        "encoding" => Error::ArrowIpc {
            source: ArrowError::IpcError("secret schema metadata".into()),
        },
        _ => unreachable!(),
    };
    diagnostic_format.report_failure(&error);
    diagnostic_format.report_failure(&error);
    diagnostic_format.write_terminal_record(serde_json::json!({"kind": "completion"}));
    std::process::exit(1);
}

#[test]
fn panics_and_source_errors_keep_safe_bounded_reporting() {
    for (scenario, code, reason) in [
        ("main_panic", "runtime_initialization", "process_panicked"),
        (
            "locked_terminal_panic",
            "runtime_initialization",
            "process_panicked",
        ),
        (
            "worker_panics",
            "runtime_initialization",
            "process_panicked",
        ),
        ("source", "runtime_initialization", "runtime_build_failed"),
        ("unprintable", "arrow_ipc", "arrow_ipc_encode_failed"),
        ("encoding", "arrow_ipc", "arrow_ipc_encode_failed"),
    ] {
        for filter in ["off", "trace", "secret-invalid-filter=not-a-level"] {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "diagnostics::tests::run_diagnostic_subprocess_scenario",
                    "--nocapture",
                ])
                .env("DAR_DIAGNOSTIC_TEST_SCENARIO", scenario)
                .env("RUST_LOG", filter)
                .env("RUST_BACKTRACE", "full")
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(3);
            while child.try_wait().unwrap().is_none() {
                if Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("{scenario} did not exit");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let output = child.wait_with_output().unwrap();
            assert_eq!(output.status.code(), Some(1), "{output:?}");
            let text = String::from_utf8(output.stderr).unwrap();
            if scenario == "locked_terminal_panic" {
                assert!(text.is_empty(), "{text}");
                continue;
            }
            assert_eq!(text.lines().count(), 1, "{text}");
            assert!(text.ends_with('\n') && text.len() <= 8192, "{text}");
            assert!(!text.contains("secret") && !text.contains('\x1b'), "{text}");
            let record: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(record["format_version"], super::DIAGNOSTIC_FORMAT_VERSION);
            assert!(record.get("message").is_none());
            assert_eq!(record["kind"], "diagnostic");
            assert_eq!(record["terminal"], true);
            assert_eq!(record["phase"], "execution");
            assert_eq!(record["code"], code);
            assert_eq!(record["reason"], reason);
            assert_eq!(record["recovery"], "unknown");
            assert_eq!(record["retryable"], Value::Null);
        }
    }
}
