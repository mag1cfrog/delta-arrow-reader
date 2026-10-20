use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arrow::ipc::reader::FileReader;
use serde_json::{Value, json};
use url::Url;

use super::{check_failure, run_with_timeout};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const TABLE_ID: &str = "12345678-1234-1234-1234-123456789abc";
const TABLE_NAME: &str = "main.sales.orders";
const PROFILE: &str = "secret profile $(do-not-run)";

/// A fake Databricks executable with controllable table metadata and credential responses.
struct DatabricksCliFixture {
    directory: tempfile::TempDir,
}

impl DatabricksCliFixture {
    fn new(azure: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let location = if azure {
            "abfss://container@account.dfs.core.windows.net/table/"
        } else {
            "s3://bucket/table/"
        };
        fs::write(
            directory.path().join("metadata.json"),
            serde_json::to_vec(&json!({
                "table_id": TABLE_ID, "full_name": TABLE_NAME, "table_type": "EXTERNAL", "data_source_format": "DELTA", "storage_location": location,
                "securable_kind_manifest": {"securable_type":"TABLE", "securable_kind":"TABLE_DELTA_EXTERNAL", "capabilities":[]},
                "columns": [{"name":"not_the_delta_schema", "type_name":"STRING"}]
            }))?,
        )?;
        let mut credentials = json!({"url":location, "expiration_time": (SystemTime::now().duration_since(UNIX_EPOCH)? + Duration::from_secs(300)).as_millis() as u64});
        if azure {
            credentials["azure_user_delegation_sas"] =
                json!({"sas_token":"sv=test&sp=rl&sig=test%2Bsignature"});
        } else {
            credentials["aws_temp_credentials"] = json!({"access_key_id":"test-key", "secret_access_key":"secret-sentinel", "session_token":"test-session"});
        }
        fs::write(
            directory.path().join("credentials.json"),
            serde_json::to_vec(&credentials)?,
        )?;
        let executable = directory.path().join("databricks");
        fs::write(
            &executable,
            r#"#!/bin/sh
set -eu
printf '%s\000' "$@" >> "$DAR_TEST_DIR/calls"
if IFS= read -r unused; then exit 98; fi
if [ -n "${DAR_TEST_PID1_SIGNAL:-}" ]; then
  printf '%s' "$PPID" > "$DAR_TEST_DIR/parent_pid"
  [ "$PPID" = 1 ] || exit 96
  kill -"$DAR_TEST_PID1_SIGNAL" "$PPID"
  exec /bin/sleep 60
fi
if [ "${DAR_TEST_HANG:-}" = 1 ]; then
  printf '%s' "$$" > "$DAR_TEST_DIR/pid"
  exec /bin/sleep 60
fi
case "$1" in
  tables) /bin/cat "$DAR_TEST_DIR/metadata.json" ;;
  temporary-table-credentials)
    if [ "${DAR_TEST_DENIED:-}" = 1 ]; then printf 'secret-sentinel\n' >&2; exit 1; fi
    /bin/cat "$DAR_TEST_DIR/credentials.json"
    ;;
  *) exit 97 ;;
esac
"#,
        )?;
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))?;
        fs::write(
            directory.path().join("options.json"),
            br#"{"aws_region":"eu-west-1"}"#,
        )?;
        Ok(Self { directory })
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .args(args)
            .env_remove("RUST_LOG")
            .env("PATH", self.directory.path())
            .env("DAR_TEST_DIR", self.directory.path());
        command
    }

    fn inspect(&self) -> Command {
        let mut command = self.command(&[
            "inspect",
            "uc://main.sales.orders",
            "--databricks-profile",
            PROFILE,
        ]);
        command
            .arg("--storage-options-file")
            .arg(self.directory.path().join("options.json"));
        command
    }

    fn calls(&self) -> Vec<String> {
        fs::read(self.directory.path().join("calls"))
            .unwrap_or_default()
            .split(|b| *b == 0)
            .filter(|arg| !arg.is_empty())
            .map(|arg| String::from_utf8(arg.to_vec()).unwrap())
            .collect()
    }
}

#[test]
fn inspection_uses_vended_s3_and_azure_credentials_and_actual_delta_schema() -> TestResult {
    let corpus = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned",
    );
    let schema =
        FileReader::try_new(fs::File::open(corpus.join("expected.arrow"))?, None)?.schema();
    let log = fs::read(corpus.join("table/_delta_log/00000000000000000000.json"))?;
    for azure in [false, true] {
        let fixture = DatabricksCliFixture::new(azure)?;
        let server = MockCloudStorageServer::new(azure, log.clone())?;
        let options = if azure {
            json!({"azure_endpoint":server.endpoint, "azure_allow_http":"true"})
        } else {
            json!({"aws_endpoint":server.endpoint, "aws_allow_http":"true", "aws_region":"eu-west-1"})
        };
        fs::write(
            fixture.directory.path().join("options.json"),
            serde_json::to_vec(&options)?,
        )?;
        for (historical, verbosity) in [(false, ""), (true, ""), (false, "trace")] {
            let mut command = fixture.inspect();
            command
                .env("AWS_ACCESS_KEY_ID", "ambient-key")
                .env("AZURE_STORAGE_ACCOUNT_KEY", "invalid-ambient-secret");
            if !verbosity.is_empty() {
                command.env("RUST_LOG", verbosity);
            }
            if historical {
                command.args(["--table-version", "0"]);
            }
            let output = run_with_timeout(&mut command)?;
            assert!(
                output.status.success(),
                "azure={azure}: {}, requests={:?}",
                String::from_utf8_lossy(&output.stderr),
                server
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|r| r.lines().next())
                    .collect::<Vec<_>>()
            );
            let diagnostic = std::str::from_utf8(&output.stderr)?;
            if verbosity.is_empty() {
                assert!(diagnostic.is_empty());
            } else {
                assert!(diagnostic.contains("event=\"table_load.completed\""));
                for sentinel in [
                    PROFILE,
                    TABLE_NAME,
                    TABLE_ID,
                    "secret",
                    "test-key",
                    "test-session",
                    "test%2Bsignature",
                    "bucket/table",
                    "account.dfs",
                ] {
                    assert!(!diagnostic.contains(sentinel), "{diagnostic}");
                }
            }
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout)?,
                json!({"format_version":1, "table_version":"0", "schema":schema})
            );
        }
        let profile = format!("--profile={PROFILE}");
        let expected = [
            "tables",
            "get",
            "--include-manifest-capabilities",
            &profile,
            "--output",
            "json",
            "--",
            TABLE_NAME,
            "temporary-table-credentials",
            "generate-temporary-table-credentials",
            "--table-id",
            TABLE_ID,
            "--operation",
            "READ",
            &profile,
            "--output",
            "json",
        ];
        assert_eq!(fixture.calls(), expected.repeat(3));
        let requests = server.requests.lock().unwrap();
        assert!(!requests.is_empty());
        for request in requests.iter() {
            let first = request.lines().next().unwrap();
            assert!(
                !first.contains(".parquet") && !first.contains(".bin"),
                "{first}"
            );
            if azure {
                assert!(first.contains("sig=test%2Bsignature"), "{first}");
            } else {
                assert!(
                    request.contains("Credential=test-key/")
                        && request.contains("/eu-west-1/s3/aws4_request"),
                    "missing UC signing credential or region"
                );
                assert!(
                    request
                        .to_ascii_lowercase()
                        .contains("x-amz-security-token: test-session")
                );
            }
        }
    }
    Ok(())
}

#[test]
fn help_malformed_input_and_direct_paths_do_not_invoke_databricks() -> TestResult {
    let fixture = DatabricksCliFixture::new(false)?;
    for args in [
        vec!["--help"],
        vec!["--version"],
        vec!["inspect", "--help"],
        vec!["inspect", "uc://a.b.c"],
        vec!["inspect", "uc://a.b", "--databricks-profile", "work"],
        vec!["scan", "uc://a.b.c", "--databricks-profile", "work"],
        vec!["inspect", "local", "--databricks-profile", "work"],
    ] {
        let output = run_with_timeout(&mut fixture.command(&args))?;
        if !args
            .iter()
            .any(|arg| matches!(*arg, "--help" | "--version"))
        {
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
        }
    }
    for key in [
        "AWS_TOKEN",
        "azure_storage_sas_key",
        "bucket",
        "account_name",
    ] {
        fs::write(
            fixture.directory.path().join("options.json"),
            serde_json::to_vec(&json!({key:"secret-sentinel"}))?,
        )?;
        let output = run_with_timeout(fixture.inspect().env("RUST_LOG", "trace"))?;
        check_failure(
            &output.stderr,
            "configuration",
            "invalid_uc_storage_options",
        );
    }
    assert!(fixture.calls().is_empty());
    let local = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table",
    );
    let output = run_with_timeout(fixture.command(&["inspect"]).arg(local).env("PATH", ""))?;
    assert!(output.status.success());
    let output = run_with_timeout(
        fixture
            .command(&["inspect", "uc://a.b.c", "--databricks-profile", "work"])
            .env("PATH", ""),
    )?;
    assert_eq!(output.status.code(), Some(2));
    let diagnostic = check_failure(&output.stderr, "configuration", "databricks_cli_missing");
    assert!(
        diagnostic.contains("Install the Databricks CLI"),
        "{diagnostic}"
    );
    Ok(())
}

#[test]
fn invalid_responses_and_denied_credentials_are_redacted() -> TestResult {
    for (response, expected) in [
        ("secret-sentinel".into(), "invalid_databricks_response"),
        ("x".repeat(1024 * 1024 + 1), "invalid_databricks_response"),
        (
            r#"{"table_id":"secret-sentinel"}"#.into(),
            "invalid_databricks_response",
        ),
    ] {
        let fixture = DatabricksCliFixture::new(false)?;
        fs::write(fixture.directory.path().join("metadata.json"), response)?;
        let output = run_with_timeout(&mut fixture.inspect())?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        check_failure(&output.stderr, "execution", expected);
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
        assert_eq!(fixture.calls().first().map(String::as_str), Some("tables"));
        assert!(
            !fixture
                .calls()
                .iter()
                .any(|a| a == "temporary-table-credentials")
        );
    }
    let fixture = DatabricksCliFixture::new(false)?;
    let output = run_with_timeout(
        fixture
            .inspect()
            .env("RUST_LOG", "trace")
            .env("DAR_TEST_DENIED", "1"),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    check_failure(&output.stderr, "execution", "uc_credential_request_failed");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    Ok(())
}

#[test]
fn uc_admission_preserves_direct_path_inspection_of_unsupported_protocols() -> TestResult {
    for feature in ["madeUpFeature", "catalogManaged"] {
        let directory = tempfile::tempdir()?;
        fs::create_dir(directory.path().join("_delta_log"))?;
        let log = format!(
            "{}\n{}\n",
            json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[feature],"writerFeatures":[feature]}}),
            json!({"metaData":{"id":"test","format":{"provider":"parquet","options":{}},"schemaString":r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}}]}"#,"partitionColumns":[],"configuration":{}}})
        );
        fs::write(
            directory
                .path()
                .join("_delta_log/00000000000000000000.json"),
            &log,
        )?;
        let fixture = DatabricksCliFixture::new(false)?;
        let server = MockCloudStorageServer::new(false, log.into_bytes())?;
        fs::write(
            fixture.directory.path().join("options.json"),
            serde_json::to_vec(
                &json!({"aws_region":"eu-west-1", "aws_endpoint":server.endpoint, "aws_allow_http":"true"}),
            )?,
        )?;
        let output = run_with_timeout(&mut fixture.inspect())?;
        assert_eq!(output.status.code(), Some(1));
        assert!(
            output.stdout.is_empty(),
            "UC inspection must not report an unsupported snapshot"
        );
        if feature == "madeUpFeature" {
            check_failure(&output.stderr, "execution", "unsupported_uc_table");
        } else {
            check_failure(&output.stderr, "snapshot", "snapshot_load");
        }
        if feature == "madeUpFeature" {
            let output = run_with_timeout(fixture.command(&["inspect"]).arg(directory.path()))?;
            assert!(
                output.status.success(),
                "direct-path protocol inspection changed"
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn uc_cancellation_remains_responsive_with_blocked_stdout() -> TestResult {
    use std::os::fd::AsRawFd;
    let fixture = DatabricksCliFixture::new(false)?;
    let schema = json!({"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{"large": "x".repeat(256 * 1024)}}]});
    let log = format!(
        "{}\n{}\n",
        json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        json!({"metaData":{"id":"test","format":{"provider":"parquet","options":{}},"schemaString":schema.to_string(),"partitionColumns":[],"configuration":{}}})
    );
    let server = MockCloudStorageServer::new(false, log.into_bytes())?;
    fs::write(
        fixture.directory.path().join("options.json"),
        serde_json::to_vec(
            &json!({"aws_region":"eu-west-1", "aws_endpoint":server.endpoint, "aws_allow_http":"true"}),
        )?,
    )?;
    let mut child = fixture
        .inspect()
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut ready = libc::pollfd {
        fd: child.stdout.as_ref().unwrap().as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: the poll descriptor points to this child's live stdout pipe.
    let polled = unsafe { libc::poll(&mut ready, 1, 5000) };
    if polled != 1 || ready.revents & libc::POLLIN == 0 {
        child.kill()?;
        let output = child.wait_with_output()?;
        panic!(
            "inspection did not produce output: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    // SAFETY: this is the live subprocess created above.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait()?.is_none() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if child.try_wait()?.is_none() {
        child.kill()?;
        child.wait()?;
        panic!("stdout blocked cancellation");
    }
    let output = child.wait_with_output()?;
    assert_eq!(output.status.code(), Some(143));
    assert!(!output.stdout.is_empty());
    check_cancellation(&output.stderr, 143);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn cancellation_terminates_pending_databricks_process() -> TestResult {
    for signal in [libc::SIGINT, libc::SIGTERM] {
        let fixture = DatabricksCliFixture::new(false)?;
        let mut child = fixture
            .inspect()
            .env("RUST_LOG", "info")
            .env("DAR_TEST_HANG", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let pid_file = fixture.directory.path().join("pid");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pid_file.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        let pid = fs::read_to_string(pid_file)?.parse::<i32>()?;
        // SAFETY: both process IDs belong to this test's subprocess tree.
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        while child.try_wait()?.is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if child.try_wait()?.is_none() {
            child.kill()?;
            panic!("cancelled CLI did not exit");
        }
        let output = child.wait_with_output()?;
        assert_eq!(output.status.code(), Some(128 + signal));
        assert!(output.stdout.is_empty());
        check_cancellation(&output.stderr, 128 + signal);
        let status_path = format!("/proc/{pid}/stat");
        let stopped = || {
            fs::read_to_string(&status_path)
                .map_or(true, |s| s.split_whitespace().nth(2) == Some("Z"))
        };
        while !stopped() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(stopped(), "Databricks process survived cancellation");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn check_cancellation(stderr: &[u8], status: i32) {
    let text = std::str::from_utf8(stderr).unwrap();
    let records = text
        .lines()
        .filter(|line| line.contains("event=\"cli.cancelled\""))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1, "{text}");
    let record = records[0];
    assert!(record.contains("INFO delta_arrow_reader_cli"), "{text}");
    assert!(record.contains("outcome=\"cancelled\""), "{text}");
    assert!(record.contains("error_code=\"uc_cancelled\""), "{text}");
    assert!(record.contains(&format!("exit_status={status}")), "{text}");
    assert!(!text.contains("ERROR"), "{text}");
    assert!(!text.contains("secret"), "{text}");
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires unshare and permission to create user/PID namespaces"]
fn pid_one_cancellation_preserves_uc_diagnostic() -> TestResult {
    for (signal, status) in [("INT", 130), ("TERM", 143)] {
        let fixture = DatabricksCliFixture::new(false)?;
        let inspect = fixture.inspect();
        // Keep unshare on the host PATH; env then execs DAR as namespace PID 1.
        // --kill-child also cleans up the namespace if the test times out.
        let output = run_with_timeout(
            Command::new("unshare")
                .args([
                    "--user",
                    "--map-root-user",
                    "--pid",
                    "--fork",
                    "--kill-child",
                ])
                .arg("/usr/bin/env")
                .arg(format!("PATH={}", fixture.directory.path().display()))
                .arg(inspect.get_program())
                .args(inspect.get_args())
                .env("DAR_TEST_DIR", fixture.directory.path())
                .env("RUST_LOG", "info")
                .env("DAR_TEST_PID1_SIGNAL", signal),
        )?;
        assert_eq!(
            fs::read_to_string(fixture.directory.path().join("parent_pid"))?,
            "1"
        );
        assert_eq!(output.status.code(), Some(status));
        assert!(output.stdout.is_empty());
        check_cancellation(&output.stderr, status);
    }
    Ok(())
}

/// A local server that serves Delta logs as S3/Azure objects and records requests.
///
/// Only GET/HEAD and log listing are needed, following the core cloud-location fixture.
struct MockCloudStorageServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockCloudStorageServer {
    fn new(azure: bool, log: Vec<u8>) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                let Ok((mut socket, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while request.len() < 16 * 1024 && !request.ends_with(b"\r\n\r\n") {
                    if socket.read_exact(&mut byte).is_err() {
                        break;
                    }
                    request.push(byte[0]);
                }
                let request = String::from_utf8(request).unwrap();
                recorded.lock().unwrap().push(request.clone());
                let Some(target) = request.split_whitespace().nth(1) else {
                    continue;
                };
                let url = Url::parse(&format!("http://localhost{target}")).unwrap();
                let query = url.query_pairs().collect::<HashMap<_, _>>();
                let listing = query.contains_key("list-type")
                    || query.get("comp").is_some_and(|v| v == "list");
                let key = "table/_delta_log/00000000000000000000.json";
                let (status, mut body) = if listing {
                    let include = key.starts_with(query.get("prefix").map_or("", |v| v.as_ref()))
                        && key > query.get("start-after").map_or("", |v| v.as_ref());
                    let entry = if !include {
                        String::new()
                    } else if azure {
                        format!(
                            "<Blob><Name>{key}</Name><Properties><Last-Modified>Wed, 01 Jan 2020 00:00:00 GMT</Last-Modified><Etag>test</Etag><Content-Length>{}</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType></Properties></Blob>",
                            log.len()
                        )
                    } else {
                        format!(
                            "<Contents><Key>{key}</Key><LastModified>2020-01-01T00:00:00Z</LastModified><Size>{}</Size></Contents>",
                            log.len()
                        )
                    };
                    let xml = if azure {
                        format!(
                            "<EnumerationResults><Blobs>{entry}</Blobs><NextMarker /></EnumerationResults>"
                        )
                    } else {
                        format!(
                            "<ListBucketResult><IsTruncated>false</IsTruncated>{entry}</ListBucketResult>"
                        )
                    };
                    (200, xml.into_bytes())
                } else if url.path()
                    == format!("/{}/{key}", if azure { "container" } else { "bucket" })
                {
                    (200, log.clone())
                } else {
                    (404, Vec::new())
                };
                let length = body.len();
                if request.starts_with("HEAD ") {
                    body.clear();
                }
                let headers = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Length: {length}\r\nContent-Type: {}\r\nLast-Modified: Wed, 01 Jan 2020 00:00:00 GMT\r\nETag: test\r\nConnection: close\r\n\r\n",
                    if listing {
                        "application/xml"
                    } else {
                        "application/octet-stream"
                    }
                );
                let _ = socket
                    .write_all(headers.as_bytes())
                    .and_then(|()| socket.write_all(&body));
            }
        });
        Ok(Self {
            endpoint,
            requests,
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for MockCloudStorageServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}
