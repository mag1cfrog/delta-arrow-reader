use super::*;
use sha2::Digest;
use std::{
    io::Read,
    net::TcpListener,
    thread,
    time::{Duration, Instant},
};

#[test]
fn placement_keeps_reader_server_and_observer_on_separate_physical_cores() -> Result<()> {
    let rows: Vec<_> = (0..16)
        .map(|cpu| json!({"cpu":cpu, "core": cpu % 8, "socket": 0}))
        .collect();
    assert_eq!(
        placement(&rows)?,
        json!({"reader":[0,1,2,3,8,9,10,11], "server":[4,5], "observer":[6]})
    );
    assert!(placement(&rows[..10]).is_err());
    let rows: Vec<_> = (0..16)
        .map(|cpu| json!({"cpu":cpu, "core":cpu % 8, "socket":cpu / 8}))
        .collect();
    assert_eq!(
        placement(&rows)?,
        json!({"reader":[0,1,2,3,4,5,6,7], "server":[8,9], "observer":[10]})
    );
    Ok(())
}

#[test]
fn stored_checksums_reject_duplicates_and_require_full_object_evidence() -> Result<()> {
    let head = |fields: &[&str]| format!("HTTP/1.1 200 OK\r\n{}\r\n\r\n200", fields.join("\r\n"));
    let correct = [
        "Content-Length: 4",
        "X-Amz-Checksum-Sha256: expected",
        "X-Amz-Checksum-Type: FULL_OBJECT",
    ];
    assert!(checksum_verified(head(&correct).as_bytes(), 4, "expected")?);
    for (size, hash, extra) in [
        (5, "expected", ""),
        (4, "wrong", ""),
        (4, "expected", "Content-Length: 4"),
        (4, "expected", "x-amz-checksum-sha256: expected"),
        (4, "expected", "x-amz-checksum-type: FULL_OBJECT"),
    ] {
        let mut fields = correct.to_vec();
        if !extra.is_empty() {
            fields.push(extra);
        }
        assert!(checksum_verified(head(&fields).as_bytes(), size, hash).is_err());
    }
    assert!(!checksum_verified(
        head(&["Content-Length: 4"]).as_bytes(),
        4,
        "expected"
    )?);
    assert!(!checksum_verified(
        head(&[
            "Content-Length: 4",
            "X-Amz-Checksum-Sha256: partial-2",
            "X-Amz-Checksum-Type: COMPOSITE"
        ])
        .as_bytes(),
        4,
        "expected"
    )?);
    assert_eq!(
        object_path("table/a+b%?# \u{00e9}.parquet"),
        "/selective-read/table/a%2Bb%25%3F%23%20%C3%A9.parquet"
    );
    Ok(())
}

#[test]
fn storage_identity_rejects_remote_endpoints_and_public_credentials() -> Result<()> {
    for invalid in [
        "http://localhost:19000",
        "http://127.0.0.1:19000/",
        "https://127.0.0.1:19000",
        "http://user:secret@127.0.0.1:19000",
        "http://127.0.0.1:19000?query",
        "http://127.0.0.1:19000#fragment",
        "http://example.com:19000",
    ] {
        assert!(endpoint(&json!({"endpoint":invalid})).is_err());
    }
    assert_eq!(
        endpoint(&json!({"endpoint":"http://127.0.0.1:19000"}))?,
        "http://127.0.0.1:19000"
    );
    let root = tempfile::tempdir()?;
    let path = root.path().join("credentials.json");
    save(
        &path,
        &json!({"access_key":"benchmark","secret_key":"123abc"}),
    )?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644))?;
    assert!(credentials(root.path()).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    assert_eq!(credentials(root.path())?["secret_key"], "123abc");
    assert!(cgroup("/a/../../tmp").is_err());
    Ok(())
}

#[test]
fn immutable_upload_uses_native_curl_and_reads_back_legacy_objects() -> Result<()> {
    for (put_status, checksum, legacy, bad_body) in [
        (200, true, false, false),
        (412, true, false, false),
        (412, true, true, false),
        (200, false, false, false),
        (412, true, true, true),
    ] {
        let root = tempfile::tempdir()?;
        let source = root.path().join("part.parquet");
        fs::write(&source, b"data")?;
        let hash = digest(&source)?;
        let encoded = STANDARD.encode(sha2::Sha256::digest(b"data"));
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        save(
            &root.path().join("server.json"),
            &json!({"endpoint": format!("http://{}", listener.local_addr()?), "bucket": BUCKET}),
        )?;
        let credentials_path = root.path().join("credentials.json");
        save(
            &credentials_path,
            &json!({"access_key":"benchmark","secret_key":"private"}),
        )?;
        fs::set_permissions(&credentials_path, fs::Permissions::from_mode(0o600))?;
        let server = thread::spawn(move || -> std::io::Result<Vec<String>> {
            let mut methods = vec!["PUT"];
            if checksum {
                methods.push("HEAD");
            }
            if !checksum || legacy {
                methods.push("GET");
            }
            let mut seen = Vec::new();
            for method in methods {
                let until = Instant::now() + Duration::from_secs(5);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < until =>
                        {
                            thread::sleep(Duration::from_millis(1))
                        }
                        Err(error) => return Err(error),
                    }
                };
                stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte)?;
                    bytes.push(byte[0]);
                }
                let request = String::from_utf8(bytes).unwrap();
                assert!(
                    request.starts_with(&format!("{method} /selective-read/table/a%20b.parquet "))
                );
                assert!(request.contains("AWS4-HMAC-SHA256"));
                assert!(!request.contains("private"));
                if method == "PUT" {
                    assert!(request.to_ascii_lowercase().contains("if-none-match: *"));
                    let mut body = [0; 4];
                    stream.read_exact(&mut body)?;
                    assert_eq!(&body, b"data");
                }
                let status = if method == "PUT" { put_status } else { 200 };
                let size = if method == "PUT" { 0 } else { 4 };
                let extra = if method == "HEAD" && !legacy {
                    format!(
                        "X-Amz-Checksum-Sha256: {encoded}\r\nX-Amz-Checksum-Type: FULL_OBJECT\r\n"
                    )
                } else {
                    String::new()
                };
                write!(
                    stream,
                    "HTTP/1.1 {status} OK\r\nContent-Length: {size}\r\n{extra}Connection: close\r\n\r\n"
                )?;
                if method == "GET" {
                    stream.write_all(if bad_body { b"bad!" } else { b"data" })?;
                }
                seen.push(method.to_owned());
            }
            Ok(seen)
        });
        let result = put_verified(root.path(), "table/a b.parquet", &source, &hash, checksum);
        let requests = server.join().unwrap()?;
        if bad_body {
            assert!(result.is_err());
        } else {
            assert_eq!(
                result?,
                if checksum && !legacy {
                    "server-sha256"
                } else {
                    "full-get"
                }
            );
        }
        assert_eq!(
            requests.last().unwrap(),
            if checksum && !legacy { "HEAD" } else { "GET" }
        );
    }
    Ok(())
}
