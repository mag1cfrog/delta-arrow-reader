use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use super::*;
use crate::http_fixture::HttpTestServer;

#[test]
fn byte_ranges_cover_bounded_open_and_suffix_reads() {
    for (header, expected) in [
        ("bytes=0-2", Some(0..3)),
        ("bytes=2-99", Some(2..6)),
        ("bytes=2-", Some(2..6)),
        ("bytes=-2", Some(4..6)),
        ("bytes=-99", Some(0..6)),
        ("bytes=-0", None),
        ("bytes=6-", None),
        ("bytes=3-2", None),
        ("bytes=0-1,3-4", None),
        ("bytes=bad-", None),
        ("bytes=-", None),
        ("invalid", None),
    ] {
        assert_eq!(byte_range(header, 6), expected, "{header}");
    }
    assert_eq!(byte_range("bytes=0-", 0), None);
    assert_eq!(
        byte_range(&format!("bytes=1-{}", usize::MAX), 6),
        Some(1..6)
    );
}

#[test]
fn file_responses_preserve_head_lengths_and_range_bytes()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = HttpTestServer::start(|request| {
        file_response(request, &Bytes::from_static(b"abcdef")).map(Some)
    })?;
    for (method, range, status, length, body, content_range) in [
        ("GET", "", 200, 6, "abcdef", None),
        ("HEAD", "", 200, 6, "", None),
        ("GET", "bytes=1-3", 206, 3, "bcd", Some("bytes 1-3/6")),
        ("HEAD", "bytes=1-3", 206, 3, "", Some("bytes 1-3/6")),
        ("GET", "bytes=-2", 206, 2, "ef", Some("bytes 4-5/6")),
        ("GET", "bytes=99-", 416, 0, "", None),
    ] {
        let mut socket = TcpStream::connect(server.origin.trim_start_matches("http://"))?;
        socket.set_read_timeout(Some(Duration::from_secs(2)))?;
        socket.set_write_timeout(Some(Duration::from_secs(2)))?;
        let range = if range.is_empty() {
            String::new()
        } else {
            format!("Range: {range}\r\n")
        };
        write!(
            socket,
            "{method} /file HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{range}\r\n"
        )?;
        let mut wire = String::new();
        socket.read_to_string(&mut wire)?;
        let (headers, actual_body) = wire.split_once("\r\n\r\n").ok_or("missing HTTP headers")?;
        assert!(
            headers.starts_with(&format!("HTTP/1.1 {status} ")),
            "{wire}"
        );
        assert!(
            headers
                .to_ascii_lowercase()
                .contains(&format!("\r\ncontent-length: {length}")),
            "{wire}"
        );
        if let Some(range) = content_range {
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains(&format!("\r\ncontent-range: {range}")),
                "{wire}"
            );
        }
        assert_eq!(actual_body, body);
    }
    assert_eq!(server.take_requests()?.len(), 6);
    Ok(())
}

#[test]
fn directory_fixture_serves_encoded_files_and_lists_only_delta_metadata()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let log = directory.path().join("_delta_log");
    let partition = directory.path().join("region=100% west");
    fs::create_dir(&log)?;
    fs::create_dir(&partition)?;
    fs::write(log.join("commit & extra.json"), "metadata")?;
    fs::write(partition.join("part.parquet"), "parquet bytes")?;
    fs::create_dir_all(directory.path().join("other/_delta_log"))?;
    fs::write(
        directory.path().join("other/_delta_log/second.json"),
        "other",
    )?;
    let table = HttpTable::from_directory(directory.path())?;
    let listing = String::from_utf8(table.listing("/_delta_log/").to_vec())?;
    assert!(listing.contains("<href>/_delta_log/commit%20&amp;%20extra.json</href>"));
    assert!(listing.contains("<getcontentlength>8</getcontentlength>"));
    assert!(!listing.contains("parquet"));
    assert!(!listing.contains("second.json"));
    let nested = String::from_utf8(table.listing("/other/_delta_log/").to_vec())?;
    assert!(nested.contains("<href>/other/_delta_log/second.json</href>"));
    assert!(!nested.contains("commit"));
    let server = HttpTestServer::start(move |request| table.respond(request).map(Some))?;
    for (method, path, status, body) in [
        (
            "GET",
            "/region=100%25%20west/part.parquet",
            200,
            "parquet bytes",
        ),
        ("HEAD", "/region=100%25%20west/part.parquet", 200, ""),
        (
            "GET",
            "/_delta_log/commit%20&%20extra.json",
            200,
            "metadata",
        ),
        ("PROPFIND", "/_delta_log", 207, listing.as_str()),
        ("PROPFIND", "/_delta_log/", 207, listing.as_str()),
        ("PROPFIND", "/other/_delta_log/", 207, nested.as_str()),
        ("GET", "/missing", 404, ""),
        ("POST", "/region=100%25%20west/part.parquet", 404, ""),
        ("PROPFIND", "/", 404, ""),
    ] {
        let mut socket = TcpStream::connect(server.origin.trim_start_matches("http://"))?;
        socket.set_read_timeout(Some(Duration::from_secs(2)))?;
        socket.set_write_timeout(Some(Duration::from_secs(2)))?;
        write!(
            socket,
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        )?;
        let mut wire = String::new();
        socket.read_to_string(&mut wire)?;
        let (headers, actual_body) = wire.split_once("\r\n\r\n").ok_or("missing HTTP headers")?;
        assert!(
            headers.starts_with(&format!("HTTP/1.1 {status} ")),
            "{wire}"
        );
        assert_eq!(actual_body, body);
    }
    assert_eq!(server.take_requests()?.len(), 9);
    Ok(())
}

#[test]
fn directory_fixture_rejects_symlinks() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    std::os::unix::fs::symlink(directory.path(), directory.path().join("loop"))?;
    let result = HttpTable::from_directory(directory.path());
    assert!(
        matches!(result, Err(crate::error::Error::InvalidInput { message }) if message.contains("only files and directories"))
    );
    Ok(())
}
