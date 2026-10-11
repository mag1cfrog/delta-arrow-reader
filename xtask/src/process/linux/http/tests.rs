use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use super::*;

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
