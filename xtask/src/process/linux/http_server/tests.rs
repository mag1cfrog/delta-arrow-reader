use std::{
    io::{Read, Write},
    net::TcpStream,
};

use super::*;

fn connect(server: &HttpTestServer) -> std::io::Result<TcpStream> {
    let mut socket = TcpStream::connect(server.origin.trim_start_matches("http://"))?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    socket.write_all(
        b"GET /test?key=value HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )?;
    Ok(socket)
}

#[test]
fn records_requests_and_serves_responses() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = HttpTestServer::start(|_| {
        Ok(Some(Response::new(Full::new(Bytes::from_static(
            b"fixture",
        )))))
    })?;
    let mut wire = String::new();
    connect(&server)?.read_to_string(&mut wire)?;
    assert!(wire.starts_with("HTTP/1.1 200 OK\r\n"), "{wire}");
    assert!(wire.ends_with("\r\n\r\nfixture"), "{wire}");
    let requests = server.take_requests()?;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::GET);
    assert_eq!(requests[0].uri, "/test?key=value");
    assert!(server.take_requests()?.is_empty());
    Ok(())
}

#[test]
fn held_requests_detect_their_own_client_disconnect()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = HttpTestServer::start(|_| Ok(None))?;
    let first_socket = connect(&server)?;
    let first = server.wait_for_request(Duration::from_secs(2))?;
    let second_socket = connect(&server)?;
    let second = server.wait_for_request(Duration::from_secs(2))?;
    assert_ne!(first.connection_id, second.connection_id);
    drop(first_socket);
    server.wait_for_disconnect(first.connection_id, Duration::from_secs(2))?;
    assert!(
        server
            .wait_for_disconnect(second.connection_id, Duration::from_millis(50))
            .is_err()
    );
    drop(second_socket);
    server.wait_for_disconnect(second.connection_id, Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn shutdown_closes_blocked_connections_and_the_listener()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = HttpTestServer::start(|_| Ok(None))?;
    let mut socket = connect(&server)?;
    server.wait_for_request(Duration::from_secs(2))?;
    let address = server.origin.trim_start_matches("http://").to_owned();
    drop(server);
    match socket.read(&mut [0]) {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("server retained a blocked connection: {other:?}"),
    }
    assert!(TcpStream::connect(address).is_err());
    Ok(())
}

#[test]
fn handler_failures_are_reported() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = HttpTestServer::start(|_| {
        InvalidInputSnafu {
            message: "fixture handler failed",
        }
        .fail()
    })?;
    let _ = connect(&server)?.read_to_end(&mut Vec::new());
    let error = server
        .take_requests()
        .expect_err("a failed handler must fail the fixture");
    assert!(error.to_string().contains("fixture handler failed"));
    Ok(())
}
