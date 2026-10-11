use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
};

use super::*;

fn connect(server: &HttpTestServer) -> std::io::Result<TcpStream> {
    connect_path(server, "/test?key=value")
}

fn connect_path(server: &HttpTestServer, path: &str) -> std::io::Result<TcpStream> {
    let mut socket = TcpStream::connect(server.origin.trim_start_matches("http://"))?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    write!(
        socket,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
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
fn shutdown_drops_listener_and_connection_tasks()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let handler = Arc::new(|_: &Request<Incoming>| Ok(None));
    let handler_lifetime = Arc::downgrade(&handler);
    let server = HttpTestServer::start(move |request| handler(request))?;
    let mut socket = connect(&server)?;
    server.wait_for_request(Duration::from_secs(2))?;
    drop(server);
    match socket.read(&mut [0]) {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("server retained a blocked connection: {other:?}"),
    }
    // A released port can be reused; track the handler owned by both task types instead.
    assert!(
        handler_lifetime.upgrade().is_none(),
        "listener or connection task retained its handler"
    );
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

#[test]
fn held_async_requests_allow_other_connections_and_resume_after_release()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let (release, released) = tokio::sync::watch::channel(false);
    let server = HttpTestServer::start_async(move |request| {
        let mut released = released.clone();
        async move {
            if request.uri().path() == "/held" {
                released.wait_for(|released| *released).await.unwrap();
            }
            Ok(Some(Response::new(Full::new(Bytes::from_static(
                b"fixture",
            )))))
        }
    })?;
    let mut held = connect_path(&server, "/held")?;
    server.wait_for_request(Duration::from_secs(2))?;
    let mut wire = String::new();
    connect_path(&server, "/ready")?.read_to_string(&mut wire)?;
    assert!(wire.ends_with("\r\n\r\nfixture"));
    held.set_read_timeout(Some(Duration::from_millis(50)))?;
    assert!(
        matches!(held.read(&mut [0]), Err(error) if matches!(error.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock))
    );
    release.send_replace(true);
    held.set_read_timeout(Some(Duration::from_secs(2)))?;
    wire.clear();
    held.read_to_string(&mut wire)?;
    assert!(wire.ends_with("\r\n\r\nfixture"));
    server.take_requests()?;
    Ok(())
}

#[test]
fn channel_body_sends_headers_before_release_and_flushes_before_disconnect()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let (send, receive) = mpsc::channel();
    let server = HttpTestServer::start_async(move |_| {
        let (sender, body) = http_body_util::channel::Channel::<Bytes>::new(1);
        send.send(sender).unwrap();
        ready(Ok(Some(
            Response::builder()
                .header("Content-Length", "7")
                .body(body)
                .unwrap(),
        )))
    })?;
    let mut socket = BufReader::new(connect(&server)?);
    let mut headers = String::new();
    loop {
        let mut line = String::new();
        assert!(socket.read_line(&mut line)? > 0);
        headers.push_str(&line);
        if line == "\r\n" {
            break;
        }
    }
    assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
    let request = server.wait_for_request(Duration::from_secs(2))?;
    socket
        .get_ref()
        .set_read_timeout(Some(Duration::from_millis(50)))?;
    assert!(
        matches!(socket.read(&mut [0]), Err(error) if matches!(error.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock))
    );
    let mut sender = receive.recv_timeout(Duration::from_secs(2))?;
    sender
        .try_send(hyper::body::Frame::data(Bytes::from_static(b"fixture")))
        .unwrap();
    drop(sender);
    socket
        .get_ref()
        .set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut body = String::new();
    socket.read_to_string(&mut body)?;
    assert_eq!(body, "fixture");
    server.wait_for_response_complete(request.connection_id, Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn disconnecting_a_held_request_does_not_count_as_a_completed_response()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = HttpTestServer::start(|_| Ok(None))?;
    let socket = connect(&server)?;
    let request = server.wait_for_request(Duration::from_secs(2))?;
    drop(socket);
    let error = server
        .wait_for_response_complete(request.connection_id, Duration::from_secs(2))
        .unwrap_err();
    assert!(error.to_string().contains("before its response completed"));
    Ok(())
}
