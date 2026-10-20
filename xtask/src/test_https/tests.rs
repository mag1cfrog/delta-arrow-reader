use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
};

use hyper::{StatusCode, body::Bytes};
use serde_json::json;

use super::*;
use crate::http_fixture::table::file_response;

#[test]
fn dependency_check_rejects_only_resolved_native_tls() -> Result<()> {
    for name in ["native-tls", "openssl", "openssl-sys"] {
        let mut metadata = json!({
            "packages": [{"id": "a", "name": "rustls"}, {"id": "b", "name": name}],
            "resolve": {"nodes": [{"id": "a"}]},
        });
        check_tls_dependencies(&metadata.to_string())?;
        metadata["resolve"]["nodes"] = json!([{"id": "a"}, {"id": "b"}]);
        assert!(
            check_tls_dependencies(&metadata.to_string())
                .unwrap_err()
                .to_string()
                .contains(name)
        );
    }
    assert!(check_tls_dependencies("{}").is_err());
    Ok(())
}

fn connect(server: &HttpTestServer, method: &str, target: &str) -> Result<BufReader<TcpStream>> {
    let mut socket = TcpStream::connect(server.origin.trim_start_matches("http://"))?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    write!(
        socket,
        "{method} {target} HTTP/1.1\r\nHost: {target}\r\n\r\n"
    )?;
    Ok(BufReader::new(socket))
}

fn headers(socket: &mut BufReader<TcpStream>, status: StatusCode) -> Result<()> {
    let mut wire = String::new();
    loop {
        let mut line = String::new();
        assert!(socket.read_line(&mut line)? > 0);
        wire.push_str(&line);
        if line == "\r\n" {
            break;
        }
    }
    assert!(
        wire.starts_with(&format!("HTTP/1.1 {} ", status.as_u16())),
        "{wire}"
    );
    Ok(())
}

#[test]
fn proxy_allows_only_its_servers_and_records_actual_tunnel_ports() -> Result<()> {
    let server = || {
        HttpTestServer::start(|request| {
            file_response(request, &Bytes::from_static(b"abcdef")).map(Some)
        })
    };
    let servers = [server()?, server()?];
    let ports = servers.each_ref().map(|server| {
        server
            .origin
            .parse::<hyper::Uri>()
            .unwrap()
            .port_u16()
            .unwrap()
    });
    let proxy = ConnectProxy::start(ports)?;
    for (server, host, port) in [
        (&servers[0], "localhost", ports[0]),
        (&servers[1], "127.0.0.1", ports[1]),
    ] {
        let mut socket = connect(&proxy.server, "CONNECT", &format!("{host}:{port}"))?;
        headers(&mut socket, StatusCode::OK)?;
        socket.get_mut().write_all(
            b"GET /probe?X-Amz-Signature=test HTTP/1.1\r\nHost: localhost\r\nRange: bytes=1-3\r\n\r\n",
        )?;
        headers(&mut socket, StatusCode::PARTIAL_CONTENT)?;
        let mut body = String::new();
        socket.read_to_string(&mut body)?;
        assert_eq!(body, "bcd");
        let requests = server.take_requests()?;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].uri, "/probe?X-Amz-Signature=test");
        assert_eq!(requests[0].range.as_ref().unwrap(), "bytes=1-3");
        assert!(
            proxy
                .upstream_ports
                .lock()
                .unwrap()
                .contains(&requests[0].peer_port)
        );
    }
    for (method, target) in [
        ("GET", "/"),
        ("CONNECT", "localhost:0"),
        ("CONNECT", "localhost"),
        ("CONNECT", &format!("example.com:{}", ports[0])),
    ] {
        headers(
            &mut connect(&proxy.server, method, target)?,
            StatusCode::FORBIDDEN,
        )?;
    }
    assert_eq!(proxy.server.take_requests()?.len(), 6);
    assert_eq!(proxy.upstream_ports.lock().unwrap().len(), 2);
    for server in servers {
        assert!(server.take_requests()?.is_empty());
    }
    Ok(())
}

#[test]
fn dropping_proxy_closes_active_tunnels() -> Result<()> {
    let server = HttpTestServer::start(|_| Ok(None))?;
    let port = server.origin.parse::<hyper::Uri>()?.port_u16().unwrap();
    let proxy = ConnectProxy::start([port, port])?;
    let mut socket = connect(&proxy.server, "CONNECT", &format!("localhost:{port}"))?;
    headers(&mut socket, StatusCode::OK)?;
    socket
        .get_mut()
        .write_all(b"GET /held HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    let request = server.wait_for_request(Duration::from_secs(2))?;
    drop(proxy);
    server.wait_for_disconnect(request.connection_id, Duration::from_secs(2))?;
    Ok(())
}
