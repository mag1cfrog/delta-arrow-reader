use std::{io::IsTerminal, net::TcpStream};

use super::*;

#[test]
fn table_access_probe_detects_connections_after_the_client_closes()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let probe = ConnectionProbe::new()?;
    probe.check_no_connections()?;
    drop(TcpStream::connect(probe.listener.local_addr()?)?);
    assert!(matches!(
        probe.check_no_connections(),
        Err(crate::error::Error::InvalidInput { .. })
    ));
    Ok(())
}

#[test]
fn terminal_probe_detects_output_written_by_a_child()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let (master, terminal) = open_terminal()?;
    assert!(terminal.is_terminal());
    check_no_terminal_output(&master)?;
    let output = output_with_redirected_stdout(
        Command::new("sh").args(["-c", "test -t 1 && printf terminal-output"]),
        Stdio::from(terminal.try_clone()?),
        Duration::from_secs(5),
    )?;
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    assert!(matches!(
        check_no_terminal_output(&master),
        Err(crate::error::Error::InvalidInput { .. })
    ));
    Ok(())
}
