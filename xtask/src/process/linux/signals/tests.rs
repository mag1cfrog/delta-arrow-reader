use std::process::Command;

use super::*;

#[test]
fn cleanup_kills_and_reaps_the_process() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let process = KillOnDrop {
        child: Command::new("sleep").arg("30").spawn()?,
    };
    let pid = Pid::from_child(&process.child);
    drop(process);
    assert_eq!(
        rustix::process::test_kill_process(pid),
        Err(rustix::io::Errno::SRCH)
    );
    Ok(())
}
