use std::{
    io::{self, Read, Write},
    os::unix::process::CommandExt,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use rustix::{
    io::ioctl_fionbio,
    process::{Pid, Signal, kill_process_group},
    thread::{CpuSet, sched_getaffinity, sched_setaffinity},
};
use serde_json::Value;

use crate::{
    Result,
    selective_read::{array, require},
};

pub(in crate::selective_read) struct Worker(Child);

pub(in crate::selective_read) struct Affinity(CpuSet);

impl Affinity {
    pub(in crate::selective_read) fn pin(config: &Value, worker: &Worker) -> Result<Self> {
        let guard = Self(sched_getaffinity(None)?);
        let mut cpus = CpuSet::new();
        for cpu in array(&config["cpus"]["observer"])? {
            let cpu: usize = cpu.as_u64().ok_or("invalid observer CPU")?.try_into()?;
            require(cpu < CpuSet::MAX_CPU, "observer CPU exceeds affinity mask")?;
            cpus.set(cpu);
        }
        sched_setaffinity(None, &cpus)?;
        worker.pin(&cpus)?;
        Ok(guard)
    }
}

impl Drop for Affinity {
    fn drop(&mut self) {
        let _ = sched_setaffinity(None, &self.0);
    }
}

impl Worker {
    pub(in crate::selective_read) fn start(
        python: &Path,
        harness: &Path,
        output: &Path,
    ) -> Result<Self> {
        Self::spawn(
            Command::new(python)
                .args(["-I", "-B", "-u"])
                .arg(harness.join("controller_worker.py"))
                .env("SELECTIVE_READ_XTASK", std::env::current_exe()?)
                .stderr(std::fs::File::create_new(
                    output.join("validation.stderr.log"),
                )?),
        )
    }

    fn spawn(command: &mut Command) -> Result<Self> {
        let worker = Self(
            command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .process_group(0)
                .spawn()?,
        );
        ioctl_fionbio(worker.0.stdin.as_ref().ok_or("missing worker input")?, true)?;
        ioctl_fionbio(
            worker.0.stdout.as_ref().ok_or("missing worker output")?,
            true,
        )?;
        Ok(worker)
    }

    pub(in crate::selective_read) fn pin(&self, cpus: &CpuSet) -> Result<()> {
        sched_setaffinity(Some(Pid::from_child(&self.0)), cpus)?;
        Ok(())
    }

    pub(in crate::selective_read) fn call(&mut self, request: &Value) -> Result<Value> {
        // Each export also retains the oracle's own memory, disk and elapsed-time limits.
        self.exchange(request, Duration::from_secs(24 * 3600))
    }

    fn exchange(&mut self, request: &Value, timeout: Duration) -> Result<Value> {
        let deadline = Instant::now() + timeout;
        let mut input = serde_json::to_vec(request)?;
        input.push(b'\n');
        let mut written = 0;
        let mut reply = Vec::new();
        loop {
            if written < input.len() {
                match self
                    .0
                    .stdin
                    .as_mut()
                    .ok_or("missing worker input")?
                    .write(&input[written..])
                {
                    Ok(0) => return Err("validation worker closed its input".into()),
                    Ok(count) => written += count,
                    Err(error) if pending(&error) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            let mut buffer = [0; 65536];
            match self
                .0
                .stdout
                .as_mut()
                .ok_or("missing worker output")?
                .read(&mut buffer)
            {
                Ok(0) => {
                    return Err("validation worker exited; inspect validation.stderr.log".into());
                }
                Ok(count) => {
                    reply.extend_from_slice(&buffer[..count]);
                    require(
                        reply.len() <= 64 * 1024 * 1024,
                        "oversized validation worker response",
                    )?;
                    if reply.contains(&b'\n') {
                        let value: Value = serde_json::from_slice(&reply)?;
                        if let Some(error) = value.get("error") {
                            return Err(super::text(error)?.to_owned().into());
                        }
                        return Ok(super::field(&value, "result")?.clone());
                    }
                }
                Err(error) if pending(&error) => {}
                Err(error) => return Err(error.into()),
            }
            require(Instant::now() < deadline, "validation worker timed out")?;
            thread::sleep(Duration::from_millis(5));
        }
    }
}

fn pending(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = kill_process_group(Pid::from_child(&self.0), Signal::KILL);
        let _ = self.0.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn worker_keeps_state_reports_errors_and_reaps_on_drop() -> Result<()> {
        let mut worker = Worker::spawn(Command::new("python3").args(["-I", "-u", "-c",
            "import json,sys\nfor n,line in enumerate(sys.stdin):\n v=json.loads(line)\n print(json.dumps({'error':'rejected'} if v.get('fail') else {'result':n}),flush=True)"]))?;
        assert_eq!(worker.call(&json!({}))?, 0);
        assert!(
            worker
                .call(&json!({"fail":true}))
                .unwrap_err()
                .to_string()
                .contains("rejected")
        );
        assert_eq!(worker.call(&json!({}))?, 2);
        let pid = worker.0.id();
        drop(worker);
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        Ok(())
    }

    #[test]
    fn worker_timeout_and_early_exit_are_errors() -> Result<()> {
        for script in ["import time; time.sleep(30)", "pass"] {
            let mut worker = Worker::spawn(Command::new("python3").args(["-I", "-c", script]))?;
            assert!(
                worker
                    .exchange(&Value::Null, Duration::from_millis(100))
                    .is_err()
            );
        }
        Ok(())
    }
}
