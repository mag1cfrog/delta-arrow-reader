mod progress;

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    mem::MaybeUninit,
    os::{
        fd::AsRawFd,
        unix::process::{CommandExt, ExitStatusExt},
    },
    path::PathBuf,
    process::{Child, Command, ExitStatus},
    thread,
    time::Duration,
};

use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    io::{FdFlags, fcntl_setfd, ioctl_fionbio},
    pipe::{PipeFlags, pipe_with},
    process::{Pid, Signal, kill_process_group, setsid},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{Result, artifacts::file_sha256};
use progress::{Progress, require};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Launch {
    command: Vec<String>,
    payload: Value,
    output: PathBuf,
    stdout: PathBuf,
    stderr: PathBuf,
    env: Option<BTreeMap<String, String>>,
    query_seconds: f64,
    cleanup_seconds: f64,
}

fn monotonic_ns() -> io::Result<u64> {
    let mut time = MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes the supplied timespec on success.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, time.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the call above succeeded.
    let time = unsafe { time.assume_init() };
    Ok(time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64)
}

fn wait4(pid: u32) -> io::Result<Option<(i32, libc::rusage)>> {
    let mut status = 0;
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: status and usage are writable; only this thread reaps this child.
    let waited = unsafe { libc::wait4(pid as i32, &mut status, libc::WNOHANG, usage.as_mut_ptr()) };
    if waited < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::Interrupted {
            Ok(None)
        } else {
            Err(error)
        };
    }
    if waited == 0 {
        return Ok(None);
    }
    let exit = ExitStatus::from_raw(status);
    // SAFETY: wait4 returned the child's PID and initialized usage.
    Ok(Some((
        exit.code().unwrap_or_else(|| -exit.signal().unwrap_or(0)),
        unsafe { usage.assume_init() },
    )))
}

fn kill_group(child: &Child) -> io::Result<()> {
    match kill_process_group(Pid::from_child(child), Signal::KILL) {
        Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

struct Events {
    pipe: File,
    saved: File,
    pending: Vec<u8>,
    open: bool,
}

impl Events {
    fn drain(&mut self, progress: &mut Progress, failure: &mut Option<io::Error>) -> Result<()> {
        let mut buffer = [0; 65536];
        // Bound each pass so a flooding child cannot starve its watchdog.
        for _ in 0..16 {
            let count = match self.pipe.read(&mut buffer) {
                Ok(count) => count,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            };
            if count == 0 {
                self.open = false;
                if !self.pending.is_empty() && failure.is_none() {
                    *failure = Some(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated watchdog message",
                    ));
                }
                return Ok(());
            }
            self.saved.write_all(&buffer[..count])?;
            if failure.is_some() {
                continue;
            }
            self.pending.extend_from_slice(&buffer[..count]);
            let parsed = (|| -> io::Result<()> {
                while let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
                    require(end <= 4096, "oversized watchdog message")?;
                    let message = serde_json::from_slice(&self.pending[..end])
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    progress.receive(&message, monotonic_ns()?)?;
                    self.pending.drain(..=end);
                }
                require(self.pending.len() <= 4096, "oversized watchdog message")
            })();
            if let Err(error) = parsed {
                *failure = Some(error);
                self.pending.clear();
            }
        }
        Ok(())
    }
}

pub(super) fn launch(input: Launch) -> Result<Value> {
    let executable_hash = file_sha256(&std::env::current_exe()?)?;
    let start = monotonic_ns()?;
    let mut progress = Progress::new(
        &input.payload,
        input.query_seconds,
        input.cleanup_seconds,
        start,
    )?;
    let program = input.command.first().ok_or("empty reader command")?;
    let mut command = Command::new(program);
    command.args(&input.command[1..]);
    if let Some(environment) = &input.env {
        command.env_clear().envs(environment);
    }
    let (read, write) = pipe_with(PipeFlags::CLOEXEC)?;
    ioctl_fionbio(&read, true)?;
    command
        .stdout(OpenOptions::new().append(true).open(&input.stdout)?)
        .stderr(OpenOptions::new().append(true).open(&input.stderr)?)
        .env("SELECTIVE_READ_CONTROL_FD", write.as_raw_fd().to_string());
    // SAFETY: the child only changes its session and descriptor flags before exec.
    unsafe {
        command.pre_exec(move || {
            setsid()?;
            fcntl_setfd(&write, FdFlags::empty())?;
            // Keep the descriptor open until exec; the closure is not dropped in the child.
            Ok(())
        });
    }
    let mut events = Events {
        pipe: File::from(read),
        saved: File::options()
            .write(true)
            .create_new(true)
            .open(input.output.join("progress.jsonl"))?,
        pending: Vec::new(),
        open: true,
    };
    let mut child = command.spawn()?;
    drop(command); // Close the parent's copy of the progress writer.
    let result = watch(
        &child,
        &mut events,
        &mut progress,
        start,
        &input,
        &executable_hash,
    );
    if result.is_err() {
        let _ = kill_group(&child);
        let until = monotonic_ns()?.saturating_add(progress.cleanup_ns);
        while monotonic_ns()? < until {
            match child.try_wait() {
                Ok(None) => thread::sleep(Duration::from_millis(10)),
                _ => break,
            }
        }
    }
    result
}

fn watch(
    child: &Child,
    events: &mut Events,
    progress: &mut Progress,
    start: u64,
    input: &Launch,
    executable_hash: &str,
) -> Result<Value> {
    let mut failure = None;
    let mut kill_deadline = None;
    let mut reaped = None;
    loop {
        events.drain(progress, &mut failure)?;
        if let Some(result) = wait4(child.id())? {
            reaped = Some(result);
            events.drain(progress, &mut failure)?;
            break;
        }
        let now = monotonic_ns()?;
        if failure.is_none() && now >= progress.deadline {
            failure = Some(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{} exceeded its deadline", progress.phase),
            ));
        }
        if failure.is_some() {
            match kill_deadline {
                None => {
                    progress.cleanup_started_ns.get_or_insert(now);
                    kill_deadline = Some(now + progress.cleanup_ns);
                }
                Some(deadline) if now >= deadline => {
                    failure = Some(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "reader did not exit after SIGKILL; cleanup unverified",
                    ));
                    break;
                }
                _ => {}
            }
            kill_group(child)?;
        }
        if events.open {
            let nanos = progress.deadline.saturating_sub(now).min(20_000_000);
            let timeout = Timespec {
                tv_sec: 0,
                tv_nsec: nanos as _,
            };
            match poll(
                &mut [PollFd::new(&events.pipe, PollFlags::IN)],
                Some(&timeout),
            ) {
                Ok(_) | Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        } else {
            thread::sleep(Duration::from_millis(10));
        }
    }
    let cpu = |time: libc::timeval| time.tv_sec as u64 * 1_000_000_000 + time.tv_usec as u64 * 1000;
    let usage = reaped.as_ref().map(|(_, usage)| usage);
    Ok(json!({
        "status": failure.as_ref().map_or("success", |error| if error.kind() == io::ErrorKind::TimedOut { "timeout" } else { "operational_failure" }),
        "failure_reason": failure.as_ref().map(ToString::to_string),
        "returncode": reaped.as_ref().map(|(code, _)| code),
        "last_phase": progress.phase,
        "completed_queries": progress.queries,
        "initialization_ns": progress.initialization_ns,
        "cleanup_started_monotonic_ns": progress.cleanup_started_ns,
        "query_deadline_seconds": input.query_seconds,
        "cleanup_deadline_seconds": input.cleanup_seconds,
        "process_elapsed_ns": monotonic_ns()? - start,
        "process_user_cpu_ns": usage.map(|usage| cpu(usage.ru_utime)),
        "process_system_cpu_ns": usage.map(|usage| cpu(usage.ru_stime)),
        "process_cpu_ns": usage.map(|usage| cpu(usage.ru_utime) + cpu(usage.ru_stime)),
        "peak_rss_bytes": usage.map(|usage| usage.ru_maxrss as u64 * 1024),
        "resource_unavailable_reason": if usage.is_some() { None } else { Some("reader did not exit; wait4 accounting unavailable") },
        "resource_boundary": "Linux wait4 for the whole reader process including startup/shutdown; waited descendants contribute CPU and maximum individual RSS",
        "cpu_resolution_ns": 1000, "rss_resolution_bytes": 1024,
        "supervisor_executable_sha256": executable_hash,
    }))
}
