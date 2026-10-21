use std::{
    collections::BTreeMap,
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::{Value, json};

use super::{
    BUCKET, affinity, array, digest, placement, properties, read_json, request, require, save,
    text, timestamp_ns, topology, verify_memory,
};
use crate::{
    Result,
    command::{command_output, run_command},
};

pub(in crate::selective_read) const GO_ENV: [(&str, &str); 7] = [
    ("GOTOOLCHAIN", "go1.24.7"),
    ("CGO_ENABLED", "0"),
    ("GOOS", "linux"),
    ("GOARCH", "amd64"),
    ("GOAMD64", "v1"),
    ("GOMAXPROCS", "8"),
    ("GOFLAGS", ""),
];
const COMMIT: &str = "07c3a429bfed433e49018cb0f78a52145d4bedeb";
const RELEASE: &str = "RELEASE.2025-09-07T16-13-09Z";

pub(in crate::selective_read) fn go() -> Command {
    let mut command = Command::new("go");
    command.envs(GO_ENV).env("GOEXPERIMENT", "");
    command
}

pub(in crate::selective_read) fn go_version() -> Result<String> {
    let version = command_output(go().arg("version"))?.trim().to_owned();
    require(
        version == "go version go1.24.7 linux/amd64",
        "benchmark server requires Go 1.24.7 linux/amd64",
    )?;
    Ok(version)
}

pub(in crate::selective_read) fn prepare(output: &Path, source: Option<&Path>) -> Result<()> {
    fs::create_dir(output)?;
    let output = fs::canonicalize(output)?;
    let source = match source {
        Some(source) => fs::canonicalize(source)?,
        None => {
            let source = output.join("source");
            run_command(
                Command::new("gh")
                    .args(["repo", "clone", "minio/minio"])
                    .arg(&source)
                    .args(["--", "--filter=blob:none", "--no-checkout"]),
            )?;
            run_command(
                Command::new("git")
                    .args(["checkout", "--detach", COMMIT])
                    .current_dir(&source),
            )?;
            source
        }
    };
    require(
        command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&source),
        )?
        .trim()
            == COMMIT,
        "MinIO source commit differs from the protocol pin",
    )?;
    let clean = || -> Result<()> {
        require(
            command_output(
                Command::new("git")
                    .args(["status", "--porcelain"])
                    .current_dir(&source),
            )?
            .is_empty(),
            "MinIO source is dirty",
        )
    };
    clean()?;
    let version = go_version()?;
    let ldflags = format!(
        "-s -w -X github.com/minio/minio/cmd.Version=2025-09-07T16:13:09Z -X github.com/minio/minio/cmd.ReleaseTag={RELEASE} -X github.com/minio/minio/cmd.CommitID={COMMIT} -X github.com/minio/minio/cmd.ShortCommitID={} -X github.com/minio/minio/cmd.CopyrightYear=2025",
        &COMMIT[..12]
    );
    let binary = output.join("minio");
    let command = vec![
        "go".to_owned(),
        "build".into(),
        "-mod=readonly".into(),
        "-p=8".into(),
        "-trimpath".into(),
        "-ldflags".into(),
        ldflags,
        "-o".into(),
        binary.to_str().ok_or("non-UTF8 MinIO output")?.into(),
        ".".into(),
    ];
    run_command(go().args(&command[1..]).current_dir(&source))?;
    clean()?;
    for name in ["go.mod", "go.sum", "LICENSE"] {
        fs::copy(source.join(name), output.join(name))?;
    }
    let root = PathBuf::from(
        command_output(Command::new("git").args(["rev-parse", "--show-toplevel"]))?.trim(),
    );
    let environment: BTreeMap<_, _> = GO_ENV[..6].iter().copied().collect();
    save(
        &output.join("build.json"),
        &json!({
            "source_commit": COMMIT, "release": RELEASE, "command": command, "environment": environment,
            "go_version": version, "binary_sha256": digest(&binary)?,
            "module_files": {"go.mod": digest(&output.join("go.mod"))?, "go.sum": digest(&output.join("go.sum"))?},
            "embedded_modules": command_output(go().args(["version", "-m"]).arg(&binary))?,
            "version": command_output(Command::new(&binary).arg("--version"))?,
            "harness_commit": command_output(Command::new("git").args(["rev-parse", "HEAD"]).current_dir(&root))?.trim(),
        }),
    )
}

pub(in crate::selective_read) fn random_hex(length: usize) -> Result<String> {
    let mut bytes = vec![0; length];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn cpu_list(cpus: &Value) -> Result<String> {
    Ok(array(cpus)?
        .iter()
        .map(|cpu| {
            cpu.as_u64()
                .map(|cpu| cpu.to_string())
                .ok_or_else(|| "invalid server CPU".into())
        })
        .collect::<Result<Vec<_>>>()?
        .join(" "))
}

pub(in crate::selective_read) fn start(build: &Path, directory: &Path, port: u16) -> Result<()> {
    require(
        (1024..u16::MAX).contains(&port),
        "use two unprivileged TCP ports",
    )?;
    let build = fs::canonicalize(build)?;
    let identity = read_json(&build.join("build.json"))?;
    require(
        identity["source_commit"] == COMMIT
            && identity["binary_sha256"] == digest(&build.join("minio"))?,
        "MinIO build differs from the pinned source or binary",
    )?;
    let rows = topology()?;
    let cpus = placement(array(&rows)?)?;
    DirBuilder::new().mode(0o700).create(directory)?;
    let directory = fs::canonicalize(directory)?;
    fs::create_dir(directory.join("data"))?;
    let secret = random_hex(32)?;
    let mut credentials = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.join("credentials.json"))?;
    serde_json::to_writer_pretty(
        &mut credentials,
        &json!({"access_key":"benchmark", "secret_key":secret}),
    )?;
    credentials.write_all(b"\n")?;
    let unit = format!("selective-read-minio-{}", random_hex(6)?);
    let settings = BTreeMap::from([
        ("MINIO_BROWSER", "off"),
        ("MINIO_PROMETHEUS_AUTH_TYPE", "public"),
        ("MINIO_UPDATE", "off"),
    ]);
    let inherited = command_output(Command::new("systemctl").args(["--user", "show-environment"]))?;
    let mut cleared: Vec<_> = inherited
        .lines()
        .filter_map(|line| line.split_once('=').map(|(name, _)| name))
        .filter(|name| {
            name.starts_with("MINIO_")
                && !settings.contains_key(name)
                && !matches!(*name, "MINIO_ROOT_USER" | "MINIO_ROOT_PASSWORD")
        })
        .collect();
    cleared.sort_unstable();
    cleared.extend(["GOGC", "GOMEMLIMIT", "GODEBUG", "GOMAXPROCS"]);
    let mut command = vec![
        "systemd-run".to_owned(),
        "--user".into(),
        "--quiet".into(),
        format!("--unit={unit}"),
        "--service-type=exec".into(),
        "-p".into(),
        "MemoryMax=4G".into(),
        "-p".into(),
        "MemorySwapMax=0".into(),
        "-p".into(),
        format!("CPUAffinity={}", cpu_list(&cpus["server"])?),
        "-p".into(),
        format!("UnsetEnvironment={}", cleared.join(" ")),
    ];
    command.extend(
        settings
            .keys()
            .copied()
            .chain(["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"])
            .map(|name| format!("--setenv={name}")),
    );
    command.extend([
        build
            .join("minio")
            .to_str()
            .ok_or("non-UTF8 MinIO path")?
            .into(),
        "server".into(),
        "--address".into(),
        format!("127.0.0.1:{port}"),
        "--console-address".into(),
        format!("127.0.0.1:{}", port + 1),
        directory
            .join("data")
            .to_str()
            .ok_or("non-UTF8 state path")?
            .into(),
    ]);
    let endpoint = format!("http://127.0.0.1:{port}");
    let curl = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|path| path.join("curl"))
        .find(|path| {
            fs::metadata(path).is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
        .ok_or("curl is not in PATH")?;
    let load: Vec<f64> = fs::read_to_string("/proc/loadavg")?
        .split_whitespace()
        .take(3)
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    save(
        &directory.join("server.json"),
        &json!({
            "format":"selective-read-storage-v1", "endpoint": endpoint, "bucket": BUCKET, "unit":unit,
            "build":build, "build_sha256":digest(&build.join("build.json"))?, "cpus":cpus, "settings":settings,
            "cache":"fresh clients; reused MinIO and OS caches; no cache flushing", "started_ns":timestamp_ns()?,
            "topology":rows, "kernel":fs::read_to_string("/proc/sys/kernel/osrelease")?.trim(),
            "cpu_info":fs::read_to_string("/proc/cpuinfo")?, "memory_info":fs::read_to_string("/proc/meminfo")?,
            "load_average":load, "storage":command_output(Command::new("findmnt").args(["--json","--target"]).arg(&directory))?,
            "curl_version":command_output(Command::new(&curl).arg("--version"))?, "curl_sha256":digest(&curl)?, "command":command,
        }),
    )?;
    let started = (|| -> Result<()> {
        run_command(
            Command::new(&command[0])
                .args(&command[1..])
                .envs(&settings)
                .env("MINIO_ROOT_USER", "benchmark")
                .env("MINIO_ROOT_PASSWORD", &secret),
        )?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let ready = Command::new(&curl)
                .args([
                    "--disable",
                    "--silent",
                    "--show-error",
                    "--noproxy",
                    "*",
                    "--fail",
                    "--max-time",
                    "1",
                    "--write-out",
                    "%{http_code}",
                ])
                .arg(format!("{endpoint}/minio/health/ready"))
                .output()?;
            if ready.status.success() && ready.stdout == b"200" {
                break;
            }
            require(Instant::now() < deadline, "MinIO did not become ready")?;
            thread::sleep(Duration::from_millis(100));
        }
        let mut actual = properties(&unit)?;
        let pid = rustix::process::Pid::from_raw(text(&actual["MainPID"])?.parse()?)
            .ok_or("invalid server PID")?;
        let affinity = affinity(Some(pid))?;
        require(json!(affinity) == cpus["server"], "server affinity changed")?;
        verify_memory(text(&actual["ControlGroup"])?, 4 * 1024u64.pow(3))?;
        require(
            digest(Path::new(&format!("/proc/{}/exe", pid.as_raw_nonzero())))?
                == identity["binary_sha256"],
            "running MinIO binary differs",
        )?;
        actual["affinity"] = json!(affinity);
        actual["verified_ns"] = json!(timestamp_ns()?);
        save(&directory.join("limits.json"), &actual)?;
        request(&directory, &format!("/{BUCKET}"), &["-X", "PUT"])?;
        Ok(())
    })();
    if started.is_err() {
        let _ = Command::new("systemctl")
            .args(["--user", "stop", &unit])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    started
}
