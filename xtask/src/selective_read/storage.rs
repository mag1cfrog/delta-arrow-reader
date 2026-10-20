use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::Write,
    os::{
        fd::AsRawFd,
        unix::{fs::PermissionsExt, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use rustix::{
    process::{Pid, Resource, getrlimit},
    thread::{CpuSet, sched_getaffinity},
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::{array, object, require, text};
use crate::{
    Result,
    artifacts::{reader_sha256, write_new_file},
    command::command_output,
    selective_read_build::digest,
};

pub(super) const BUCKET: &str = "selective-read";

pub(super) mod service;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Request {
    Topology,
    Placement {
        rows: Vec<Value>,
    },
    Properties {
        unit: String,
    },
    State {
        directory: PathBuf,
    },
    VerifyServer {
        directory: PathBuf,
    },
    Credentials {
        directory: PathBuf,
    },
    ReaderEnvironment {
        directory: PathBuf,
    },
    ReaderPrefix {
        directory: PathBuf,
        unit: Option<String>,
    },
    PutVerified {
        directory: PathBuf,
        key: String,
        source: PathBuf,
        expected: String,
        checksum: bool,
    },
    #[serde(rename = "request")]
    Http {
        directory: PathBuf,
        path: String,
        options: Vec<String>,
    },
}

pub(super) fn run(operation: Request) -> Result<Value> {
    match operation {
        Request::Topology => topology(),
        Request::Placement { rows } => placement(&rows),
        Request::Properties { unit } => properties(&unit),
        Request::State { directory } => state(&directory),
        Request::VerifyServer { directory } => {
            verify_server(&directory)?;
            Ok(Value::Null)
        }
        Request::Credentials { directory } => credentials(&directory),
        Request::ReaderEnvironment { directory } => reader_environment(&directory),
        Request::ReaderPrefix { directory, unit } => reader_prefix(&directory, unit.as_deref()),
        Request::PutVerified {
            directory,
            key,
            source,
            expected,
            checksum,
        } => Ok(json!(put_verified(
            &directory, &key, &source, &expected, checksum
        )?)),
        Request::Http {
            directory,
            path,
            options,
        } => Ok(json!(STANDARD.encode(request(
            &directory,
            &path,
            &options.iter().map(String::as_str).collect::<Vec<_>>()
        )?))),
    }
}

pub(super) fn read_json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_reader(File::open(path)?)?)
}

pub(super) fn save(path: &Path, value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_new_file(path, &bytes)?;
    Ok(())
}

pub(super) fn exclusive(directory: &Path) -> Result<File> {
    let lock = OpenOptions::new()
        .append(true)
        .create(true)
        .open(directory.join(".lock"))?;
    // SAFETY: lock owns a valid file descriptor; closing it releases the lock.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(lock)
}

pub(super) fn timestamp_ns() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()?)
}

pub(super) fn affinity(pid: Option<Pid>) -> Result<Vec<usize>> {
    let cpus = sched_getaffinity(pid)?;
    Ok((0..CpuSet::MAX_CPU)
        .filter(|cpu| cpus.is_set(*cpu))
        .collect())
}

pub(super) fn topology() -> Result<Value> {
    let mut rows = Vec::new();
    for cpu in affinity(None)? {
        let root = PathBuf::from(format!("/sys/devices/system/cpu/cpu{cpu}"));
        let core: i64 = fs::read_to_string(root.join("topology/core_id"))?
            .trim()
            .parse()?;
        let socket: i64 = fs::read_to_string(root.join("topology/physical_package_id"))?
            .trim()
            .parse()?;
        let siblings = fs::read_to_string(root.join("topology/thread_siblings_list"))?;
        let governor = root.join("cpufreq/scaling_governor");
        let governor = if governor.exists() {
            Some(fs::read_to_string(governor)?.trim().to_owned())
        } else {
            None
        };
        rows.push(json!({"cpu": cpu, "core": core, "socket": socket, "siblings": siblings.trim(), "governor": governor}));
    }
    Ok(json!(rows))
}

fn placement(rows: &[Value]) -> Result<Value> {
    let mut groups: Vec<((i64, i64), Vec<u64>)> = Vec::new();
    for row in rows {
        let key = (
            row["socket"].as_i64().ok_or("invalid CPU socket")?,
            row["core"].as_i64().ok_or("invalid CPU core")?,
        );
        let cpu = row["cpu"].as_u64().ok_or("invalid CPU index")?;
        if let Some((_, cpus)) = groups.iter_mut().find(|(group, _)| *group == key) {
            cpus.push(cpu);
        } else {
            groups.push((key, vec![cpu]));
        }
    }
    let mut cores = groups.into_iter().map(|(_, cpus)| cpus);
    let mut readers = Vec::new();
    while readers.len() < 8 {
        let Some(core) = cores.next() else {
            break;
        };
        readers.extend(core.into_iter().take(8 - readers.len()));
    }
    let remaining: Vec<_> = cores.collect();
    require(
        readers.len() == 8 && remaining.len() >= 3,
        "need eight reader CPUs plus two server cores and one observer core without shared SMT siblings",
    )?;
    readers.sort_unstable();
    Ok(
        json!({"reader": readers, "server": [remaining[0][0], remaining[1][0]], "observer": [remaining[2][0]]}),
    )
}

pub(super) fn properties(unit: &str) -> Result<Value> {
    require(
        !unit.starts_with('-') && !unit.is_empty(),
        "invalid service unit",
    )?;
    let output = command_output(
        Command::new("systemctl")
            .args(["--user", "show", unit])
            .args(
                [
                    "MainPID",
                    "ControlGroup",
                    "MemoryMax",
                    "MemorySwapMax",
                    "ActiveState",
                ]
                .map(|name| format!("--property={name}")),
            ),
    )?;
    let values: Map<_, _> = output
        .lines()
        .map(|line| {
            let (key, value) = line.split_once('=').ok_or("invalid systemctl properties")?;
            Ok((key.to_owned(), json!(value)))
        })
        .collect::<Result<_>>()?;
    Ok(Value::Object(values))
}

pub(super) fn endpoint(value: &Value) -> Result<&str> {
    let endpoint = text(&value["endpoint"])?;
    let url = url::Url::parse(endpoint)?;
    require(
        url.port().is_some() && endpoint == format!("http://127.0.0.1:{}", url.port().unwrap()),
        "expected a plain loopback HTTP endpoint",
    )?;
    Ok(endpoint)
}

pub(super) fn state(directory: &Path) -> Result<Value> {
    let value = read_json(&directory.join("server.json"))?;
    endpoint(&value)?;
    require(value["bucket"] == BUCKET, "unexpected benchmark bucket")?;
    Ok(value)
}

pub(super) fn verify_server(directory: &Path) -> Result<()> {
    let config = state(directory)?;
    let saved = read_json(&directory.join("limits.json"))?;
    let actual = properties(text(&config["unit"])?)?;
    require(
        actual["ActiveState"] == "active"
            && object(&actual)?
                .iter()
                .all(|(k, v)| saved.get(k) == Some(v)),
        "server changed or stopped",
    )?;
    require(
        config["build_sha256"] == digest(&Path::new(text(&config["build"])?).join("build.json"))?,
        "server build record changed",
    )?;
    let pid = Pid::from_raw(text(&actual["MainPID"])?.parse()?).ok_or("invalid server PID")?;
    require(
        json!(affinity(Some(pid))?) == config["cpus"]["server"],
        "server affinity changed",
    )?;
    verify_memory(text(&actual["ControlGroup"])?, 4 * 1024u64.pow(3))?;
    super::network::verify(directory)?;
    Ok(())
}

fn cgroup(name: &str) -> Result<PathBuf> {
    require(
        name.starts_with('/') && !name.split('/').any(|part| part == ".."),
        "invalid cgroup path",
    )?;
    Ok(PathBuf::from(format!("/sys/fs/cgroup{name}")))
}

pub(super) fn verify_memory(name: &str, bytes: u64) -> Result<()> {
    let group = cgroup(name)?;
    require(
        fs::read_to_string(group.join("memory.max"))?.trim() == bytes.to_string()
            && fs::read_to_string(group.join("memory.swap.max"))?.trim() == "0",
        "effective memory or swap limit changed",
    )
}

pub(super) fn credentials(directory: &Path) -> Result<Value> {
    let path = directory.join("credentials.json");
    require(
        fs::metadata(&path)?.permissions().mode() & 0o077 == 0,
        "credentials must be private",
    )?;
    let value = read_json(&path)?;
    for key in ["access_key", "secret_key"] {
        require(
            text(&value[key])?
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
                && !text(&value[key])?.is_empty(),
            "invalid dedicated benchmark credentials",
        )?;
    }
    Ok(value)
}

pub(super) fn curl(directory: &Path, path: &str, options: &[&str]) -> Result<Child> {
    let config = state(directory)?;
    let secret = credentials(directory)?;
    let configuration = format!(
        "user = \"{}:{}\"\naws-sigv4 = \"aws:amz:us-east-1:s3\"\n",
        text(&secret["access_key"])?,
        text(&secret["secret_key"])?
    );
    let mut child = Command::new("curl")
        .args([
            "--disable",
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--config",
            "-",
        ])
        .arg(format!("{}{path}", endpoint(&config)?))
        .args(options)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    if let Err(error) = child
        .stdin
        .take()
        .unwrap()
        .write_all(configuration.as_bytes())
    {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error.into());
    }
    Ok(child)
}

pub(super) fn response(child: Child) -> Result<Vec<u8>> {
    let output = child.wait_with_output()?;
    require(
        output.status.success(),
        &format!(
            "local S3 request failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
    )?;
    Ok(output.stdout)
}

pub(super) fn request(directory: &Path, path: &str, options: &[&str]) -> Result<Vec<u8>> {
    let mut arguments = vec!["--fail", "--max-time", "60"];
    arguments.extend(options);
    response(curl(directory, path, &arguments)?)
}

pub(super) fn http(url: &str, method: &str, timeout: f64) -> Result<Vec<u8>> {
    require(timeout.is_finite() && timeout > 0.0, "invalid HTTP timeout")?;
    response(
        Command::new("curl")
            .args([
                "--disable",
                "--silent",
                "--show-error",
                "--noproxy",
                "*",
                "--fail",
                "--max-time",
                &timeout.to_string(),
                "-X",
                method,
                url,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    )
}

fn object_path(key: &str) -> String {
    let mut result = format!("/{BUCKET}/");
    for byte in key.bytes() {
        if byte.is_ascii_alphanumeric() || b"/-._~".contains(&byte) {
            result.push(byte as char);
        } else {
            result.push_str(&format!("%{byte:02X}"));
        }
    }
    result
}

fn checksum_verified(headers: &[u8], size: u64, expected: &str) -> Result<bool> {
    let headers = std::str::from_utf8(headers)?;
    require(
        headers.ends_with("200"),
        "object HEAD request did not return 200",
    )?;
    let mut fields = BTreeMap::<String, Vec<String>>::new();
    for line in headers[..headers.len() - 3]
        .split("\r\n")
        .skip(1)
        .filter(|line| !line.is_empty())
    {
        let (name, value) = line
            .split_once(':')
            .ok_or("invalid object response headers")?;
        fields
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value.trim().to_owned());
    }
    require(
        fields.get("content-length") == Some(&vec![size.to_string()]),
        "stored object length changed",
    )?;
    for name in ["x-amz-checksum-sha256", "x-amz-checksum-type"] {
        require(
            fields.get(name).is_none_or(|values| values.len() == 1),
            "duplicate checksum headers",
        )?;
    }
    if let Some(sha) = fields.get("x-amz-checksum-sha256")
        && fields.get("x-amz-checksum-type") == Some(&vec!["FULL_OBJECT".into()])
    {
        require(
            sha == &vec![expected.to_owned()],
            "stored object checksum changed",
        )?;
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn put_verified(
    directory: &Path,
    key: &str,
    source: &Path,
    expected: &str,
    checksum: bool,
) -> Result<&'static str> {
    require(
        expected.len() == 64 && expected.bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid SHA-256 checksum",
    )?;
    let bytes: Vec<u8> = (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(&expected[i..i + 2], 16))
        .collect::<std::result::Result<_, _>>()?;
    let checksum_header = STANDARD.encode(bytes);
    let header = format!("x-amz-checksum-sha256: {checksum_header}");
    let source = fs::canonicalize(source)?;
    let path = object_path(key);
    let status = response(curl(
        directory,
        &path,
        &[
            "--upload-file",
            source.to_str().ok_or("non-UTF8 source path")?,
            "--header",
            "If-None-Match: *",
            "--header",
            &header,
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}",
        ],
    )?)?;
    require(
        matches!(status.as_slice(), b"200" | b"412"),
        "immutable object upload failed",
    )?;
    if checksum {
        let headers = response(curl(
            directory,
            &path,
            &[
                "--head",
                "--fail",
                "--header",
                "x-amz-checksum-mode: ENABLED",
                "--write-out",
                "%{http_code}",
            ],
        )?)?;
        if checksum_verified(&headers, fs::metadata(&source)?.len(), &checksum_header)? {
            return Ok("server-sha256");
        }
    }
    let mut child = curl(directory, &path, &["--fail"])?;
    let hash = reader_sha256(child.stdout.take().unwrap());
    let output = child.wait_with_output()?;
    require(
        output.status.success() && hash? == expected,
        "stored object readback checksum changed",
    )?;
    Ok("full-get")
}

pub(super) fn reader_environment(directory: &Path) -> Result<Value> {
    let config = state(directory)?;
    let proxy = directory.join("network.json");
    let network = if proxy.exists() {
        Some(read_json(&proxy)?)
    } else {
        None
    };
    if let Some(network) = &network {
        endpoint(network)?;
        require(
            network["upstream"] == config["endpoint"],
            "proxy points to another server",
        )?;
    }
    let endpoint = endpoint(network.as_ref().unwrap_or(&config))?;
    let secret = credentials(directory)?;
    let mut env: BTreeMap<_, _> = std::env::vars()
        .filter(|(key, _)| {
            !key.starts_with("AWS_")
                && !key.starts_with("MINIO_")
                && !matches!(
                    key.to_ascii_lowercase().as_str(),
                    "http_proxy" | "https_proxy" | "all_proxy" | "no_proxy"
                )
        })
        .collect();
    for (key, value) in [
        ("AWS_ACCESS_KEY_ID", text(&secret["access_key"])?),
        ("AWS_SECRET_ACCESS_KEY", text(&secret["secret_key"])?),
        ("AWS_REGION", "us-east-1"),
        ("AWS_DEFAULT_REGION", "us-east-1"),
        ("AWS_ENDPOINT_URL", endpoint),
        ("AWS_ENDPOINT", endpoint),
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("NO_PROXY", "*"),
        ("no_proxy", "*"),
    ] {
        env.insert(key.to_owned(), value.to_owned());
    }
    Ok(json!(env))
}

pub(super) fn reader_prefix(directory: &Path, unit: Option<&str>) -> Result<Value> {
    let config = state(directory)?;
    let cpus = array(&config["cpus"]["reader"])?
        .iter()
        .map(|v| {
            v.as_u64()
                .map(|cpu| cpu.to_string())
                .ok_or_else(|| "invalid reader CPU".into())
        })
        .collect::<Result<Vec<_>>>()?
        .join(",");
    let mut prefix = vec!["systemd-run", "--user", "--scope", "--quiet", "--collect"];
    if let Some(unit) = unit {
        prefix.extend(["--unit", unit]);
    }
    let binary = std::env::current_exe()?;
    let directory = fs::canonicalize(directory)?;
    prefix.extend([
        "-p",
        "MemoryMax=8G",
        "-p",
        "MemorySwapMax=0",
        "taskset",
        "--cpu-list",
        &cpus,
        binary.to_str().ok_or("non-UTF8 controller path")?,
        "selective-read",
        "exec-reader",
        "--state",
        directory.to_str().ok_or("non-UTF8 state path")?,
        "--",
    ]);
    Ok(json!(prefix))
}

pub(super) fn exec_reader(directory: &Path, arguments: &[OsString]) -> Result<()> {
    require(!arguments.is_empty(), "missing reader command")?;
    let config = state(directory)?;
    let groups = fs::read_to_string("/proc/self/cgroup")?;
    let name = groups
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or("missing unified cgroup")?;
    let cpus = affinity(None)?;
    require(
        json!(cpus) == config["cpus"]["reader"],
        "effective reader affinity changed",
    )?;
    verify_memory(name, 8 * 1024u64.pow(3))?;
    let output = Path::new(arguments.last().unwrap())
        .parent()
        .ok_or("invalid reader output path")?;
    save(
        &output.join("limits.json"),
        &json!({"cpu_affinity": cpus, "control_group": name,
        "max_file_size_bytes": getrlimit(Resource::Fsize).current.map(|v| json!(v)).unwrap_or(json!(-1)),
        "process_memory_bytes": 8 * 1024u64.pow(3), "swap_bytes": 0, "verified_ns": timestamp_ns()?,
        "enforcement": "systemd user scope and taskset"}),
    )?;
    Err(Command::new(&arguments[0])
        .args(&arguments[1..])
        .exec()
        .into())
}

#[cfg(test)]
mod tests;
