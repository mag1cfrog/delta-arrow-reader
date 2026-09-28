"""Prepare and operate the dedicated, pinned loopback MinIO benchmark server."""

import argparse
import base64
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import platform
import resource
import secrets
import shutil
import subprocess
import sys
import time
from urllib.parse import quote, urlsplit
from urllib.request import build_opener, ProxyHandler

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import digest, save

COMMIT = "07c3a429bfed433e49018cb0f78a52145d4bedeb"
RELEASE = "RELEASE.2025-09-07T16-13-09Z"
BUCKET = "selective-read"
GO_ENV = {"GOTOOLCHAIN": "go1.24.7", "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64",
          "GOAMD64": "v1", "GOMAXPROCS": "8"}
HTTP = build_opener(ProxyHandler({}))


def prepare(output, source=None):
    output = output.resolve()
    output.mkdir()
    if source is None:
        source = output / "source"
        subprocess.run(["gh", "repo", "clone", "minio/minio", str(source), "--", "--filter=blob:none", "--no-checkout"], check=True)
        subprocess.run(["git", "checkout", "--detach", COMMIT], cwd=source, check=True)
    source = source.resolve()
    assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip() == COMMIT
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=source)
    environment = dict(os.environ, **GO_ENV, GOFLAGS="", GOEXPERIMENT="")
    version = subprocess.check_output(["go", "version"], env=environment, text=True).strip()
    assert version == "go version go1.24.7 linux/amd64", version
    ldflags = "-s -w " + " ".join(f"-X github.com/minio/minio/cmd.{k}={v}" for k, v in {
        "Version": "2025-09-07T16:13:09Z", "ReleaseTag": RELEASE, "CommitID": COMMIT,
        "ShortCommitID": COMMIT[:12], "CopyrightYear": "2025"}.items())
    command = ["go", "build", "-mod=readonly", "-p=8", "-trimpath", "-ldflags", ldflags, "-o", str(output / "minio"), "."]
    subprocess.run(command, cwd=source, env=environment, check=True)
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=source)
    for name in ("go.mod", "go.sum", "LICENSE"):
        shutil.copyfile(source / name, output / name)
    save(output / "build.json", {"source_commit": COMMIT, "release": RELEASE, "command": command, "environment": GO_ENV,
        "go_version": version, "binary_sha256": digest(output / "minio"),
        "module_files": {name: digest(output / name) for name in ("go.mod", "go.sum")},
        "embedded_modules": subprocess.check_output(["go", "version", "-m", str(output / "minio")], env=environment, text=True),
        "version": subprocess.check_output([str(output / "minio"), "--version"], text=True),
        "harness_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=HERE, text=True).strip(),
        "source_sha256": digest(Path(__file__))})


def topology():
    rows = []
    for cpu in sorted(os.sched_getaffinity(0)):
        root = Path(f"/sys/devices/system/cpu/cpu{cpu}")
        rows.append({"cpu": cpu, "core": int((root / "topology/core_id").read_text()),
                     "socket": int((root / "topology/physical_package_id").read_text()),
                     "siblings": (root / "topology/thread_siblings_list").read_text().strip(),
                     "governor": (root / "cpufreq/scaling_governor").read_text().strip()
                                  if (root / "cpufreq/scaling_governor").exists() else None})
    return rows


def placement(rows):
    groups = {}
    for row in rows:
        groups.setdefault((row["socket"], row["core"]), []).append(row["cpu"])
    cores = list(groups.values())
    readers = []
    while cores and len(readers) < 8:
        readers.extend(cores.pop(0)[:8 - len(readers)])
    if len(readers) != 8 or len(cores) < 3:
        raise ValueError("need eight reader CPUs plus two server cores and one observer core without shared SMT siblings")
    return {"reader": sorted(readers), "server": [cores[0][0], cores[1][0]], "observer": [cores[2][0]]}


def properties(unit):
    names = ("MainPID", "ControlGroup", "MemoryMax", "MemorySwapMax", "ActiveState")
    text = subprocess.check_output(["systemctl", "--user", "show", unit, *[f"--property={n}" for n in names]], text=True)
    return dict(line.split("=", 1) for line in text.splitlines())


def state(path):
    value = json.loads((path / "server.json").read_text())
    endpoint = urlsplit(value["endpoint"])
    assert endpoint.scheme == "http" and endpoint.hostname == "127.0.0.1" and endpoint.port
    assert not endpoint.username and not endpoint.password and not endpoint.path and not endpoint.query and not endpoint.fragment
    assert value["bucket"] == BUCKET
    return value


def verify_server(directory):
    config = state(directory)
    saved = json.loads((directory / "limits.json").read_text())
    actual = properties(config["unit"])
    assert actual == {k: saved[k] for k in actual} and actual["ActiveState"] == "active", "server changed or stopped"
    assert digest(Path(config["build"]) / "build.json") == config["build_sha256"], "server build record changed"
    group = Path("/sys/fs/cgroup" + actual["ControlGroup"])
    assert sorted(os.sched_getaffinity(int(actual["MainPID"]))) == config["cpus"]["server"]
    assert (group / "memory.max").read_text().strip() == str(4 * 1024**3)
    assert (group / "memory.swap.max").read_text().strip() == "0"


@contextmanager
def exclusive(directory):
    with (directory / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def credentials(directory):
    path = directory / "credentials.json"
    assert path.stat().st_mode & 0o077 == 0, "credentials must be private"
    return json.loads(path.read_text())


def curl(directory, path, *options, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    config = state(directory)
    secret = credentials(directory)
    # curl's native SigV4 signer avoids another SDK. Secrets travel on stdin,
    # never in argv, observation records, request URLs or a curl configuration file.
    text = f'user = "{secret["access_key"]}:{secret["secret_key"]}"\naws-sigv4 = "aws:amz:us-east-1:s3"\n'
    process = subprocess.Popen(["curl", "--disable", "--silent", "--show-error", "--noproxy", "*", "--config", "-",
        config["endpoint"] + path, *options], stdin=subprocess.PIPE, stdout=stdout, stderr=stderr)
    process.stdin.write(text.encode())
    process.stdin.close()
    process.stdin = None
    return process


def request(directory, path, *options):
    process = curl(directory, path, "--fail", "--max-time", "60", *options)
    body, error = process.communicate()
    if process.returncode:
        raise RuntimeError(f"local S3 request failed ({process.returncode}): {error.decode()}")
    return body


def start(build, directory, port):
    assert 1024 <= port < 65535, "use two unprivileged TCP ports"
    build = build.resolve()
    directory = directory.resolve()
    identity = json.loads((build / "build.json").read_text())
    assert identity["source_commit"] == COMMIT and identity["binary_sha256"] == digest(build / "minio")
    rows = topology()
    cpus = placement(rows)
    directory.mkdir()
    directory.chmod(0o700)
    (directory / "data").mkdir()
    secret = {"access_key": "benchmark", "secret_key": secrets.token_hex(32)}
    save(directory / "credentials.json", secret)
    (directory / "credentials.json").chmod(0o600)
    unit = "selective-read-minio-" + secrets.token_hex(6)
    settings = {"MINIO_BROWSER": "off", "MINIO_PROMETHEUS_AUTH_TYPE": "public", "MINIO_UPDATE": "off"}
    environment = dict(os.environ, **settings, MINIO_ROOT_USER=secret["access_key"], MINIO_ROOT_PASSWORD=secret["secret_key"])
    service_env = subprocess.check_output(["systemctl", "--user", "show-environment"], text=True)
    cleared = sorted(name for line in service_env.splitlines() if (name := line.split("=", 1)[0]).startswith("MINIO_")
                     and name not in (*settings, "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"))
    cleared += ["GOGC", "GOMEMLIMIT", "GODEBUG", "GOMAXPROCS"]
    command = ["systemd-run", "--user", "--quiet", f"--unit={unit}", "--service-type=exec",
               "-p", "MemoryMax=4G", "-p", "MemorySwapMax=0", "-p", "CPUAffinity=" + " ".join(map(str, cpus["server"])),
               "-p", "UnsetEnvironment=" + " ".join(cleared),
               *[f"--setenv={name}" for name in (*settings, "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")],
               str(build / "minio"), "server", "--address", f"127.0.0.1:{port}", "--console-address", f"127.0.0.1:{port + 1}", str(directory / "data")]
    config = {"format": "selective-read-storage-v1", "endpoint": f"http://127.0.0.1:{port}", "bucket": BUCKET,
              "unit": unit, "build": str(build), "build_sha256": digest(build / "build.json"), "cpus": cpus,
              "settings": settings, "source_sha256": digest(Path(__file__)),
              "cache": "fresh clients; reused MinIO and OS caches; no cache flushing",
              "started_ns": time.time_ns(), "topology": rows, "kernel": platform.release(),
              "cpu_info": Path("/proc/cpuinfo").read_text(), "memory_info": Path("/proc/meminfo").read_text(),
              "load_average": list(os.getloadavg()), "storage": subprocess.check_output(["findmnt", "--json", "--target", str(directory)], text=True),
              "curl_version": subprocess.check_output(["curl", "--version"], text=True),
              "curl_sha256": digest(Path(shutil.which("curl"))), "python": sys.version, "command": command}
    save(directory / "server.json", config)
    subprocess.run(command, env=environment, check=True)
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with HTTP.open(config["endpoint"] + "/minio/health/ready", timeout=1) as response:
                    assert response.status == 200
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("MinIO did not become ready")
                time.sleep(.1)
        actual = properties(unit)
        pid = int(actual["MainPID"])
        group = Path("/sys/fs/cgroup" + actual["ControlGroup"])
        assert sorted(os.sched_getaffinity(pid)) == cpus["server"]
        assert (group / "memory.max").read_text().strip() == str(4 * 1024**3)
        assert (group / "memory.swap.max").read_text().strip() == "0"
        assert digest(Path(f"/proc/{pid}/exe")) == identity["binary_sha256"]
        save(directory / "limits.json", actual | {"affinity": sorted(os.sched_getaffinity(pid)), "verified_ns": time.time_ns()})
        request(directory, "/" + BUCKET, "-X", "PUT")
    except BaseException:
        subprocess.run(["systemctl", "--user", "stop", unit], check=False)
        raise


def inventory(fixtures):
    manifest = json.loads((fixtures / "manifest.json").read_text())
    assert manifest["status"] == "complete" and manifest["protocol"] == "selective-read-v1"
    objects = []
    for table in manifest["tables"]:
        logs = table.get("delta_logs", [table["delta_log"]])
        dvs = [f["deletion_vector"] for f in table["files"] if "deletion_vector" in f]
        for item in logs + table["files"] + dvs:
            name = str(Path(table["path"]) / item["path"])
            path = (fixtures / name).resolve()
            assert path.is_relative_to(fixtures.resolve()) and path.is_file(), name
            assert path.stat().st_size == item["bytes"] and digest(path) == item["sha256"], name
            objects.append({"path": name, "bytes": item["bytes"], "sha256": item["sha256"]})
    assert len({o["path"] for o in objects}) == len(objects)
    return objects


def upload(directory, fixtures, output):
    objects = inventory(fixtures)
    prefix = digest(fixtures / "manifest.json")
    with exclusive(directory):
        verify_server(directory)
        for item in objects:
            put_verified(directory, prefix + "/" + item["path"], fixtures / item["path"], item["sha256"])
    save(output, {"status": "verified", "server_sha256": digest(directory / "server.json"),
                  "fixture_manifest_sha256": prefix, "prefix": prefix, "objects": objects,
                  "table_root": f"s3://{BUCKET}/{prefix}", "verified_ns": time.time_ns()})


def put_verified(directory, key, source, expected):
    import hashlib
    path = f"/{BUCKET}/" + quote(key, safe="/")
    # A content-addressed location is immutable, including repeat uploads.
    proc = curl(directory, path, "--upload-file", str(source.resolve()),
                "--header", "If-None-Match: *", "--header", "x-amz-checksum-sha256: " + base64.b64encode(bytes.fromhex(expected)).decode(),
                "--output", "/dev/null", "--write-out", "%{http_code}")
    status, error = proc.communicate()
    assert proc.returncode == 0 and status.decode() in ("200", "412"), (status, error)
    proc = curl(directory, path, "--fail")
    actual = hashlib.file_digest(proc.stdout, "sha256").hexdigest()
    error = proc.stderr.read()
    assert proc.wait() == 0 and actual == expected, (key, error)


def reader_environment(directory):
    config = state(directory)
    secret = credentials(directory)
    # A dedicated server must not inherit cloud profiles, metadata credentials,
    # alternate S3 endpoints or proxies from the invoking shell.
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AWS_", "MINIO_"))
           and k.lower() not in ("http_proxy", "https_proxy", "all_proxy", "no_proxy")}
    env.update(AWS_ACCESS_KEY_ID=secret["access_key"], AWS_SECRET_ACCESS_KEY=secret["secret_key"],
               AWS_REGION="us-east-1", AWS_DEFAULT_REGION="us-east-1", AWS_ENDPOINT_URL=config["endpoint"],
               AWS_ENDPOINT=config["endpoint"], AWS_ALLOW_HTTP="true", AWS_VIRTUAL_HOSTED_STYLE_REQUEST="false",
               AWS_EC2_METADATA_DISABLED="true", NO_PROXY="*", no_proxy="*")
    return env


def reader_prefix(directory, unit=None):
    cpus = state(directory)["cpus"]["reader"]
    return ["systemd-run", "--user", "--scope", "--quiet", "--collect", *(["--unit", unit] if unit else []), "-p", "MemoryMax=8G", "-p", "MemorySwapMax=0",
            "taskset", "--cpu-list", ",".join(map(str, cpus)), sys.executable, "-B", str(Path(__file__).resolve()),
            "reader", "--state", str(directory.resolve()), "--"]


def exec_reader(directory, arguments):
    # Verify the effective limits inside the scope before any reader table I/O.
    name = next(line.removeprefix("0::") for line in Path("/proc/self/cgroup").read_text().splitlines() if line.startswith("0::"))
    group = Path("/sys/fs/cgroup" + name)
    limits = {"cpu_affinity": sorted(os.sched_getaffinity(0)), "control_group": name,
              "max_file_size_bytes": resource.getrlimit(resource.RLIMIT_FSIZE)[0],
              "process_memory_bytes": int((group / "memory.max").read_text()),
              "swap_bytes": int((group / "memory.swap.max").read_text()), "verified_ns": time.time_ns(),
              "enforcement": "systemd user scope and taskset"}
    assert limits["cpu_affinity"] == state(directory)["cpus"]["reader"]
    assert limits["process_memory_bytes"] == 8 * 1024**3 and limits["swap_bytes"] == 0
    save(Path(arguments[-1]).parent / "limits.json", limits)
    os.execv(arguments[0], arguments)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("prepare")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--source", type=Path)
    p = commands.add_parser("start")
    p.add_argument("--build", type=Path, required=True)
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("--port", type=int, default=19000)
    p = commands.add_parser("stop")
    p.add_argument("--state", type=Path, required=True)
    p = commands.add_parser("upload")
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("--fixtures", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p = commands.add_parser("reader", help=argparse.SUPPRESS)
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.output, args.source)
    elif args.command == "start":
        start(args.build, args.state, args.port)
    elif args.command == "upload":
        upload(args.state, args.fixtures, args.output)
    elif args.command == "reader":
        exec_reader(args.state, args.arguments[1:])
    else:
        with exclusive(args.state):
            subprocess.run(["systemctl", "--user", "stop", state(args.state)["unit"]], check=True)
