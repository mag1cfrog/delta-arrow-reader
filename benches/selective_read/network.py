"""Attach an opt-in, shared-bandwidth HTTP proxy to the benchmark MinIO server."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid
from urllib.parse import urlsplit
from urllib.request import Request

import storage
from storage import digest, save

HERE = Path(__file__).resolve().parent
BOUNDARY = "proxy client-facing HTTP ResponseWriter accepted body bytes"


def config(directory):
    path = directory / "network.json"
    if not path.exists():
        return None
    value = json.loads(path.read_text())
    endpoint = urlsplit(value["endpoint"])
    assert endpoint.scheme == "http" and endpoint.hostname == "127.0.0.1" and endpoint.port
    assert not endpoint.username and not endpoint.password and not endpoint.path and not endpoint.query and not endpoint.fragment
    assert value["upstream"] == storage.state(directory)["endpoint"], "proxy points to another server"
    return value


def control(directory, *, reset=False, traced=False, timeout=10):
    value = config(directory)
    if value is None:
        return None
    url = value["endpoint"] + "/_benchmark/network" + ("?trace=true" if traced else "")
    with storage.HTTP.open(Request(url, method="POST" if reset else "GET"), timeout=timeout) as response:
        result = json.load(response)
    assert result["profile"] == value["profile"], "proxy network profile changed"
    return result


def verify(directory):
    value = config(directory)
    if value is None:
        return
    output = Path(value["output"])
    assert json.loads((output / "network.json").read_text()) == value, "proxy record changed"
    assert digest(output / "proxy") == value["binary_sha256"], "proxy binary changed"
    actual = storage.properties(value["unit"])
    assert actual == value["limits"] and actual["ActiveState"] == "active", "proxy changed or stopped"
    assert sorted(os.sched_getaffinity(int(actual["MainPID"]))) == [value["cpu"]]
    group = Path("/sys/fs/cgroup" + actual["ControlGroup"])
    assert (group / "memory.max").read_text().strip() == str(512 * 1024**2)
    assert (group / "memory.swap.max").read_text().strip() == "0"
    control(directory)


def start(directory, output, port, latency_ms, jitter_ms, mbps, seed):
    storage.verify_server(directory)
    assert config(directory) is None, "stop the attached proxy before starting another"
    assert 1024 <= port < 65536 and 0 <= jitter_ms <= latency_ms <= 60000 and 0 <= mbps <= 100000
    assert len(seed.encode()) <= 128
    server = storage.state(directory)
    assert port not in (urlsplit(server["endpoint"]).port, urlsplit(server["endpoint"]).port + 1)
    occupied = {cpu for group in server["cpus"].values() for cpu in group}
    candidates = [row["cpu"] for row in storage.topology() if not occupied.intersection(
        {other["cpu"] for other in server["topology"] if (other["core"], other["socket"]) == (row["core"], row["socket"])})]
    assert candidates, "proxy needs one physical core separate from reader, MinIO and observer"
    output = output.resolve()
    output.mkdir()
    shutil.copyfile(HERE / "network_proxy.go", output / "network_proxy.go")
    environment = dict(os.environ, **storage.GO_ENV, GOFLAGS="", GOEXPERIMENT="")
    version = subprocess.check_output(["go", "version"], env=environment, text=True).strip()
    assert version == "go version go1.24.7 linux/amd64", version
    build = ["go", "build", "-trimpath", "-o", str(output / "proxy"), str(output / "network_proxy.go")]
    subprocess.run(build, env=environment, check=True)
    unit = "selective-read-network-" + uuid.uuid4().hex[:12]
    profile = dict(latency_ms=latency_ms, jitter_ms=jitter_ms, mbps=mbps, seed=seed)
    command = ["systemd-run", "--user", "--quiet", f"--unit={unit}", "--service-type=exec",
               "-p", "MemoryMax=512M", "-p", "MemorySwapMax=0", "-p", f"CPUAffinity={candidates[0]}",
               "-p", "UnsetEnvironment=GOGC GOMEMLIMIT GODEBUG", "--setenv=GOMAXPROCS=1",
               str(output / "proxy"), "--listen", f"127.0.0.1:{port}", "--upstream", server["endpoint"],
               "--latency-ms", str(latency_ms), "--jitter-ms", str(jitter_ms), "--mbps", str(mbps), "--seed", seed]
    value = dict(format="selective-read-network-v1", output=str(output), unit=unit, cpu=candidates[0],
                 endpoint=f"http://127.0.0.1:{port}", upstream=server["endpoint"], profile=profile,
                 jitter="uniform integer microseconds; SHA256(seed, method, request URI, Range); identical requests repeat",
                 byte_boundary=BOUNDARY, chunk_bytes=65536, go_version=version, build_command=build,
                 build_environment=storage.GO_ENV, binary_sha256=digest(output / "proxy"),
                 source_sha256={p.name: digest(p) for p in (Path(__file__), HERE / "network_proxy.go")},
                 command=command, started_ns=time.time_ns())
    subprocess.run(command, check=True)
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                with storage.HTTP.open(value["endpoint"] + "/_benchmark/network", timeout=1) as response:
                    assert json.load(response)["profile"] == profile
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("proxy did not become ready")
                time.sleep(.05)
        value["limits"] = storage.properties(unit)
        save(output / "network.json", value)
        save(directory / "network.json", value)
        verify(directory)
    except BaseException:
        subprocess.run(["systemctl", "--user", "stop", unit], check=False)
        (directory / "network.json").unlink(missing_ok=True)
        raise


def stop(directory):
    value = config(directory)
    if value is not None:
        # A transient service can disappear before stop (or while it runs).
        # Prove the process is gone instead of requiring systemctl to find it.
        subprocess.run(["systemctl", "--user", "stop", value["unit"]], check=False, capture_output=True)
        actual = storage.properties(value["unit"])
        assert actual["ActiveState"] != "active" and actual["MainPID"] == "0", "proxy did not stop"
        receipt = Path(value["output"]) / "stopped.json"
        if not receipt.exists():
            save(receipt, dict(stopped_ns=time.time_ns(), properties=actual))
        (directory / "network.json").unlink()


def finish(directory, output):
    value = control(directory)
    if value is None:
        return None
    assert value["active"] == 0 and not value["trace_truncated"], "proxy is active or trace is incomplete"
    records = value.pop("records", [])
    value.update(boundary=BOUNDARY, by_class={})
    if value["trace_enabled"]:
        assert len(records) == value["requests"] and sum(r["response_bytes"] for r in records) == value["response_bytes"]
        with (output / "network-requests.jsonl").open("x") as target:
            for record in records:
                target.write(json.dumps(record, sort_keys=True) + "\n")
                group = value["by_class"].setdefault(record["object_class"], dict(requests=0, response_bytes=0))
                group["requests"] += 1
                group["response_bytes"] += record["response_bytes"]
    save(output / "network-capture.json", value)
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("start", "stop"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--port", type=int, default=19002)
    parser.add_argument("--latency-ms", type=int, default=200)
    parser.add_argument("--jitter-ms", type=int, default=20)
    parser.add_argument("--mbps", type=int, default=150)
    parser.add_argument("--seed", default="0")
    args = parser.parse_args()
    with storage.exclusive(args.state):
        if args.command == "start":
            if args.output is None:
                parser.error("start requires --output")
            start(args.state, args.output, args.port, args.latency_ms, args.jitter_ms, args.mbps, args.seed)
        else:
            stop(args.state)
