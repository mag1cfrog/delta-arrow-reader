"""Run committed SQL benchmark processes with the macOS session controls."""

from contextlib import contextmanager
import ctypes
import fcntl
import gzip
import json
import os
from pathlib import Path
import platform
import re
import runpy
from statistics import median
import subprocess
import sys
import time
from unittest.mock import patch

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
BENCHES = HERE.parent
REPO = HERE.parents[3]
MAC = ("Mac14,6", "Apple M2 Max", "26.7.3", "25G342")
BUILD_PROCESSES = {"cargo", "rustc", "sccache", "clang", "clang++", "swiftc", "xcodebuild", "ld", "ld64"}
BENCHMARK_PROCESSES = {"functions", "execution", "planning", "nullif", "nullif_620", "array_contains",
                       "bounded_windows", "pending_range_isolated", "planning_attribution",
                       "functions_1406_driver", "dar", "selective-read-fixtures", "limactl"}
OMITTED_CONTROLS = ["CPU pinning", "address-space limit", "peak RSS", "Linux CPU and governor metadata"]


def command(*args):
    return subprocess.check_output(args, text=True, timeout=15).strip()


def check_host():
    assert sys.platform == "darwin" and platform.machine() == "arm64" and __debug__
    identity = (command("/usr/sbin/sysctl", "-n", "hw.model"),
                command("/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"),
                command("/usr/bin/sw_vers", "-productVersion"),
                command("/usr/bin/sw_vers", "-buildVersion"))
    assert identity == MAC, "host differs from the committed machine"
    power = command("/usr/bin/pmset", "-g", "batt").splitlines()[0]
    modes = {parts[0]: parts[1] for line in command("/usr/bin/pmset", "-g").splitlines()
             if (parts := line.split()) and parts[0] in ("lowpowermode", "powermode")}
    assert "'AC Power'" in power and modes.get("powermode") == "2" and modes.get("lowpowermode", "0") == "0"
    running = {Path(line.strip()).name for line in command("/bin/ps", "-A", "-o", "comm=").splitlines()}
    competing = running & (BUILD_PROCESSES | BENCHMARK_PROCESSES)
    assert not competing, f"build, benchmark or VM processes are active: {sorted(competing)}"
    return dict(zip(("model", "chip", "macos", "build"), identity), power="mains, High Power",
                qos="user-interactive, set and checked on main and benchmark worker threads",
                omitted_controls=OMITTED_CONTROLS)


def prefer_performance_cores():
    libc = ctypes.CDLL(None)
    set_qos = libc.pthread_set_qos_class_self_np
    set_qos.argtypes = [ctypes.c_uint, ctypes.c_int]
    set_qos.restype = ctypes.c_int
    get_qos = libc.qos_class_self
    get_qos.argtypes = []
    get_qos.restype = ctypes.c_uint
    assert set_qos(0x21, 0) == 0 and get_qos() == 0x21, "interactive QoS was not set"


@contextmanager
def session(protocol):
    protocol = protocol.resolve()
    assert not command("git", "-C", str(REPO), "status", "--porcelain")
    head = command("git", "-C", str(REPO), "rev-parse", "HEAD")
    assert subprocess.check_output(["git", "-C", str(REPO), "show",
                                    f"{head}:{protocol.relative_to(REPO)}"]) == protocol.read_bytes()
    host = check_host()
    prefer_performance_cores()
    lock = REPO.parent / ".dar-macos-timing.lock"
    with lock.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with subprocess.Popen(["/usr/bin/caffeinate", "-is", "-w", str(os.getpid())]) as awake:
            try:
                assert awake.poll() is None
                yield {"protocol_commit": head, "host": host}
                check_host()
            finally:
                if awake.poll() is None:
                    awake.terminate()
                    awake.wait(timeout=10)


def strip_controls(argv):
    args = list(argv)
    if args[:2] == ["taskset", "-c"]:
        args = args[3:]
    if args[:1] == ["prlimit"]:
        assert args[1].startswith("--as=") and args[2] == "--"
        args = args[3:]
    return args


@contextmanager
def legacy_controls():
    """Adapt process creation for the selected frozen driver's per-cell call."""
    run = subprocess.run

    def native_run(argv, **kwargs):
        args = list(argv)
        if args[:1] == ["timeout"]:
            bound = int(args[1].removesuffix("s"))
            kwargs["timeout"] = min(kwargs.get("timeout", bound), bound)
            args = args[2:]
        kwargs.pop("preexec_fn", None)
        return run(strip_controls(args), **kwargs)

    with patch.object(subprocess, "run", native_run):
        yield


@contextmanager
def legacy_runner(relative):
    """Import one historical runner without invoking its Linux schedule."""
    source = BENCHES / relative
    sys.path.insert(0, str(source.parent))
    collisions = {path.stem: sys.modules.pop(path.stem) for path in source.parent.glob("*.py")
                  if path.stem in sys.modules}
    prior = set(sys.modules)
    try:
        with legacy_controls():
            yield runpy.run_path(str(source))
    finally:
        sys.path.pop(0)
        for name in set(sys.modules) - prior:
            module_path = getattr(sys.modules[name], "__file__", None)
            if module_path and Path(module_path).is_relative_to(BENCHES):
                sys.modules.pop(name)
        sys.modules.update(collisions)


def safe_record(value, roots):
    """Replace run-local path prefixes before serializing benchmark evidence."""
    if isinstance(value, dict):
        return {key: safe_record(item, roots) for key, item in value.items()}
    if isinstance(value, list):
        return [safe_record(item, roots) for item in value]
    if isinstance(value, str):
        for root, label in roots:
            value = value.replace(str(root), label)
        assert not re.search(r"/(Users|home)/[^/\s]+", value), "unmapped home path in evidence"
    return value


def call(binary, args, destination, timeout):
    assert timeout > 0
    destination.mkdir(parents=True, exist_ok=False)
    name = destination / "stdout.jsonl"
    with name.open("xb") as stdout, (destination / "stderr.log").open("xb") as stderr:
        try:
            process = subprocess.run([binary, *map(str, args)], stdout=stdout, stderr=stderr,
                                     timeout=timeout, env={"PATH": os.defpath, "TZ": "UTC"})
            status = "ok" if process.returncode == 0 else f"exit_{process.returncode}"
        except subprocess.TimeoutExpired:
            status = "timeout"
    rows = []
    with name.open("rb") as stream:
        for line in stream:
            if line.endswith(b"\n") and line.startswith(b"{"):
                rows.append(json.loads(line))
    return status, rows


@contextmanager
def archive(destination):
    with destination.open("xb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as stream:
            yield stream


def emit(stream, row, roots=()):
    stream.write((json.dumps(safe_record(row, roots), sort_keys=True, separators=(",", ":")) + "\n").encode())
    stream.flush()


def paired(arms, blocks):
    assert set(arms) == {"before", "after", "a", "b"}
    assert all(len(values) == blocks for values in arms.values())
    before, after, a, b = (arms[name] for name in ("before", "after", "a", "b"))
    floor = max(abs(median(a) - median(b)), (max(a) - min(a)) / 2, (max(b) - min(b)) / 2)
    delta = median(right - left for left, right in zip(before, after, strict=True))
    return {"before": median(before), "after": median(after), "delta": delta, "floor": floor,
            "decision": "faster" if delta < -floor else "slower" if delta > floor else "within floor"}


def bounded_timeout(deadline, process_limit):
    remaining = min(deadline - time.monotonic(), process_limit)
    if remaining <= 0:
        raise TimeoutError("committed session bound exhausted")
    return remaining
