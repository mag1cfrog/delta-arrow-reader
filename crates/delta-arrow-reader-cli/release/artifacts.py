"""Package CLI binaries and check the release artifact format using Python 3.11+."""

import argparse
import gzip
import hashlib
import io
import json
import re
import shutil
import tarfile
import zlib
from itertools import islice
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[3]
RUST_TARGETS = {
    "amd64": "x86_64-unknown-linux-gnu",
    "arm64": "aarch64-unknown-linux-gnu",
}
MEMBER_MODES = {"dar": 0o755, "LICENSE": 0o644, "NOTICE": 0o644}
MANIFEST = "cli-artifacts.json"
MIN_GLIBC = "2.36"


def build_identity(version, source_commit, rust_toolchain):
    for name, value, pattern in (
        ("version", version, r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"),
        ("source commit", source_commit, r"[0-9a-f]{40}"),
        ("Rust toolchain", rust_toolchain, r"[0-9]+\.[0-9]+\.[0-9]+"),
    ):
        if not re.fullmatch(pattern, value):
            raise ValueError(f"invalid {name}: {value!r}")
    return {
        "schema_version": 1,
        "version": version,
        "source_commit": source_commit,
        "rust_toolchain": rust_toolchain,
    }


def archive_name(version, architecture):
    return f"dar-v{version}-linux-{architecture}-gnu.tar.gz"


def file_sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def require_regular_file(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"expected a regular file: {path}")


def check_files(directory, expected):
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError(f"expected a directory: {directory}")
    actual = sorted(path.name for path in directory.iterdir())
    if actual != sorted(expected):
        raise ValueError(f"{directory}: expected {sorted(expected)!r}, got {actual!r}")
    for name in expected:
        require_regular_file(directory / name)


def unique_json_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def write_manifest(directory, identity, targets):
    manifest = {**identity, "targets": targets}
    (directory / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n")


def check_runtime_dependencies(dependencies):
    if (
        not isinstance(dependencies, list)
        or not dependencies
        or any(
            not isinstance(name, str)
            or not re.fullmatch(r"[A-Za-z0-9_+.-]+\.so(?:\.[0-9]+)*", name)
            for name in dependencies
        )
        or dependencies != sorted(set(dependencies))
    ):
        raise ValueError(
            "runtime_dependencies must be sorted, unique ELF library names"
        )


def describe_archive(directory, identity, architecture, dependencies):
    check_runtime_dependencies(dependencies)
    filename = archive_name(identity["version"], architecture)
    path = directory / filename
    require_regular_file(path)
    with (
        gzip.open(path, "rb") as compressed,
        tarfile.open(fileobj=compressed, mode="r:") as archive,
    ):
        members = list(islice(archive, 4))
        if [member.name for member in members] != list(MEMBER_MODES):
            raise ValueError(
                f"{filename}: expected only dar, LICENSE, NOTICE, in order"
            )
        for member in members:
            if (
                member.type != tarfile.REGTYPE
                or member.pax_headers
                or member.mode != MEMBER_MODES[member.name]
                or member.size <= 0
            ):
                raise ValueError(
                    f"{filename}: invalid file or permissions: {member.name}"
                )
            with archive.extractfile(member) as source:
                if member.name == "dar":
                    dar_sha256 = hashlib.file_digest(source, "sha256").hexdigest()
                elif member.name == "LICENSE":
                    license_text = (REPOSITORY / "LICENSE").read_bytes()
                    if (
                        member.size != len(license_text)
                        or source.read() != license_text
                    ):
                        raise ValueError(f"{filename}: repository LICENSE differs")
                else:
                    repository_notice = (REPOSITORY / "NOTICE").read_bytes() + b"\n"
                    if source.read(
                        len(repository_notice)
                    ) != repository_notice or not any(
                        chunk.strip()
                        for chunk in iter(lambda: source.read(1024 * 1024), b"")
                    ):
                        raise ValueError(
                            f"{filename}: repository or dependency notices missing"
                        )
        # tarfile stops at the first zero header, before checking the gzip footer.
        content_end = members[-1].offset_data + members[-1].size
        compressed.seek(content_end)
        padding_size = 0
        for chunk in iter(lambda: compressed.read(1024 * 1024), b""):
            if chunk.strip(b"\0"):
                raise ValueError(f"{filename}: unexpected data after archive members")
            padding_size += len(chunk)
        if (
            padding_size < (-content_end % tarfile.BLOCKSIZE) + 2 * tarfile.BLOCKSIZE
            or (content_end + padding_size) % tarfile.BLOCKSIZE
        ):
            raise ValueError(f"{filename}: incomplete tar end blocks")
    return {
        "os": "linux",
        "architecture": architecture,
        "rust_target": RUST_TARGETS[architecture],
        "libc": "glibc",
        "min_glibc": MIN_GLIBC,
        "runtime_dependencies": dependencies,
        "archive": filename,
        "archive_sha256": file_sha256(path),
        "dar_sha256": dar_sha256,
    }


def check_manifest(directory, identity, architectures):
    manifest = json.loads(
        (directory / MANIFEST).read_text(), object_pairs_hook=unique_json_fields
    )
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {*identity, "targets"}
        or any(manifest[key] != value for key, value in identity.items())
        or type(manifest["schema_version"]) is not int
    ):
        raise ValueError(f"{directory}: manifest build identity differs")
    targets = manifest["targets"]
    if not isinstance(targets, list) or len(targets) != len(architectures):
        raise ValueError(f"{directory}: incomplete target list")
    for architecture, target in zip(architectures, targets, strict=True):
        if not isinstance(target, dict):
            raise ValueError(f"{directory}: target must be an object")
        expected = describe_archive(
            directory, identity, architecture, target.get("runtime_dependencies")
        )
        if target != expected:
            raise ValueError(f"{directory}: {architecture} metadata or hashes differ")
    return targets


def package(
    binary, dependency_notices, directory, identity, architecture, dependencies
):
    check_runtime_dependencies(dependencies)
    for path in (binary, dependency_notices):
        require_regular_file(path)
        if path.stat().st_size == 0:
            raise ValueError(f"empty input: {path}")
    notice = (
        (REPOSITORY / "NOTICE").read_bytes() + b"\n" + dependency_notices.read_bytes()
    )
    directory.mkdir(parents=True, exist_ok=False)
    path = directory / archive_name(identity["version"], architecture)
    with (
        path.open("xb") as output,
        gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as compressed,
        tarfile.open(
            fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT
        ) as archive,
    ):
        for name, mode in MEMBER_MODES.items():
            if name == "NOTICE":
                source = io.BytesIO(notice)
            else:
                source_path = binary if name == "dar" else REPOSITORY / "LICENSE"
                source = source_path.open("rb")
            with source:
                member = tarfile.TarInfo(name)
                member.mode = mode
                member.size = source.seek(0, io.SEEK_END)
                source.seek(0)
                archive.addfile(member, source)
    target = describe_archive(directory, identity, architecture, dependencies)
    write_manifest(directory, identity, [target])


def extract(directory, destination, identity, architecture):
    check_files(directory, [archive_name(identity["version"], architecture), MANIFEST])
    [target] = check_manifest(directory, identity, [architecture])
    destination.mkdir(parents=True, exist_ok=False)
    with tarfile.open(directory / target["archive"], "r:gz") as archive:
        for name, mode in MEMBER_MODES.items():
            path = destination / name
            with archive.extractfile(name) as source, path.open("xb") as output:
                shutil.copyfileobj(source, output)
            path.chmod(mode)
    if (
        file_sha256(destination / "dar") != target["dar_sha256"]
        or file_sha256(directory / target["archive"]) != target["archive_sha256"]
    ):
        raise ValueError("archive or binary changed during extraction")


def checksum_text(directory, identity):
    names = [archive_name(identity["version"], arch) for arch in RUST_TARGETS]
    return "".join(
        f"{file_sha256(directory / name)}  {name}\n" for name in [*names, MANIFEST]
    )


def assemble(inputs, directory, identity):
    if inputs.is_symlink() or not inputs.is_dir():
        raise ValueError(f"expected a directory: {inputs}")
    if sorted(path.name for path in inputs.iterdir()) != sorted(RUST_TARGETS):
        raise ValueError("expected exactly the amd64 and arm64 input directories")
    targets = []
    for architecture in RUST_TARGETS:
        source = inputs / architecture
        check_files(source, [archive_name(identity["version"], architecture), MANIFEST])
        targets.extend(check_manifest(source, identity, [architecture]))
    directory.mkdir(parents=True, exist_ok=False)
    for target in targets:
        shutil.copyfile(
            inputs / target["architecture"] / target["archive"],
            directory / target["archive"],
        )
    write_manifest(directory, identity, targets)
    (directory / "SHA256SUMS").write_text(checksum_text(directory, identity))
    verify(directory, identity)


def verify(directory, identity):
    check_files(
        directory,
        [
            *(archive_name(identity["version"], arch) for arch in RUST_TARGETS),
            MANIFEST,
            "SHA256SUMS",
        ],
    )
    check_manifest(directory, identity, list(RUST_TARGETS))
    if (directory / "SHA256SUMS").read_text() != checksum_text(directory, identity):
        raise ValueError("SHA256SUMS differs from the complete artifact set")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command, description in (
        (
            "package",
            "Package one already validated binary and its declared build metadata",
        ),
        ("extract", "Validate and extract one platform artifact into a new directory"),
        ("assemble", "Combine both platform artifacts without repacking the archives"),
        ("verify", "Check the complete artifact format, declared identity, and hashes"),
    ):
        subparser = commands.add_parser(command, help=description)
        subparser.add_argument("directory", type=Path)
        subparser.add_argument("--version", required=True)
        subparser.add_argument("--source-commit", required=True)
        subparser.add_argument("--rust-toolchain", required=True)
        if command == "package":
            subparser.add_argument("--binary", type=Path, required=True)
            subparser.add_argument("--dependency-notices", type=Path, required=True)
            subparser.add_argument(
                "--architecture", choices=RUST_TARGETS, required=True
            )
            subparser.add_argument(
                "--runtime-dependency", action="append", required=True
            )
        elif command == "assemble":
            subparser.add_argument("--inputs", type=Path, required=True)
        elif command == "extract":
            subparser.add_argument(
                "--architecture", choices=RUST_TARGETS, required=True
            )
            subparser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    try:
        identity = build_identity(args.version, args.source_commit, args.rust_toolchain)
        if args.command == "package":
            package(
                args.binary,
                args.dependency_notices,
                args.directory,
                identity,
                args.architecture,
                sorted(args.runtime_dependency),
            )
        elif args.command == "assemble":
            assemble(args.inputs, args.directory, identity)
        elif args.command == "extract":
            extract(args.directory, args.destination, identity, args.architecture)
        else:
            verify(args.directory, identity)
    except (OSError, ValueError, tarfile.TarError, EOFError, zlib.error) as error:
        parser.exit(1, f"artifact {args.command} failed: {error}\n")
    print(f"Artifact {args.command} succeeded: {args.directory}")


if __name__ == "__main__":
    main()
