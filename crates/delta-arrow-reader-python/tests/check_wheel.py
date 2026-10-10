import argparse
from email.parser import BytesParser
from pathlib import Path
from zipfile import BadZipFile, ZipFile


ARTIFACT_PLATFORM_TAGS = {
    "delta-arrow-reader-linux-x86_64-wheel": "manylinux_2_28_x86_64",
    "delta-arrow-reader-windows-x86_64-wheel": "win_amd64",
    "delta-arrow-reader-macos-arm64-wheel": "macosx_12_0_arm64",
    "delta-arrow-reader-macos-x86_64-wheel": "macosx_12_0_x86_64",
}


def check_wheel(path: Path, expected_version: str, platform_tag: str) -> None:
    tag = f"cp310-abi3-{platform_tag}"
    filename = f"delta_arrow_reader-{expected_version}-{tag}.whl"
    if path.name != filename:
        raise ValueError(f"expected wheel {filename!r}, got {path.name!r}")

    dist_info = f"delta_arrow_reader-{expected_version}.dist-info"
    with ZipFile(path) as wheel:
        for name, expected_headers in (
            ("METADATA", {"Name": "delta-arrow-reader", "Version": expected_version}),
            ("WHEEL", {"Root-Is-Purelib": "false", "Tag": tag}),
        ):
            expected_path = f"{dist_info}/{name}"
            paths = [
                member
                for member in wheel.namelist()
                if member.endswith(f".dist-info/{name}")
            ]
            if paths != [expected_path]:
                raise ValueError(
                    f"expected exactly one {expected_path!r}, got {paths!r}"
                )
            headers = BytesParser().parsebytes(
                wheel.read(expected_path), headersonly=True
            )
            for field, expected in expected_headers.items():
                actual = headers.get_all(field)
                if actual != [expected]:
                    raise ValueError(
                        f"{name}: expected one {field}: {expected!r}, got {actual!r}"
                    )


def check_wheel_set(directory: Path, expected_version: str) -> None:
    artifacts = sorted(path.name for path in directory.iterdir())
    if artifacts != sorted(ARTIFACT_PLATFORM_TAGS):
        raise ValueError(
            f"expected artifacts {sorted(ARTIFACT_PLATFORM_TAGS)!r}, got {artifacts!r}"
        )
    for artifact, platform_tag in ARTIFACT_PLATFORM_TAGS.items():
        paths = list((directory / artifact).iterdir())
        if len(paths) != 1:
            raise ValueError(f"{artifact}: expected one wheel, got {len(paths)} entries")
        check_wheel(paths[0], expected_version, platform_tag)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check one reader wheel or a complete platform artifact set."
    )
    parser.add_argument("path", type=Path, help="Wheel file or artifact set directory")
    parser.add_argument("--expected-version", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--platform-tag")
    mode.add_argument("--artifact-set", action="store_true")
    args = parser.parse_args()
    try:
        if args.artifact_set:
            check_wheel_set(args.path, args.expected_version)
        else:
            check_wheel(args.path, args.expected_version, args.platform_tag)
    except (OSError, BadZipFile, ValueError) as error:
        parser.exit(1, f"wheel check failed: {error}\n")
    print(f"Validated {args.path.name}")


if __name__ == "__main__":
    main()
