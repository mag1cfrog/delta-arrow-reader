import argparse
from email.parser import BytesParser
from pathlib import Path
from zipfile import BadZipFile, ZipFile


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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check the reader wheel's identity and tags."
    )
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--platform-tag", required=True)
    args = parser.parse_args()
    try:
        check_wheel(args.wheel, args.expected_version, args.platform_tag)
    except (OSError, BadZipFile, ValueError) as error:
        parser.exit(1, f"wheel check failed: {error}\n")
    print(f"Validated {args.wheel.name}")


if __name__ == "__main__":
    main()
