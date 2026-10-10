"""Validate a Python release request and emit inputs for the wheel builder."""

import json
import os
from pathlib import Path
import re
import subprocess
import tomllib


def prepare_release(event_name, event, ref, source_sha):
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("source must be a full commit SHA")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if head != source_sha:
        raise ValueError("checkout does not match the event commit")
    manifest = subprocess.check_output(["git", "show", "HEAD:Cargo.toml"], text=True)
    version = tomllib.loads(manifest)["workspace"]["package"]["version"]
    if not re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version):
        raise ValueError("Python releases require a stable X.Y.Z Cargo version")

    tag = f"v{version}"
    if event_name == "workflow_dispatch":
        inputs = event.get("inputs", {})
        publish_to = inputs.get("publish-to", "none")
        if publish_to not in ("none", "testpypi", "pypi"):
            raise ValueError("publish-to must be none, testpypi, or pypi")
        if inputs.get("expected-version") != version:
            raise ValueError(f"expected-version must match Cargo version {version}")
    elif event_name == "release":
        release = event.get("release", {})
        if (
            event.get("action") != "published"
            or release.get("draft") is not False
            or release.get("prerelease") is not False
        ):
            raise ValueError("only stable published releases are supported")
        if release.get("tag_name") != tag:
            raise ValueError(f"release tag must match {tag}")
        publish_to = "pypi"
    else:
        raise ValueError(f"unsupported release event: {event_name}")

    if ref.startswith("refs/tags/"):
        if ref != f"refs/tags/{tag}":
            raise ValueError(f"selected tag must match {tag}")
        tag_sha = subprocess.check_output(
            ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], text=True,
        ).strip()
        if tag_sha != source_sha:
            raise ValueError("selected tag does not point to the event commit")
    elif publish_to == "pypi" or not ref.startswith("refs/heads/"):
        raise ValueError(
            "PyPI requires a version tag; other modes accept branches or version tags"
        )

    return {
        "source-ref": source_sha,
        "expected-version": version,
        "publish-to": publish_to,
    }


def main():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    outputs = prepare_release(
        os.environ["GITHUB_EVENT_NAME"], event,
        os.environ["GITHUB_REF"], os.environ["GITHUB_SHA"],
    )
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        for name, value in outputs.items():
            output.write(f"{name}={value}\n")


if __name__ == "__main__":
    try:
        main()
    except (KeyError, OSError, ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"Python release check failed: {error}")
