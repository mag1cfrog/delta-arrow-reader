"""PEP 517 hooks for the Python package and its native build tools."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile

import maturin
from maturin import (
    get_requires_for_build_editable,
    get_requires_for_build_sdist,
    get_requires_for_build_wheel,
    prepare_metadata_for_build_editable,
    prepare_metadata_for_build_wheel,
)


def _xtask_command(task, *arguments):
    cargo = os.environ.get("CARGO", "cargo")
    cargo_arguments = [flag for flag in ("--offline", "--frozen") if flag in arguments]
    return [
        cargo, "run", "--locked", *cargo_arguments, "-p", "xtask", "--", task, *arguments,
    ]


def build_sdist(sdist_directory, config_settings=None):
    with tempfile.TemporaryDirectory(prefix="python-sdist-") as temporary:
        filename = maturin.build_sdist(temporary, config_settings)
        subprocess.run(
            _xtask_command(
                "complete-python-sdist",
                str(Path(temporary) / filename),
                str(Path(sdist_directory).resolve() / filename),
            ),
            check=True,
        )
    return filename


def _build_wheel(wheel_directory, config_settings, metadata_directory, editable):
    arguments = [
        "--project-directory", str(Path.cwd()),
        "--wheel-directory", str(Path(wheel_directory).resolve()),
        "--python", sys.executable,
    ]
    if metadata_directory is not None:
        arguments.extend(["--metadata-directory", str(Path(metadata_directory).resolve())])
    if editable:
        arguments.append("--editable")
    arguments.extend(maturin.get_maturin_pep517_args(config_settings))
    return subprocess.check_output(
        _xtask_command("build-python-wheel", *arguments), encoding="utf-8"
    ).strip()


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    return _build_wheel(wheel_directory, config_settings, metadata_directory, False)


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    return _build_wheel(wheel_directory, config_settings, metadata_directory, True)
