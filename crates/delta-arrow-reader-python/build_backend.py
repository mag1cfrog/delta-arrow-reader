"""PEP 517 hooks for the Python package and its native build tools."""

import os
from pathlib import Path
import subprocess
import tempfile

import maturin
from maturin import (
    build_editable,
    build_wheel,
    get_requires_for_build_editable,
    get_requires_for_build_sdist,
    get_requires_for_build_wheel,
    prepare_metadata_for_build_editable,
    prepare_metadata_for_build_wheel,
)


def build_sdist(sdist_directory, config_settings=None):
    cargo = os.environ.get("CARGO", "cargo")
    workspace = Path(
        subprocess.check_output(
            [cargo, "locate-project", "--workspace", "--message-format", "plain"],
            text=True,
        ).strip()
    ).parent
    with tempfile.TemporaryDirectory(prefix="python-sdist-") as temporary:
        filename = maturin.build_sdist(temporary, config_settings)
        subprocess.run(
            [
                cargo,
                "run",
                "--locked",
                "--manifest-path",
                str(workspace / "xtask" / "Cargo.toml"),
                "--",
                "complete-python-sdist",
                str(Path(temporary) / filename),
                str(Path(sdist_directory).resolve() / filename),
            ],
            check=True,
        )
    return filename
