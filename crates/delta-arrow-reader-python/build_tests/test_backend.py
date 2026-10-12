import importlib.util
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import Mock, patch


class BackendTests(unittest.TestCase):
    def setUp(self):
        self.maturin = types.ModuleType("maturin")
        for name in (
            "build_sdist",
            "build_wheel",
            "build_editable",
            "get_requires_for_build_sdist",
            "get_requires_for_build_wheel",
            "get_requires_for_build_editable",
            "prepare_metadata_for_build_wheel",
            "prepare_metadata_for_build_editable",
        ):
            setattr(self.maturin, name, Mock())
        path = Path(__file__).resolve().parents[1] / "build_backend.py"
        spec = importlib.util.spec_from_file_location("build_backend", path)
        self.backend = importlib.util.module_from_spec(spec)
        with patch.dict("sys.modules", {"maturin": self.maturin}):
            spec.loader.exec_module(self.backend)

    def test_completes_sdist_from_a_temporary_archive_with_argument_lists(self):
        filename = "delta_arrow_reader-1.2.3.tar.gz"
        settings = {"build-args": "--locked"}
        self.maturin.build_sdist.return_value = filename
        with tempfile.TemporaryDirectory(prefix="workspace with spaces ") as temporary:
            workspace = Path(temporary)
            output = workspace / "output directory"
            with (
                patch.object(
                    self.backend.subprocess,
                    "check_output",
                    return_value=str(workspace / "Cargo.toml") + "\n",
                ),
                patch.object(self.backend.subprocess, "run") as run,
            ):
                self.assertEqual(self.backend.build_sdist(str(output), settings), filename)
            source_dir, forwarded = self.maturin.build_sdist.call_args.args
            self.assertEqual(forwarded, settings)
            self.assertNotEqual(Path(source_dir), output)
            command = run.call_args.args[0]
            self.assertEqual(command[1:], [
                "run",
                "--locked",
                "--manifest-path",
                str(workspace / "xtask" / "Cargo.toml"),
                "--",
                "complete-python-sdist",
                str(Path(source_dir) / filename),
                str(output / filename),
            ])
            self.assertEqual(run.call_args.kwargs, {"check": True})
            self.assertFalse(Path(source_dir).exists())

    def test_native_tool_failure_does_not_return_a_completed_archive(self):
        self.maturin.build_sdist.return_value = "delta_arrow_reader-1.2.3.tar.gz"
        with (
            patch.object(
                self.backend.subprocess,
                "check_output",
                return_value="/source/Cargo.toml\n",
            ),
            patch.object(
                self.backend.subprocess,
                "run",
                side_effect=subprocess.CalledProcessError(1, ["cargo"]),
            ),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                self.backend.build_sdist("output")
        self.assertFalse(Path(self.maturin.build_sdist.call_args.args[0]).exists())

    def test_metadata_hooks_forward_without_building_native_tools(self):
        settings = {"build-args": "--locked"}
        with patch.object(self.backend.subprocess, "run") as run:
            self.backend.prepare_metadata_for_build_wheel("metadata", settings)
            self.backend.prepare_metadata_for_build_editable("metadata")
        self.maturin.prepare_metadata_for_build_wheel.assert_called_once_with(
            "metadata", settings
        )
        self.maturin.prepare_metadata_for_build_editable.assert_called_once_with(
            "metadata"
        )
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
