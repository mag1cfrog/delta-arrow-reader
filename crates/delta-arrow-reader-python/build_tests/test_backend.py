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
            "get_maturin_pep517_args",
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
            with patch.object(self.backend.subprocess, "run") as run:
                self.assertEqual(self.backend.build_sdist(str(output), settings), filename)
            source_dir, forwarded = self.maturin.build_sdist.call_args.args
            self.assertEqual(forwarded, settings)
            self.assertNotEqual(Path(source_dir), output)
            command = run.call_args.args[0]
            self.assertEqual(command[1:], [
                "run",
                "--locked",
                "-p", "xtask",
                "--",
                "complete-python-sdist",
                str(Path(source_dir) / filename),
                str(output / filename),
            ])
            self.assertEqual(run.call_args.kwargs, {"check": True})
            self.assertFalse(Path(source_dir).exists())

    def test_native_tool_failure_does_not_return_a_completed_archive(self):
        self.maturin.build_sdist.return_value = "delta_arrow_reader-1.2.3.tar.gz"
        with patch.object(self.backend.subprocess, "run",
                          side_effect=subprocess.CalledProcessError(1, ["cargo"])):
            with self.assertRaises(subprocess.CalledProcessError):
                self.backend.build_sdist("output")
        self.assertFalse(Path(self.maturin.build_sdist.call_args.args[0]).exists())

    def test_wheel_hooks_forward_frontend_paths_and_maturin_arguments(self):
        filename = "delta_arrow_reader-1.2.3-cp310-abi3-linux_x86_64.whl"
        settings = {"maturin.build-args": ["--profile", "dev", "--target-dir", "build output"]}
        self.maturin.get_maturin_pep517_args.return_value = settings["maturin.build-args"]
        for editable in (False, True):
            with self.subTest(editable=editable), patch.object(
                self.backend.subprocess, "check_output",
                return_value=filename + "\n",
            ) as output:
                hook = self.backend.build_editable if editable else self.backend.build_wheel
                self.assertEqual(hook("wheel output", settings, "prepared metadata"), filename)
                command = output.call_args.args[0]
                self.assertEqual(command[1:7], [
                    "run", "--locked", "-p", "xtask", "--", "build-python-wheel",
                ])
                self.assertEqual(command[7:15], [
                    "--project-directory", str(Path.cwd()),
                    "--wheel-directory", str(Path("wheel output").resolve()),
                    "--python", self.backend.sys.executable,
                    "--metadata-directory", str(Path("prepared metadata").resolve()),
                ])
                self.assertEqual("--editable" in command, editable)
                self.assertEqual(command[-4:], settings["maturin.build-args"])
                self.assertEqual(output.call_args.kwargs, {"encoding": "utf-8"})

    def test_network_restrictions_apply_before_bootstrapping_native_tools(self):
        for flag in ("--offline", "--frozen"):
            self.maturin.get_maturin_pep517_args.return_value = [flag]
            for hook in (self.backend.build_wheel, self.backend.build_editable):
                with self.subTest(flag=flag, hook=hook.__name__), patch.object(
                    self.backend.subprocess, "check_output",
                    side_effect=subprocess.CalledProcessError(1, ["cargo", "run"]),
                ) as output:
                    with self.assertRaises(subprocess.CalledProcessError):
                        hook("output")
                    command = output.call_args.args[0]
                    separator = command.index("--")
                    self.assertIn("--locked", command[:separator])
                    self.assertIn(flag, command[:separator])
                    self.assertIn(flag, command[separator + 1:])
                    self.assertEqual(output.call_count, 1)
        self.maturin.build_wheel.assert_not_called()
        self.maturin.build_editable.assert_not_called()



if __name__ == "__main__":
    unittest.main()
