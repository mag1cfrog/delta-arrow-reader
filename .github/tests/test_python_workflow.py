import itertools
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows/pypi.yml"


class PythonWorkflowTests(unittest.TestCase):
    def setUp(self):
        # BaseLoader preserves the workflow's "on" key instead of treating it as True.
        self.workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        self.jobs = self.workflow["jobs"]
        self.upload = self.jobs["publish-testpypi"]

    def test_build_only_is_default(self):
        inputs = self.workflow["on"]["workflow_dispatch"]["inputs"]
        self.assertEqual(inputs["publish-to"]["default"], "none")
        self.assertEqual(inputs["publish-to"]["options"], ["none", "testpypi"])

    def test_only_upload_job_can_request_oidc(self):
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        for name, job in self.jobs.items():
            with self.subTest(job=name):
                self.assertEqual(
                    job.get("permissions", {}).get("id-token") == "write",
                    name == "publish-testpypi",
                )

    def test_publisher_identity_and_upload_gate(self):
        self.assertEqual(self.upload["environment"], {
            "name": "testpypi-release",
            "url": "https://test.pypi.org/project/delta-arrow-reader/",
        })
        self.assertEqual(set(self.upload["needs"]), {"prepare", "check"})
        self.assertEqual(" ".join(self.upload["if"].split()), (
            "github.repository == 'mag1cfrog/delta-arrow-reader' && "
            "github.event_name == 'workflow_dispatch' && "
            "needs.prepare.outputs.publish-to == 'testpypi' && "
            "needs.check.result == 'success'"
        ))
        publish = self.upload["steps"][-1]
        self.assertEqual(publish["uses"], "pypa/gh-action-pypi-publish@release/v1")
        self.assertEqual(publish["with"], {
            "repository-url": "https://test.pypi.org/legacy/",
            "packages-dir": "dist/",
            "skip-existing": "false",
        })

    def test_upload_waits_for_successful_artifact_validation(self):
        check = self.jobs["check"]
        self.assertEqual(set(check["needs"]), {"prepare", "build"})
        self.assertEqual(check["if"], "always()")
        self.assertEqual(check["env"], {
            "PREPARE_RESULT": "${{ needs.prepare.result }}",
            "BUILD_RESULT": "${{ needs.build.result }}",
            "WHEELS_VALIDATED": "${{ needs.build.outputs.validated }}",
        })
        for prepare, build, validated in itertools.product(
            ("success", "failure", "skipped", "cancelled"),
            ("success", "failure", "skipped", "cancelled"),
            ("true", "false", ""),
        ):
            with self.subTest(prepare=prepare, build=build, validated=validated):
                result = subprocess.run(
                    ["bash", "-e", "-o", "pipefail", "-c", check["steps"][0]["run"]],
                    capture_output=True, text=True,
                    env={
                        **os.environ, "PREPARE_RESULT": prepare,
                        "BUILD_RESULT": build, "WHEELS_VALIDATED": validated,
                    },
                )
                expected = prepare == build == "success" and validated == "true"
                self.assertEqual(result.returncode == 0, expected, result.stdout)

    def test_incomplete_artifacts_cannot_reach_upload_directory(self):
        stage = next(step for step in self.upload["steps"] if step.get("id") == "stage")
        checker = Path("crates/delta-arrow-reader-python/tests/check_wheel.py")
        with tempfile.TemporaryDirectory(prefix="python-upload-test-") as temporary:
            directory = Path(temporary)
            (directory / checker.parent).mkdir(parents=True)
            shutil.copyfile(WORKFLOW.parents[2] / checker, directory / checker)
            (directory / "wheel-artifacts").mkdir()
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", stage["run"]],
                cwd=directory, capture_output=True, text=True,
                env={**os.environ, "EXPECTED_VERSION": "1.2.3"},
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("expected artifacts", result.stderr)
            self.assertFalse((directory / "dist").exists())


if __name__ == "__main__":
    unittest.main()
