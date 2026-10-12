"""Dependency preparation must use the transferred repository and block incomplete builds."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).resolve().parents[2] / "bin/flink-suite.sh"


class TestRuntimePreparationTest(unittest.TestCase):
    def test_each_line_prepares_its_own_runtime_and_preserves_download_failure(self):
        source = RUNNER.read_text()
        start = source.index('if [[ "${SUITE_MODE}" == "prepare" ]]; then\n')
        end = source.index('\nif [[ -n "${FLINK_SUITE_SHARD:-}" ]]; then', start)
        command = source[start:end]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            executable = root / "python3"
            executable.write_text(
                '#!' + sys.executable + '\n'
                'import json, os, sys\n'
                'with open(os.environ["CALLS"], "a") as output:\n'
                '    output.write(json.dumps([os.environ.get("MAVEN_USER_HOME"), os.getcwd(), sys.argv[1:]]) + "\\n")\n'
                'if sys.argv[1].endswith("ci-maven-dependencies.py"):\n'
                '    sys.exit(int(os.environ["DOWNLOAD_STATUS"]))\n'
            )
            executable.chmod(0o755)
            calls = root / "calls.jsonl"
            for line in ("2.2", "1.18"):
                suite = root / f"suite with spaces/{line}"
                (suite / "flink").mkdir(parents=True)
                for status, probe_status in ((0, 0), (7, 0), (0, 9)):
                    with self.subTest(line=line, status=status, probe_status=probe_status):
                        calls.unlink(missing_ok=True)
                        result = subprocess.run(
                            ["bash", "-c", 'flink_mvn() { echo "offline-probe:$*"; return "$PROBE_STATUS"; }\n' + command],
                            capture_output=True, text=True,
                            env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                                 "CALLS": str(calls), "DOWNLOAD_STATUS": str(status),
                                 "PROBE_STATUS": str(probe_status), "MAVEN_SETTINGS": "settings",
                                 "SUITE_MODE": "prepare", "FLINK_LINE": line, "FLINK_VERSION": "test",
                                 "FLINK_TARGET_PROFILE": "java11-target" if line == "2.2" else "",
                                 "REPO_ROOT": str(RUNNER.parents[1]), "SUITE_ROOT": str(suite),
                                 "SUITE_MAVEN_REPO": str(suite / "m2"),
                                 "SUITE_MAVEN_USER_HOME": str(suite / "maven-wrapper"),
                                 "FLINK_ROOT": str(suite / "flink"), "CONTRACT_FILE": "contracts"},
                        )
                        self.assertEqual(status or probe_status, result.returncode, result.stderr)
                        invocations = [json.loads(row) for row in calls.read_text().splitlines()]
                        wrapper_home, working_directory, arguments = invocations[0]
                        self.assertEqual(str(suite / "maven-wrapper"), wrapper_home)
                        self.assertEqual(str(suite / "flink"), working_directory)
                        self.assertEqual(str(suite / "m2"), arguments[arguments.index("--repository") + 1])
                        self.assertEqual(str(suite / "flink/mvnw"), arguments[arguments.index("--maven") + 1])
                        self.assertEqual("java11-target" if line == "2.2" else "",
                                         arguments[arguments.index("--profile") + 1])
                        artifacts = [arguments[i + 1] for i, value in enumerate(arguments) if value == "--artifact"]
                        self.assertEqual(["org.apache.maven.surefire:surefire-junit-platform:3.2.2",
                                          "org.junit.platform:junit-platform-launcher:" +
                                          ("1.9.1" if line == "1.18" else "1.11.4")], artifacts)
                        self.assertEqual(1 if status or probe_status else 2, len(invocations))
                        self.assertEqual(not (status or probe_status), "build is ready" in result.stdout)
                        if status:
                            self.assertNotIn("offline-probe:", result.stdout)
                        else:
                            self.assertIn(" -o ", result.stdout)
                            self.assertIn("CalcITCase#testSimpleProject", result.stdout)
                            self.assertIn("-Dmaven.test.failure.ignore=false", result.stdout)


if __name__ == "__main__":
    unittest.main()
