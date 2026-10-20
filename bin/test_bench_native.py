#!/usr/bin/env python3
"""Exercise the smoke runner's process, coverage and failure contracts."""
import collections
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).with_name("bench-native.py")


class SmokeWorkersTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.fixture = self.root / "fixture"
        self.executable(self.fixture, r'''
import os, pathlib, sys, time
index, count = map(int, os.environ.get("SF_NATIVE_BENCH_SHARD", "0/1").split("/"))
gate = pathlib.Path(os.environ["PROBE_ROOT"]) / "gate"
if count > 1:
    gate.mkdir(exist_ok=True)
    (gate / str(index)).touch()
    deadline = time.monotonic() + 5
    while len(list(gate.iterdir())) < count:
        if time.monotonic() > deadline:
            raise RuntimeError("workers did not overlap")
        time.sleep(.01)
for fixture in range(19):
    if fixture % count == index:
        print(f'NATIVE_ALLOCATION,"fixture/{fixture}",{fixture},{fixture * 10},0,0')
        print(f'Testing fixture/{fixture}\nSuccess')
sys.exit(7 if os.environ.get("FAIL_SHARD") == str(index) else 0)
''')
        self.executable(self.root / "cargo", r'''
import json, os, pathlib, sys
root = pathlib.Path(os.environ["PROBE_ROOT"])
if sys.argv[1] == "metadata":
    print(json.dumps({"packages": [{"id": "probe", "name": "streamfusion",
        "targets": [{"kind": ["bench"], "name": "persistent_state"}]}],
        "workspace_members": ["probe"], "target_directory": str(root / "target")}))
elif "--no-run" in sys.argv:
    if os.environ.get("FAIL_BUILD"):
        print("native compile failed")
        sys.exit(9)
    with (root / "compiles").open("a") as log:
        log.write("compile\n")
    print(json.dumps({"reason": "compiler-artifact", "target": {"name": "persistent_state"},
        "executable": str(root / "fixture")}))
else:
    os.execv(str(root / "fixture"), [str(root / "fixture"), "--test"])
''')
        self.executable(self.root / "rustc", 'print("smoke runner fixture compiler")\n')
        self.environment = {**os.environ, "PATH": str(self.root) + os.pathsep + os.environ["PATH"],
                            "PROBE_ROOT": str(self.root)}
        self.environment.pop("SF_NATIVE_BENCH_SHARD", None)

    def executable(self, path, source):
        path.write_text(f"#!{sys.executable}\n" + source)
        path.chmod(0o755)

    def run_smoke(self, workers, name, **environment):
        output = self.root / name
        result = subprocess.run([sys.executable, str(RUNNER), "--smoke", "--smoke-workers", str(workers),
                                 "--output", str(output)], env={**self.environment, **environment},
                                capture_output=True, text=True, timeout=20)
        return result, output

    def rows(self, output):
        with (output / "allocations.csv").open() as source:
            return collections.Counter(tuple(row) for row in csv.reader(source))

    def test_workers_compile_once_overlap_and_preserve_every_case(self):
        serial, before = self.run_smoke(1, "serial")
        parallel, after = self.run_smoke(4, "parallel")
        self.assertEqual(serial.returncode, 0, serial.stderr)
        self.assertEqual(parallel.returncode, 0, parallel.stderr)
        self.assertEqual(self.rows(before), self.rows(after))
        self.assertEqual(sum(self.rows(after).values()), 20)  # Header plus nineteen cases.
        self.assertEqual((self.root / "compiles").read_text(), "compile\n")
        metadata = json.loads((after / "metadata.json").read_text())
        self.assertEqual(len(metadata["smoke_shards"]), 4)
        self.assertGreater(metadata["suite_seconds"]["streamfusion/persistent_state"], 0)

    def test_any_failed_worker_fails_runner_and_retains_every_worker_log(self):
        result, output = self.run_smoke(4, "failed", FAIL_SHARD="2")
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual(len(list(output.glob("*-shard-*.log"))), 4)
        self.assertEqual(sum(self.rows(output).values()), 20)
        self.assertIn("fixture/18", (output / "streamfusion-persistent_state.log").read_text())

    def test_parallel_timing_is_rejected_before_building(self):
        for flags in [["--smoke-workers", "4"], ["--smoke", "--smoke-workers", "0"]]:
            result = subprocess.run([sys.executable, str(RUNNER), *flags], env=self.environment,
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2)
        self.assertFalse((self.root / "compiles").exists())

    def test_failed_compilation_never_launches_workers(self):
        result, output = self.run_smoke(4, "build-failed", FAIL_BUILD="true")
        self.assertEqual(result.returncode, 9, result.stderr)
        self.assertFalse(list(output.glob("*-shard-*.log")))
        self.assertIn("native compile failed", (output / "streamfusion-persistent_state.log").read_text())


if __name__ == "__main__":
    unittest.main()
