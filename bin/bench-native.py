#!/usr/bin/env python3
"""Discover and run release-mode Criterion suites with allocation diagnostics."""
import argparse
import concurrent.futures
import csv
import datetime
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "native" / "Cargo.toml"


def run_logged(command, path, environment):
    records = []
    with path.open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, env=environment)
        try:
            for line in process.stdout:
                log.write(line)
                log.flush()
                if line.startswith("NATIVE_ALLOCATION,"):
                    record = next(csv.reader([line.rstrip()]))[1:]
                    if len(record) != 5:
                        raise ValueError(f"Malformed allocation record: {line}")
                    records.append(record)
        except BaseException:
            process.terminate()
            process.wait()
            raise
        return process.wait(), records


def run_smoke_workers(command, output, environment, workers, details):
    build_command = [*command[:command.index("--")], "--no-run", "--message-format=json"]
    build_log = output / "streamfusion-persistent_state-build.log"
    details["commands"].append(build_command)
    result, _ = run_logged(build_command, build_log, environment)
    if result:
        (output / "streamfusion-persistent_state.log").write_text(build_log.read_text())
        return result, []
    executables = set()
    for line in build_log.read_text().splitlines():
        try:
            artifact = json.loads(line)
        except ValueError:
            continue
        if (artifact.get("reason") == "compiler-artifact"
                and artifact.get("target", {}).get("name") == "persistent_state"
                and artifact.get("executable")):
            executables.add(artifact["executable"])
    if len(executables) != 1:
        raise ValueError(f"Expected one persistent_state executable, found {executables}")
    worker_command = [executables.pop(), *command[command.index("--") + 1:]]
    details["smoke_shards"] = [
        {"command": worker_command, "environment": {"SF_NATIVE_BENCH_SHARD": f"{index}/{workers}"}}
        for index in range(workers)
    ]

    def run(index):
        worker_env = {**environment, "SF_NATIVE_BENCH_SHARD": f"{index}/{workers}"}
        path = output / f"streamfusion-persistent_state-shard-{index}.log"
        start = time.monotonic()
        result, records = run_logged(worker_command, path, worker_env)
        print(f"persistent_state worker {index + 1}/{workers}: {len(records)} cases, "
              f"{time.monotonic() - start:.1f}s, exit {result}", flush=True)
        return result, records

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(run, range(workers)))
    with (output / "streamfusion-persistent_state.log").open("w") as log:
        log.write(build_log.read_text())
        for index in range(workers):
            log.write(f"\nSmoke worker {index + 1}/{workers}\n")
            log.write((output / f"streamfusion-persistent_state-shard-{index}.log").read_text())
    return next((result for result, _ in results if result), 0), [
        record for _, records in results for record in records
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="List suites without compiling")
    parser.add_argument("--smoke", action="store_true", help="Run fixtures once using Criterion --test")
    parser.add_argument("--smoke-workers", type=int, default=1,
                        help="Workers within this runner for persistent_state smoke fixtures (default: 1)")
    parser.add_argument("--package", action="append", default=[], help="Select a workspace package (repeatable)")
    parser.add_argument("--bench", action="append", default=[], help="Select a suite (repeatable)")
    parser.add_argument("--features", action="append", default=[], help="Cargo features for each selected package (repeatable)")
    parser.add_argument("--filter", help="Criterion case filter")
    parser.add_argument("--output", type=Path, help="Artifact directory; defaults under native/target")
    baseline = parser.add_mutually_exclusive_group()
    baseline.add_argument("--save-baseline")
    baseline.add_argument("--baseline")
    args = parser.parse_args()
    if args.smoke and (args.save_baseline or args.baseline):
        parser.error("Smoke checks do not produce timing baselines")
    if args.smoke_workers < 1 or (args.smoke_workers != 1 and not args.smoke):
        parser.error("--smoke-workers must be positive and parallel workers require --smoke")
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--manifest-path", str(MANIFEST), "--locked", "--no-deps", "--format-version", "1"
    ], cwd=ROOT, text=True))
    suites = sorted((package["name"], target["name"]) for package in metadata["packages"]
                    if package["id"] in metadata["workspace_members"]
                    for target in package["targets"] if "bench" in target["kind"])
    packages = {package for package, _ in suites}
    benches = {bench for _, bench in suites}
    for unknown in set(args.package) - packages:
        parser.error(f"No benchmark package named {unknown}")
    for unknown in set(args.bench) - benches:
        parser.error(f"No benchmark suite named {unknown}")
    selected = [(package, bench) for package, bench in suites
                if (not args.package or package in args.package) and (not args.bench or bench in args.bench)]
    if not selected:
        parser.error("No suites match the selection")
    if args.list:
        for package, bench in selected:
            print(f"{package}\t{bench}")
        return 0
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output = args.output or Path(metadata["target_directory"]) / "native-benchmarks" / stamp
    output.mkdir(parents=True, exist_ok=True)
    details = {
        "timestamp_utc": stamp, "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True), "platform": platform.platform(),
        "cpu_count": os.cpu_count(), "mode": "smoke" if args.smoke else "timing", "suites": selected,
        "filter": args.filter, "features": args.features, "save_baseline": args.save_baseline, "baseline": args.baseline,
        "smoke_workers": args.smoke_workers, "suite_seconds": {},
        "commands": [], "environment": {key: os.environ[key] for key in ("RUSTFLAGS", "CARGO_BUILD_JOBS", "JAVA_HOME") if key in os.environ},
    }
    environment = os.environ.copy()
    if any(bench in {"scalar_registry", "jvm_truncate", "parquet_host_reader", "over_heap_snapshot", "fixed_window_calendar", "window_floating_distinct", "session_calendar", "window_tvf_calendar", "window_consumers_calendar"} for _, bench in selected):
        classpath = environment.get("SF_NATIVE_BENCH_CLASSPATH")
        if not classpath:
            classpath_file = (output / "java-classpath.txt").resolve()
            java_phase = "test-compile" if any(bench in {"jvm_truncate", "parquet_host_reader", "over_heap_snapshot"} for _, bench in selected) else "compile"
            command = ["mvn", "-B", "-ntp", "-pl", "streamfusion-runtime", "-am", java_phase,
                       "org.apache.maven.plugins:maven-dependency-plugin:3.7.0:build-classpath",
                       "-Dnative.build.skip=true", f"-Dmdep.outputFile={classpath_file}"]
            details["java_setup_command"] = command
            print("Preparing JVM benchmark classes; see java-build.log", flush=True)
            with (output / "java-build.log").open("w") as log:
                subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            classpath = os.pathsep.join([str(ROOT / "streamfusion-runtime" / "target" / "classes"),
                                       str(ROOT / "streamfusion-runtime" / "target" / "test-classes"),
                                       classpath_file.read_text().strip()])
            environment["SF_NATIVE_BENCH_CLASSPATH"] = classpath
        details["java_classpath"] = classpath
        details["java_version"] = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True)
    with (output / "allocations.csv").open("w", newline="") as allocation_file:
        writer = csv.writer(allocation_file, lineterminator="\n")
        writer.writerow(["package", "suite", "case", "allocation_calls", "requested_bytes", "shared_output_buffer_bytes", "new_output_buffer_bytes"])
        for package, bench in selected:
            command = ["cargo", "bench", "--manifest-path", str(MANIFEST), "--locked", "-p", package, "--bench", bench]
            for features in args.features:
                command.extend(["--features", features])
            command.append("--")
            if args.filter:
                command.append(args.filter)
            if args.smoke:
                command.append("--test")
            for option in ("save_baseline", "baseline"):
                if getattr(args, option):
                    command.extend(["--" + option.replace("_", "-"), getattr(args, option)])
            parallel = args.smoke and args.smoke_workers > 1 and (package, bench) == ("streamfusion", "persistent_state")
            if not parallel:
                details["commands"].append(command)
            (output / "metadata.json").write_text(json.dumps(details, indent=2) + "\n")
            print(f"Running {package}/{bench}; log: {output / (package + '-' + bench + '.log')}", flush=True)
            start = time.monotonic()
            if parallel:
                result, records = run_smoke_workers(command, output, environment, args.smoke_workers, details)
            else:
                result, records = run_logged(command, output / f"{package}-{bench}.log", environment)
            for record in records:
                writer.writerow([package, bench, *record])
            allocation_file.flush()
            details["suite_seconds"][f"{package}/{bench}"] = round(time.monotonic() - start, 3)
            (output / "metadata.json").write_text(json.dumps(details, indent=2) + "\n")
            print(f"Finished {package}/{bench}: {details['suite_seconds'][f'{package}/{bench}']:.1f}s", flush=True)
            if result:
                print(f"Suite failed ({result}); see {output / f'{package}-{bench}.log'}", file=sys.stderr)
                return result
    print(f"Artifacts: {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
