"""Require the identical pinned upstream streaming invocations in both engine runs."""
import sys
import re
from collections import Counter
from pathlib import Path
import xml.etree.ElementTree as ET

CLASS = "org.apache.iceberg.flink.TestFlinkTableSinkExtended"


def cases(directory):
    root = ET.parse(Path(directory) / f"TEST-{CLASS}.xml").getroot()
    executed, skipped = set(), set()
    occurrences = Counter()
    for case in root.findall("testcase"):
        raw = case.attrib["name"]
        match = re.fullmatch(r"(.*)\(\)\[(\d+)\]", raw)
        method = match.group(1) if match else raw
        occurrences[method] += 1
        # Surefire 3.2 omitted invocation indices; its serial report retains parameter order.
        index = int(match.group(2)) if match else occurrences[method]
        name = (method, index)
        if case.find("failure") is not None or case.find("error") is not None:
            raise ValueError(f"Failed upstream case: {name}")
        if case.find("skipped") is not None:
            skipped.add(name)
        else:
            executed.add(name)
    if len(executed) != 12 or len(skipped) != 8:
        raise ValueError(f"Expected 12 streaming and 8 excluded batch cases, found {len(executed)}/{len(skipped)}")
    return executed, skipped


def main(stock, native):
    if cases(stock) != cases(native):
        raise ValueError("Stock/native upstream invocation inventories differ")
    reports = Path(native)
    for name, count in (("IcebergSinkAdapterTest", 1), ("NativeIcebergSinkTest", 7), ("NativeIcebergSqlTest", 4), ("NativeIcebergKeyedTest", 8), ("NativeIcebergReaderTest", 7), ("NativeIcebergTypeTest", 2)):
        root = ET.parse(reports / f"TEST-tech.streamfusion.iceberg.{name}.xml").getroot()
        if any(case.find("failure") is not None or case.find("error") is not None
               for case in root.findall("testcase")):
            raise ValueError(f"Failed native witness: {name}")
        passed = [case for case in root.findall("testcase") if not list(case.findall("failure"))
                  and case.find("error") is None and case.find("skipped") is None]
        if len(passed) < count:
            raise ValueError(f"Missing native hook/readback witnesses: {name}")
    print("Iceberg: identical 12 upstream streaming cases; native hook, SQL file-encoder, keyed recovery, and readback witnesses passed")


if __name__ == "__main__":
    try:
        main(*sys.argv[1:])
    except (ValueError, OSError, ET.ParseError) as error:
        raise SystemExit(f"Iceberg suite evidence failed: {error}")
