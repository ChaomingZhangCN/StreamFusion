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


def source_cases(directory):
    name = "org.apache.iceberg.flink.source.TestIcebergSourceSql"
    root = ET.parse(Path(directory) / f"TEST-{name}.xml").getroot()
    executed, skipped = set(), set()
    for case in root.findall("testcase"):
        method = case.attrib["name"]
        if case.find("failure") is not None or case.find("error") is not None:
            raise ValueError(f"Failed upstream source case: {method}")
        (skipped if case.find("skipped") is not None else executed).add(method)
    if executed != {"testWatermarkValidConfig", "testWatermarkInvalidConfig"} or len(skipped) != 5:
        raise ValueError(f"Unexpected streaming source SQL inventory: {executed}/{skipped}")
    return executed, skipped


def main(stock, native):
    if cases(stock) != cases(native):
        raise ValueError("Stock/native upstream invocation inventories differ")
    if source_cases(stock) != source_cases(native):
        raise ValueError("Stock/native upstream source SQL invocation inventories differ")
    reports = Path(native)
    for name, count in (("IcebergSinkAdapterTest", 1), ("NativeIcebergSinkTest", 8), ("NativeIcebergSqlTest", 11), ("NativeIcebergKeyedTest", 26), ("NativeIcebergReaderTest", 8), ("NativeIcebergTypeTest", 3), ("NativeIcebergPredicateTest", 10), ("NativeIcebergTaskMetadataTest", 4), ("NativeIcebergFloatingPredicateTest", 28), ("NativeIcebergTemporalPredicateTest", 12), ("NativeIcebergRangeTest", 1)):
        root = ET.parse(reports / f"TEST-tech.streamfusion.iceberg.{name}.xml").getroot()
        if any(case.find("failure") is not None or case.find("error") is not None
               for case in root.findall("testcase")):
            raise ValueError(f"Failed native witness: {name}")
        passed = [case for case in root.findall("testcase") if not list(case.findall("failure"))
                  and case.find("error") is None and case.find("skipped") is None]
        if len(passed) < count:
            raise ValueError(f"Missing native hook/readback witnesses: {name}")
    print("Iceberg: identical 14 upstream streaming sink/source SQL cases; native hook, SQL file-encoder, keyed recovery, and readback witnesses passed")


if __name__ == "__main__":
    try:
        main(*sys.argv[1:])
    except (ValueError, OSError, ET.ParseError) as error:
        raise SystemExit(f"Iceberg suite evidence failed: {error}")
