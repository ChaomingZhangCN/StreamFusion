# AppendFast Top-1 throughput

`AppendTop1SustainedBenchmark` measures constant `ROW_NUMBER() <= 1` over a
runtime row source and a blackhole row sink. The native plan must contain
`NativeColumnarTopN`, `RowDataToArrow` and `ArrowToRowData`, and its execution
metrics must show consumed rows. The release `bench` profile retains Arrow IPC
on the keyed exchange. Source generation, conversion, JNI and state access all
remain in the running job.

Each fresh job warms up for five seconds after the rank operator starts
consuming input. The benchmark then takes the difference between that job's
rank input counters over a ten-second interval, using the actual elapsed time.
Three independent jobs per engine alternate their execution order. The report
retains every counter boundary and interval rate, then reports the arithmetic
mean of those rates. It does not select maximum TPS. Submission and startup
are outside this sustained interval; complete-job timing remains a separate
measurement.

The default configuration uses one TaskManager with eight slots, parallelism
one, a 48 GiB task off-heap budget, mini-batch size 1,024, and a one-hour timer
latency so count boundaries drive native bundles. Both engines receive the same
nullable sort values and nullable eight-character payloads. Rank sorts the
value descending and the payload ascending. Values decrease on each revisit by
default; `top1.improving=true` makes them increase instead. Keys recur cyclically:
65,536 keys exercise a larger retained state, rather than unique-key insertion.
Use a bounded complete-job benchmark separately to measure unique-key growth.

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=AppendTop1SustainedBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtop1.backend=rocksdb -Dtop1.keys=65536 -Dtop1.improving=false
```

`top1.backend` accepts `memory` and `rocksdb`; the latter uses Flink's released
RocksDB backend for the stock control and StreamFusion's backend for the native
control. `top1.keys`, `top1.width`, `top1.warmupSeconds`, `top1.seconds` and
`top1.runs` configure the workload and repetition. Use released Flink 1.18 with
`-Pflink-1.18,bench`. Measurements should run alone on the host.

For the previous-StreamFusion comparison required by
[#317](https://github.com/datafusion-contrib/StreamFusion/issues/317), build the
canonical baseline and candidate as separate release libraries with identical
allocator features. Run this same harness against each in separate JVMs, and
retain both library/source identities plus every stock control. Replacing a
library while a running JVM has it mapped is unsafe. This harness adds
measurement coverage; it does not change rank execution or establish that the
pending Top-1 optimization is ready to enable.
