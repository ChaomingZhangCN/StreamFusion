# Interrupted, contended Iceberg diagnostic

This is an interrupted diagnostic, excluded from the final performance matrix.
Twenty of 120 cells completed their full warmup and three measured pairs, with committed
parity and native file/task witnesses. The benchmark JVM was then killed with exit 137
at q4 memory/off warmup, before that cell completed.

Other-session Java test workers overlapped the run. The retained host probe observed
load averages above 180 and substantial swapping. The completed observations remain
visible; they cannot establish clean stock/native performance comparisons. No query,
resource, checkpoint, commit or compression setting was changed to compensate.

`interruption/` retains the complete driver log, partial raw observations (including the
uncompleted cell's progress), and host-load samples. `progress.json` and the per-cell
archives retain the 20 validated cells. A separate fresh matrix will use the same
production code and settings once the host is available.
