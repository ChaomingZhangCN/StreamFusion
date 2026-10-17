# Final Iceberg validation

After all timed comparisons, the stock/native upstream SQL and control suite, native source/sink/recovery fixtures, six release Rust units and 126 release+mimalloc Criterion fixture checks were rerun. `summary.json` records the exact report inventory; `reports.tar.gz` retains sanitized XML and logs. The suite verifier requires identical 14 upstream streaming SQL and 127 range/dynamic control cases. Batch exclusions remain explicit. Java tests run serially with 4 GiB heaps and the same isolated release native library hashes as the Nexmark matrix.
