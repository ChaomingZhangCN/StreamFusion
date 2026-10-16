# 47 — Iceberg keeps its released Java streaming control plane

Comet provides the model for explicit Iceberg file-task metadata and Arrow/JNI ownership.
StreamFusion retains Flink's streaming discovery, split ordinals, row offsets, watermark
emitter, operator/coordinator state, rolling writers, delete encoders and committers.
Released iceberg-rust reads admitted Parquet tasks and encodes one Java-selected data
file at a time. Java FileIO supplies credentials, paths, streams and positional reads.
This extends the [encoding-only sink boundary](22-parquet-sink-encoding-only-native.md).

Flink's range sink adds another host boundary: its statistics operator emits borrowed
row views plus occasional control records into a stateful Java range partitioner.
The adapter delegates the original operator lifecycle/events and coordinator provider,
then applies that same partitioner synchronously to Arrow-backed row views. Control
records update the partitioner at the producer; selected row positions become Arrow
batches through the shared Rust splitter. The normal custom partitioner still disables
unaligned capture on that exchange. Checkpointed statistics, learned-assignment timing,
rescale handling and per-channel row order stay Java-owned; no native sort/routing policy
or separate statistics checkpoint format is introduced.

The cost is Java control traversal, batch gathering when selections are sparse, JNI
and host IO callbacks. These remain inside the separate Iceberg Nexmark timing. The
version-gated topology adapter changes payload factories/types before serialization;
unknown connector layouts retain the normal planning fallback. Wider resource accounting
and remaining sink checkpoints are tracked in [#373](https://github.com/datafusion-contrib/StreamFusion/issues/373).

The dynamic sink uses the same boundary: wrap its declared `Map` factory cache while
delegating storage/eviction to the original LRU cache. Decorate newly created factories
before the host initializes them, so there is no provisional file creation or writer
reimplementation. The generator/updater still decide metadata and routing over borrowed
views; target-homogeneous selections cross both forward and keyed paths as Arrow batches.
A fixed payload layout avoids the host's materialized-row schema conversion becoming a
transport boundary. Unknown layouts require a separate columnar conversion checkpoint.
