package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class NativeIcebergKeyedTest {
  @TempDir Path temp;
  private static final AtomicBoolean FAILED = new AtomicBoolean();
  private static final AtomicBoolean RESTORED = new AtomicBoolean();

  @ParameterizedTest
  @CsvSource({
    "false,false,none,false", "true,false,none,false",
    "false,true,hash,false", "true,true,hash,false",
    "false,true,hash,true", "true,true,hash,true",
    "false,false,range,false", "true,false,range,false"
  })
  void hostDeletesAndRecoveryPreserveNativeDataPositions(
      boolean upsert, boolean fail, String distribution, boolean unaligned) throws Exception {
    FAILED.set(false);
    RESTORED.set(false);
    String location = temp.resolve("keyed").toUri().toString();
    var schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "value", Types.IntegerType.get())),
            java.util.Set.of(1));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.builderFor(schema).bucket("id", 4).build(),
                Map.of(TableProperties.FORMAT_VERSION, "2"),
                location);
    var configuration =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type", "fixed-delay",
                "restart-strategy.fixed-delay.attempts", "1",
                "restart-strategy.fixed-delay.delay", "10 ms"));
    configuration.set(
        org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY,
        temp.resolve("checkpoints").toUri().toString());
    var env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(2);
    env.setMaxParallelism(16);
    env.enableCheckpointing(100);
    if (unaligned) env.getCheckpointConfig().enableUnalignedCheckpoints();
    var input =
        env.addSource(new PhasedSource(fail))
            .returns(ArrowBatchTypeInformation.INSTANCE)
            .setParallelism(1);
    NativeIcebergSink.append(
        input,
        table,
        TableLoader.fromHadoopTable(location),
        configuration,
        Map.of(
            "distribution-mode",
            distribution,
            "upsert-enabled",
            Boolean.toString(upsert),
            "write-parallelism",
            "2"));
    var job = env.executeAsync("native-iceberg-keyed-checkpoints");
    try {
      job.getJobExecutionResult().get(60, java.util.concurrent.TimeUnit.SECONDS);
    } catch (Exception failure) {
      try {
        job.cancel().get();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
    table.refresh();
    var actual = new java.util.TreeMap<Integer, Integer>();
    try (var rows = IcebergGenerics.read(table).build()) {
      for (var row : rows) {
        assertNull(actual.put((Integer) row.getField("id"), (Integer) row.getField("value")));
      }
    }
    assertEquals(Map.of(1, 11, 3, 30, 5, 50), actual);
    assertTrue(
        java.util.stream.StreamSupport.stream(table.snapshots().spliterator(), false).count() >= 2);
    var deleteKinds = java.util.EnumSet.noneOf(org.apache.iceberg.FileContent.class);
    for (var snapshot : table.snapshots()) {
      for (var delete : snapshot.addedDeleteFiles(table.io())) {
        deleteKinds.add(delete.content());
        try (var parquet =
            org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
                org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(delete.location()),
                    new org.apache.hadoop.conf.Configuration()))) {
          assertTrue(parquet.getFooter().getFileMetaData().getCreatedBy().contains("parquet-mr"));
        }
      }
    }
    assertEquals(
        java.util.Set.of(
            org.apache.iceberg.FileContent.EQUALITY_DELETES,
            org.apache.iceberg.FileContent.POSITION_DELETES),
        deleteKinds);
    assertEquals(fail, FAILED.get());
    assertEquals(fail, RESTORED.get());
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static ArrowBatch batch(int phase) {
    int[] keys = phase == 0 ? new int[] {1, 2, 3} : new int[] {1, 1, 2, 4, 4, 5};
    int[] values = phase == 0 ? new int[] {10, 20, 30} : new int[] {10, 11, 20, 40, 40, 50};
    RowKind[] kinds =
        phase == 0
            ? new RowKind[] {RowKind.INSERT, RowKind.INSERT, RowKind.INSERT}
            : new RowKind[] {
              RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE,
              RowKind.INSERT, RowKind.DELETE, RowKind.UPDATE_AFTER
            };
    var id = new IntVector("id", NativeAllocator.SHARED);
    var value = new IntVector("value", NativeAllocator.SHARED);
    var kind = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
    var root = VectorSchemaRoot.of(id, value, kind);
    root.allocateNew();
    for (int i = 0; i < keys.length; i++) {
      id.setSafe(i, keys[i]);
      value.setSafe(i, values[i]);
      kind.setSafe(i, kinds[i].toByteValue());
    }
    root.setRowCount(keys.length);
    return new ArrowBatch(root);
  }

  private static final class PhasedSource
      implements SourceFunction<ArrowBatch>, CheckpointedFunction, CheckpointListener {
    private final boolean fail;
    private volatile boolean running = true;
    private volatile int completed;
    private int next;
    private transient ListState<Integer> state;
    private transient Map<Long, Integer> snapshots;

    private PhasedSource(boolean fail) {
      this.fail = fail;
    }

    @Override
    public void run(SourceContext<ArrowBatch> context) throws Exception {
      if (next == 0) {
        synchronized (context.getCheckpointLock()) {
          context.collect(batch(0));
          next = 1;
        }
      }
      long deadline = System.nanoTime() + java.time.Duration.ofSeconds(45).toNanos();
      while (running && completed < 1 && next < 2) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("checkpoint timed out");
        Thread.sleep(5);
      }
      if (!running) return;
      synchronized (context.getCheckpointLock()) {
        context.collect(batch(1));
        if (fail && FAILED.compareAndSet(false, true)) {
          throw new IllegalStateException("injected failure after an uncommitted update batch");
        }
        next = 2;
      }
      while (running && completed < 2) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("checkpoint timed out");
        Thread.sleep(5);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.update(List.of(next));
      snapshots.put(context.getCheckpointId(), next);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("iceberg-arrow-source-phase", Integer.class));
      snapshots = new ConcurrentHashMap<>();
      if (context.isRestored()) {
        for (int phase : state.get()) next = phase;
        completed = next;
        RESTORED.set(true);
      }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      completed = Math.max(completed, snapshots.getOrDefault(checkpointId, 0));
      snapshots.keySet().removeIf(id -> id <= checkpointId);
    }
  }
}
