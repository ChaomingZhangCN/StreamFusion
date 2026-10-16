package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSubtaskKeySelector;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Extends the core captured-channel recovery fixture through the actual Iceberg writer exchange.
 */
class NativeIcebergCapturedRecoveryTest {
  private static final int ROWS = 12288;
  private static final int MAX = 257;
  private static final AtomicBoolean FAILED = new AtomicBoolean();
  private static final AtomicBoolean PAST_END = new AtomicBoolean();
  private static final Set<org.apache.flink.runtime.jobgraph.OperatorID> CHANNEL_OPERATORS =
      new HashSet<>();

  @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS)
  Path temp;

  @ParameterizedTest
  @CsvSource({"2,3,false", "3,2,false", "2,3,true", "3,2,true"})
  void restoresCapturedArrowExchangeAndCommittedRows(int before, int after, boolean keyed)
      throws Exception {
    FAILED.set(false);
    var schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "k", Types.IntegerType.get()),
                Types.NestedField.required(2, "id", Types.IntegerType.get())),
            keyed ? Set.of(1, 2) : Set.of());
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.builderFor(schema).bucket("k", 4).build(),
                Map.of("format-version", "2"),
                temp.resolve("table").toUri().toString());
    try {
      run(before, table, null, keyed);
    } catch (Exception expected) {
      assertTrue(
          FAILED.get(), "first job failed before the intentional checkpoint failure: " + expected);
    }
    assertTrue(FAILED.get());
    Path retained;
    try (var paths = Files.walk(temp.resolve("checkpoints"))) {
      retained =
          paths
              .filter(path -> path.getFileName().toString().equals("_metadata"))
              .map(Path::getParent)
              .max(
                  java.util.Comparator.comparingLong(
                      path -> Long.parseLong(path.getFileName().toString().substring(4))))
              .orElseThrow();
    }
    var metadata = org.apache.flink.test.util.TestUtils.loadCheckpointMetadata(retained.toString());
    assertTrue(
        metadata.getOperatorStates().stream()
            .filter(operator -> CHANNEL_OPERATORS.contains(operator.getOperatorID()))
            .flatMap(operator -> operator.getStates().stream())
            .anyMatch(
                state ->
                    !state.getInputChannelState().isEmpty()
                        || !state.getResultSubpartitionState().isEmpty()),
        "checkpoint did not retain any Arrow exchange channel state");
    run(after, table, retained, keyed);
    table.refresh();
    var seen = new HashSet<Integer>();
    try (var records = IcebergGenerics.read(table).build()) {
      for (var row : records) {
        int id = (Integer) row.getField("id");
        assertTrue(seen.add(id), "duplicate after captured-channel restore");
        assertEquals(id % 97, row.getField("k"));
      }
    }
    assertEquals(ROWS, seen.size());
    NativeIcebergSqlTest.assertNativeDataFiles(table, "captured-channel rescale");

    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void run(int parallelism, Table table, Path restore, boolean keyed) throws Exception {
    var config =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type", "none", "taskmanager.memory.task.off-heap.size", "1gb"));
    config.set(
        CheckpointingOptions.CHECKPOINTS_DIRECTORY, temp.resolve("checkpoints").toUri().toString());
    config.set(
        CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    if (restore != null)
      config.set(StateRecoveryOptions.SAVEPOINT_PATH, restore.toUri().toString());
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(parallelism);
    env.setMaxParallelism(MAX);
    env.enableCheckpointing(50);
    env.getCheckpointConfig().enableUnalignedCheckpoints();
    var input =
        env.fromSequence(0, 1000000)
            .setParallelism(1)
            .uid("source")
            .flatMap(
                (Long index, org.apache.flink.util.Collector<ArrowBatch> out) -> {
                  if (index >= ROWS / 64) {
                    PAST_END.set(true);
                    java.util.concurrent.locks.LockSupport.parkNanos(5_000_000);
                    return;
                  }
                  var keys = new IntVector("k", NativeAllocator.SHARED);
                  var ids = new IntVector("id", NativeAllocator.SHARED);
                  var root = VectorSchemaRoot.of(keys, ids);
                  root.allocateNew();
                  for (int i = 0; i < 64; i++) {
                    int id = Math.toIntExact(index * 64 + i);
                    keys.setSafe(i, id % 97);
                    ids.setSafe(i, id);
                  }
                  root.setRowCount(64);
                  out.collect(new ArrowBatch(root));
                })
            .returns(ArrowBatchTypeInformation.INSTANCE)
            .setParallelism(1)
            .uid("arrow-payload");
    var sink =
        NativeIcebergSink.append(
            input,
            table,
            TableLoader.fromHadoopTable(table.location()),
            config,
            Map.of("distribution-mode", "hash", "upsert-enabled", Boolean.toString(keyed)),
            "captured");
    var pending = new java.util.ArrayDeque<org.apache.flink.api.dag.Transformation<?>>();
    pending.add(sink.getTransformation());
    boolean inserted = false;
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (next instanceof OneInputTransformation writer
          && writer.getOperatorFactory() instanceof SimpleOperatorFactory<?> factory
          && factory
              .getOperator()
              .getClass()
              .getName()
              .equals("org.apache.iceberg.flink.sink.IcebergStreamWriter")) {
        assertEquals(MAX, writer.getMaxParallelism());
        var exchange =
            (org.apache.flink.api.dag.Transformation<ArrowBatch>) writer.getInputs().get(0);
        var slow =
            new DataStream<>(env, exchange)
                .map(new SlowWriter())
                .returns(ArrowBatchTypeInformation.INSTANCE)
                .uid("slow-writer")
                .name("capture-slow-writer")
                .setParallelism(parallelism)
                .setMaxParallelism(MAX);
        var transformation = (OneInputTransformation) slow.getTransformation();
        transformation.setStateKeySelector(new ArrowBatchSubtaskKeySelector(MAX));
        transformation.setStateKeyType(org.apache.flink.api.common.typeinfo.Types.INT);
        IcebergSinkAdapter.field(OneInputTransformation.class, "input").set(writer, transformation);
        inserted = true;
        break;
      }
      pending.addAll(next.getInputs());
    }
    assertTrue(inserted);
    var graph = env.getStreamGraph();
    if (restore == null) {
      CHANNEL_OPERATORS.clear();
      var hashes =
          new org.apache.flink.streaming.api.graph.StreamGraphHasherV2()
              .traverseStreamGraphAndGenerateHashes(graph);
      for (var node : graph.getStreamNodes())
        if (node.getOperatorName().contains("iceberg-arrow-hash-router")
            || node.getOperatorName().contains("capture-slow-writer"))
          CHANNEL_OPERATORS.add(
              new org.apache.flink.runtime.jobgraph.OperatorID(hashes.get(node.getId())));
      assertEquals(2, CHANNEL_OPERATORS.size());
    }
    PAST_END.set(false);
    var job = env.executeAsync(graph);
    if (restore == null) {
      job.getJobExecutionResult().get(90, java.util.concurrent.TimeUnit.SECONDS);
    } else {
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
      while (!PAST_END.get() && System.nanoTime() < deadline) {
        if (job.getJobStatus().get().isGloballyTerminalState()) job.getJobExecutionResult().get();
        Thread.sleep(10);
      }
      assertTrue(PAST_END.get());
      job.stopWithSavepoint(
              true,
              temp.resolve("drain").toUri().toString(),
              org.apache.flink.core.execution.SavepointFormatType.CANONICAL)
          .get(120, java.util.concurrent.TimeUnit.SECONDS);
      job.getJobExecutionResult().get(120, java.util.concurrent.TimeUnit.SECONDS);
    }
  }

  private static final class SlowWriter extends RichMapFunction<ArrowBatch, ArrowBatch>
      implements CheckpointListener {
    private int rows;

    @Override
    public ArrowBatch map(ArrowBatch batch) {
      rows += batch.rowCount();
      java.util.concurrent.locks.LockSupport.parkNanos(2_000_000);
      return batch;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      if (rows >= 1000 && FAILED.compareAndSet(false, true))
        throw new RuntimeException("intentional failure after captured checkpoint " + checkpointId);
    }
  }
}
