package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.operator.ArrowBatchTypeInformation;

class NativeIcebergSourceRecoveryTest {
  @TempDir Path temp;
  private static final AtomicInteger EMITTED = new AtomicInteger();
  private static final AtomicInteger CHECKPOINTED = new AtomicInteger();

  @Test
  void midFileSavepointRestoresWithNewParallelismAndCommitsEveryRowOnce() throws Exception {
    EMITTED.set(0);
    CHECKPOINTED.set(0);
    var input = NativeIcebergReaderTest.fixture(temp, false);
    NativeIcebergSourceSqlTest.append(input, 101, 2001);
    var output =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                input.schema(),
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                Map.of("format-version", "2", "write.distribution-mode", "none"),
                temp.resolve("output").toUri().toString());
    var first = environment(input, output, 2, null, true).executeAsync("iceberg-source-recovery");
    String savepoint;
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (EMITTED.get() < 21 && System.nanoTime() < deadline) Thread.sleep(5);
      assertTrue(EMITTED.get() >= 21);
      savepoint =
          first
              .triggerSavepoint(
                  temp.resolve("savepoints").toUri().toString(), SavepointFormatType.CANONICAL)
              .get(30, TimeUnit.SECONDS);
      assertTrue(EMITTED.get() < 2001, "savepoint must interrupt a partially emitted task");
    } finally {
      first.cancel().get(30, TimeUnit.SECONDS);
      first.getJobExecutionResult().handle((result, failure) -> null).get(30, TimeUnit.SECONDS);
    }
    assertTrue(CHECKPOINTED.get() > 0 && CHECKPOINTED.get() < 2001);
    EMITTED.set(CHECKPOINTED.get());
    NativeIcebergSourceSqlTest.append(input, 2001, 2004);
    var restored =
        environment(input, output, 3, savepoint, false).executeAsync("iceberg-source-recovery");
    boolean stopped = false;
    try {
      // The restored sink commits only at the drain savepoint; wait for the remaining input first.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (EMITTED.get() < 2004 && System.nanoTime() < deadline) Thread.sleep(5);
      assertTrue(EMITTED.get() >= 2004);
      restored
          .stopWithSavepoint(
              true, temp.resolve("final").toUri().toString(), SavepointFormatType.CANONICAL)
          .get(30, TimeUnit.SECONDS);
      restored.getJobExecutionResult().get(30, TimeUnit.SECONDS);
      stopped = true;
      output.refresh();
      var ids = new java.util.TreeSet<Integer>();
      int count = 0;
      try (var rows = IcebergGenerics.read(output).build()) {
        for (var row : rows) {
          count++;
          assertTrue(ids.add((Integer) row.getField("id")), "duplicate after restore");
        }
      }
      assertEquals(2004, count);
      assertEquals(
          java.util.stream.IntStream.range(0, 2004)
              .boxed()
              .collect(java.util.stream.Collectors.toSet()),
          ids);
      NativeIcebergSqlTest.assertNativeDataFiles(output, "recovered native source/sink");
    } finally {
      if (!stopped) {
        restored.cancel().get(30, TimeUnit.SECONDS);
        restored
            .getJobExecutionResult()
            .handle((result, failure) -> null)
            .get(30, TimeUnit.SECONDS);
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static StreamExecutionEnvironment environment(
      Table input, Table output, int parallelism, String restore, boolean slow) {
    var config =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type",
                "none",
                "taskmanager.memory.task.off-heap.size",
                "256 mb"));
    if (restore != null) config.setString("execution.savepoint.path", restore);
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(parallelism);
    env.setMaxParallelism(16);
    env.enableCheckpointing(60_000);
    var stream =
        NativeIcebergSource.read(
                env,
                input,
                TableLoader.fromHadoopTable(input.location()),
                config,
                Map.of(
                    "streaming",
                    "true",
                    "monitor-interval",
                    "10ms",
                    "starting-strategy",
                    "TABLE_SCAN_THEN_INCREMENTAL"),
                input.schema(),
                7)
            .uid("iceberg-input")
            .map(new Delay(slow))
            .returns(ArrowBatchTypeInformation.INSTANCE)
            .name("delay")
            .uid("delay");
    NativeIcebergSink.append(
        stream,
        output,
        TableLoader.fromHadoopTable(output.location()),
        config,
        Map.of("distribution-mode", "none"),
        "iceberg-output");
    return env;
  }

  private static final class Delay
      extends org.apache.flink.api.common.functions.RichMapFunction<
          tech.streamfusion.operator.ArrowBatch, tech.streamfusion.operator.ArrowBatch>
      implements org.apache.flink.streaming.api.checkpoint.CheckpointedFunction {
    private final boolean slow;

    private Delay(boolean slow) {
      this.slow = slow;
    }

    @Override
    public tech.streamfusion.operator.ArrowBatch map(tech.streamfusion.operator.ArrowBatch batch)
        throws Exception {
      try {
        if (slow) Thread.sleep(20);
        EMITTED.addAndGet(batch.rowCount());
        return batch;
      } catch (Throwable failure) {
        batch.root().close();
        throw failure;
      }
    }

    @Override
    public void initializeState(
        org.apache.flink.runtime.state.FunctionInitializationContext context) {}

    @Override
    public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context) {
      CHECKPOINTED.set(EMITTED.get());
    }
  }
}
