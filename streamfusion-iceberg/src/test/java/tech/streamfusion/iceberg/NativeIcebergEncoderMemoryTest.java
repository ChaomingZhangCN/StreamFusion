package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

class NativeIcebergEncoderMemoryTest {
  @TempDir Path temp;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encoderRetentionIsChargedAndReleasedOnCloseOrQuotaFailure(boolean exhaust) throws Exception {
    TaskOffHeapMemory.initialize(
        Configuration.fromMap(Map.of("taskmanager.memory.task.off-heap.size", "8mb")));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                new Schema(Types.NestedField.required(17, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"),
                temp.resolve("table").toUri().toString());
    var context = new ArrowWriteContext();
    var settings = IcebergParquetSettings.translate(Map.of("parquet.enable.dictionary", "false"));
    try (var ids = new IntVector("id", NativeAllocator.SHARED)) {
      ids.allocateNew(10000);
      for (int i = 0; i < 10000; i++) ids.set(i, i);
      ids.setValueCount(10000);
      try (var root = VectorSchemaRoot.of(ids);
          var appender =
              new NativeIcebergAppender(
                  context,
                  table.io().newOutputFile(table.location() + "/data.parquet"),
                  SchemaParser.toJson(table.schema()),
                  settings,
                  MetricsConfig.forTable(table))) {
        context.begin(root);
        var rows = ArrowConversion.createArrowReader(root, FlinkSchemaUtil.convert(table.schema()));
        for (int i = 0; i < 64; i++) {
          var row = rows.read(i);
          context.position(i, row);
          appender.add(row);
        }
        context.drain();
        assertTrue(
            TaskOffHeapMemory.reservedBytes() > NativeAllocator.SHARED.getAllocatedMemory(),
            "encoder buffers must be charged in addition to shared Arrow allocations");
        if (exhaust) {
          long remaining = TaskOffHeapMemory.availableBytes() - 4096;
          try (var held =
              TaskOffHeapMemory.reserve("fixture", "leave room for C Data structs", remaining)) {
            var failure =
                assertThrows(
                    NativeMemoryLimitException.class,
                    () -> {
                      for (int i = 64; i < 9000; i++) {
                        var row = rows.read(i);
                        context.position(i, row);
                        appender.add(row);
                      }
                      context.drain();
                    });
            context.fail(failure);
          }
        } else {
          for (int i = 64; i < 9000; i++) {
            var row = rows.read(i);
            context.position(i, row);
            appender.add(row);
          }
          context.drain();
          appender.close();
          assertEquals(9000, appender.metrics().recordCount());
        }
        context.clear();
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
    assertEquals(0L, NativeAllocator.SHARED.getAllocatedMemory());
    assertEquals(0L, TaskOffHeapMemory.reservedBytes());
  }
}
