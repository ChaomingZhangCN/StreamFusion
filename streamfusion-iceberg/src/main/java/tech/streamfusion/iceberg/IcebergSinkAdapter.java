package tech.streamfusion.iceberg;

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import tech.streamfusion.operator.ArrowBatch;

/** Reuses the released legacy sink graph, changing only the writer's payload/factory boundary. */
final class IcebergSinkAdapter {
  private static final String WRITER = "org.apache.iceberg.flink.sink.IcebergStreamWriter";

  private IcebergSinkAdapter() {}

  static boolean compatible() {
    return "1.12.0".equals(IcebergBuild.version());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static DataStreamSink<Void> append(
      DataStream<ArrowBatch> input,
      Table table,
      TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      IcebergTaskWriterFactory.Decorator decorator) {
    if (!compatible()) {
      throw new IllegalArgumentException("Iceberg writer adapter requires released Iceberg 1.12.0");
    }
    FlinkWriteConf writeConf = new FlinkWriteConf(table, options, configuration);
    if (writeConf.distributionMode() != DistributionMode.NONE
        || writeConf.overwriteMode()
        || writeConf.upsertMode()
        || !table.schema().identifierFieldIds().isEmpty()) {
      throw new IllegalArgumentException(
          "Initial Iceberg adapter requires append-only distribution none");
    }
    // No RowData crosses this edge: replace the sole writer consumer before graph serialization.
    DataStream<RowData> hostInput =
        new DataStream<>(
            input.getExecutionEnvironment(), (Transformation) input.getTransformation());
    DataStreamSink<Void> sink =
        FlinkSink.forRowData(hostInput)
            .table(table)
            .tableLoader(loader)
            .flinkConf(configuration)
            .setAll(options)
            .append();
    int replaced = 0;
    for (Transformation<?> transformation : sink.getTransformation().getTransitivePredecessors()) {
      if (!(transformation instanceof OneInputTransformation)) continue;
      var factory = ((OneInputTransformation<?, ?>) transformation).getOperatorFactory();
      if (!(factory instanceof SimpleOperatorFactory)) continue;
      Object operator = ((SimpleOperatorFactory<?>) factory).getOperator();
      if (!operator.getClass().getName().equals(WRITER)) continue;
      Field field = field(operator.getClass(), "taskWriterFactory");
      try {
        RowDataTaskWriterFactory original = (RowDataTaskWriterFactory) field.get(operator);
        field.set(
            operator,
            new IcebergTaskWriterFactory(
                original, FlinkSchemaUtil.convert(table.schema()), decorator));
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Cannot install released Iceberg writer adapter", e);
      }
      replaced++;
    }
    if (replaced != 1) {
      throw new IllegalStateException("Expected one normal Iceberg writer, found " + replaced);
    }
    return sink;
  }

  static Field field(Class<?> owner, String name) {
    try {
      Field field = owner.getDeclaredField(name);
      field.setAccessible(true);
      return field;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unsupported released Iceberg writer layout", e);
    }
  }
}
