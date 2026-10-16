package tech.streamfusion.iceberg;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
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
    return "1.12.0".equals(IcebergBuild.version())
        && org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("2.2.");
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
    if ((writeConf.distributionMode() != DistributionMode.NONE
            && writeConf.distributionMode() != DistributionMode.HASH
            && !(writeConf.distributionMode() == DistributionMode.RANGE
                && !table.schema().identifierFieldIds().isEmpty()))
        || writeConf.overwriteMode()) {
      throw new IllegalArgumentException(
          "Iceberg streaming adapter requires none/hash distribution");
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
    for (Transformation<?> transformation : predecessors(sink.getTransformation())) {
      if (!(transformation instanceof OneInputTransformation)) continue;
      var factory = ((OneInputTransformation<?, ?>) transformation).getOperatorFactory();
      if (!(factory instanceof SimpleOperatorFactory)) continue;
      Object operator = ((SimpleOperatorFactory<?>) factory).getOperator();
      if (!operator.getClass().getName().equals(WRITER)) continue;
      adaptHashInput((OneInputTransformation) transformation, input, table);
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

  private static List<Transformation<?>> predecessors(Transformation<?> root) {
    var seen = new IdentityHashMap<Transformation<?>, Boolean>();
    var pending = new java.util.ArrayDeque<Transformation<?>>();
    var result = new ArrayList<Transformation<?>>();
    pending.add(root);
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (seen.put(next, true) != null) continue;
      result.add(next);
      pending.addAll(next.getInputs());
    }
    return result;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void adaptHashInput(
      OneInputTransformation writer, DataStream<ArrowBatch> input, Table table) {
    var selector = writer.getStateKeySelector();
    if (selector == null) return;
    if (!(writer.getInputs().get(0) instanceof PartitionTransformation)) {
      throw new IllegalStateException("Unsupported released Iceberg hash input layout");
    }
    PartitionTransformation original = (PartitionTransformation) writer.getInputs().get(0);
    if (!(original.getPartitioner()
        instanceof org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner)) {
      throw new IllegalStateException("Unsupported released Iceberg hash partitioner");
    }
    int max = writer.getMaxParallelism();
    if (max <= 0) max = input.getExecutionEnvironment().getMaxParallelism();
    if (max <= 0)
      max = KeyGroupRangeAssignment.computeDefaultMaxParallelism(writer.getParallelism());
    boolean recoverable =
        input.getExecutionEnvironment().getCheckpointConfig().isUnalignedCheckpointsEnabled();
    var router =
        new OneInputTransformation<ArrowBatch, ArrowBatch>(
            (Transformation<ArrowBatch>) original.getInputs().get(0),
            "iceberg-arrow-hash-router",
            org.apache.flink.streaming.api.operators.SimpleOperatorFactory.of(
                new IcebergHashRouter(
                    FlinkSchemaUtil.convert(table.schema()),
                    selector,
                    writer.getParallelism(),
                    max,
                    recoverable)),
            tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE,
            original.getParallelism(),
            false);
    var exchange =
        new PartitionTransformation<>(
            router,
            new tech.streamfusion.planner.ColumnarKeyGroupPartitioner(max, recoverable),
            original.getExchangeMode());
    try {
      field(OneInputTransformation.class, "input").set(writer, exchange);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Cannot adapt the released Iceberg batch exchange", e);
    }
    // The writer has no Flink keyed state; its Iceberg key/delete bookkeeping remains in Java.
    writer.setStateKeySelector(null);
    writer.setStateKeyType(null);
    writer.setMaxParallelism(max);
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
