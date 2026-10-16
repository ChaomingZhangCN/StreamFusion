package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.iceberg.flink.IcebergTableSink;
import tech.streamfusion.iceberg.IcebergSinkPlan;

final class IcebergSinkMatcher {
  private IcebergSinkMatcher() {}

  static boolean appliesTo(StreamPhysicalSink sink) {
    return sink.tableSink() instanceof IcebergTableSink;
  }

  static RelNode substitute(StreamPhysicalSink sink, PlanContext context) {
    if (!Boolean.getBoolean("streamfusion.experimental.iceberg.enabled")) {
      context.decline(
          "Iceberg acceleration requires experimental opt-in until release comparisons pass");
      return null;
    }
    if (!NativeConfig.operatorEnabled("icebergSink")) {
      context.decline(Substitution.disabledReason("icebergSink"));
      return null;
    }
    if (sink.upsertMaterialize()
        || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInput())) {
      context.decline(
          "Iceberg append acceleration requires insert-only input without upsert materialization");
      return null;
    }
    try {
      IcebergSinkPlan plan =
          IcebergSinkPlan.capture(
              (IcebergTableSink) sink.tableSink(),
              FlinkTypeFactory.toLogicalRowType(sink.getInput().getRowType()));
      return new StreamPhysicalNativeIcebergSink(
          sink.getCluster(), sink.getTraitSet(), sink.getInput(), sink.getRowType(), plan);
    } catch (IllegalArgumentException | LinkageError e) {
      context.decline("Iceberg sink: " + e.getMessage());
      return null;
    }
  }
}
