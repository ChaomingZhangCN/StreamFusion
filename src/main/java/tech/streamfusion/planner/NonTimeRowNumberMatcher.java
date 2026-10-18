package tech.streamfusion.planner;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.operator.RowDataArrowConverter;

/** The released Flink 2.2 non-time ROWS rank, with row-identity update pairs. */
final class NonTimeRowNumberMatcher {
  private NonTimeRowNumberMatcher() {}

  static boolean hasShape(StreamPhysicalOverAggregate over) {
    Window window = over.logicWindow();
    if (window.groups.size() != 1) return false;
    Window.Group group = window.groups.get(0);
    if (group.aggCalls.size() != 1
        || group.aggCalls.get(0).getKind() != SqlKind.ROW_NUMBER
        || group.orderKeys.getFieldCollations().size() != 1) return false;
    var orderType =
        over.getInput()
            .getRowType()
            .getFieldList()
            .get(group.orderKeys.getFieldCollations().get(0).getFieldIndex())
            .getType();
    return !FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(orderType)
        && !FlinkTypeFactory$.MODULE$.isProctimeIndicatorType(orderType);
  }

  static boolean matches(StreamPhysicalOverAggregate over) {
    return unsupportedReason(over) == null;
  }

  static String unsupportedReason(StreamPhysicalOverAggregate over) {
    if (!hasShape(over)) return "non-time ROW_NUMBER: requires one numeric rank group";
    Window.Group group = over.logicWindow().groups.get(0);
    var inputType = over.getInput().getRowType();
    var order = group.orderKeys.getFieldCollations().get(0);
    SqlTypeName orderType =
        inputType.getFieldList().get(order.getFieldIndex()).getType().getSqlTypeName();
    if (order.direction != RelFieldCollation.Direction.ASCENDING
        || (orderType != SqlTypeName.INTEGER && orderType != SqlTypeName.BIGINT)) {
      return "non-time ROW_NUMBER: requires one ascending INT or BIGINT order column";
    }
    if (!group.isRows
        || !group.lowerBound.isUnbounded()
        || !group.lowerBound.isPreceding()
        || !group.upperBound.isCurrentRow()) {
      return "non-time ROW_NUMBER: requires ROWS UNBOUNDED PRECEDING through CURRENT ROW";
    }
    if (!ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) over.getInput())) {
      return "non-time ROW_NUMBER: updating inputs require a separate retained-row contract";
    }
    if (ShortcutUtils.unwrapTableConfig(over).getIdleStateRetention().toMillis() > 0) {
      return "non-time ROW_NUMBER: requires disabled idle-state TTL";
    }
    for (int key : group.keys.toArray()) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
          inputType.getFieldList().get(key).getType().getSqlTypeName())) {
        return "non-time ROW_NUMBER: unsupported partition key type";
      }
    }
    if (!ArrowRowTypeSupport.supports(FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType))
        || !RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(over.getRowType()))) {
      return "non-time ROW_NUMBER: unsupported Arrow row payload";
    }
    return null;
  }

  static RelNode substitute(StreamPhysicalOverAggregate over, PlanContext ctx) {
    Window.Group group = over.logicWindow().groups.get(0);
    var order = group.orderKeys.getFieldCollations().get(0);
    int[] partitions = group.keys.toArray();
    return new StreamPhysicalNativeColumnarTopN(
        over.getCluster(),
        over.getTraitSet(),
        ctx.columnarInput(over.getInput(), partitions),
        over.getRowType(),
        partitions,
        new int[] {order.getFieldIndex()},
        new int[] {1},
        new int[] {order.nullDirection == RelFieldCollation.NullDirection.FIRST ? 1 : 0},
        0,
        Long.MAX_VALUE,
        -1,
        true,
        false,
        null,
        true,
        false,
        true);
  }
}
