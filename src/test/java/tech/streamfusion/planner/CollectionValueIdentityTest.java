package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class CollectionValueIdentityTest {
  @Test
  void sensitiveConsumersRequireAnExplicitlyRepresentedColumnarProducer() {
    var config = new Configuration();
    config.setString("streamfusion.experimental.collectionExpressions", "true");
    try (var ignored = NativeConfig.usePlannerConfig(config)) {
      var types = new JavaTypeFactoryImpl();
      var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(types));
      RelNode source =
          LogicalValues.createEmpty(
              cluster,
              types.builder().add("s", types.createSqlType(SqlTypeName.VARCHAR, 20)).build());
      var ordinary =
          new StreamPhysicalRowDataToArrow(
              source.getCluster(), source.getTraitSet(), source, false);
      var represented =
          new StreamPhysicalRowDataToArrow(
              source.getCluster(), source.getTraitSet(), source, false, null, true);
      assertTrue(CollectionValueIdentity.unverifiedInput(consumer(ordinary)));
      assertFalse(CollectionValueIdentity.unverifiedInput(consumer(represented)));
      var encoded =
          RexExpression.encodeProjections(
              List.of(RexInputRef.of(0, source.getRowType())), List.of("s"));
      assertNotNull(encoded);
      var ordinaryCalc =
          new StreamPhysicalNativeCalc(
              source.getCluster(), source.getTraitSet(), represented, source.getRowType(), encoded);
      assertTrue(CollectionValueIdentity.unverifiedInput(consumer(ordinaryCalc)));
    }
  }

  private static LogicalCalc consumer(RelNode input) {
    var rex = input.getCluster().getRexBuilder();
    var array =
        rex.makeCall(
            SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, RexInputRef.of(0, input.getRowType()));
    var output = rex.getTypeFactory().builder().add("a", array.getType()).build();
    return LogicalCalc.create(
        input, RexProgram.create(input.getRowType(), List.of(array), null, output, rex));
  }
}
