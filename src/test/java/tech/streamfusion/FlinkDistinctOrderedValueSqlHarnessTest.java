package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDistinctOrderedValueSqlHarnessTest {
  private static final String ORDERED =
      "FIRST_VALUE(DISTINCT f), LAST_VALUE(DISTINCT f), FIRST_VALUE(DISTINCT d), "
          + "LAST_VALUE(DISTINCT d)";
  private static final String REDUCTIONS =
      ORDERED + ", COUNT(DISTINCT f), COUNT(DISTINCT d), COUNT(*)";

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void floatingFirstOccurrencesSurviveDuplicateRemovalsAndGroupRecreation(boolean retract)
      throws Exception {
    Supplier<TableEnvironment> setup =
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          var stream =
              env.fromSequence(0, retract ? 16 : 7)
                  .map(
                      index -> {
                        int slot = (int) (index % 8);
                        return row(
                            slot, index >= 8 && index < 16 ? RowKind.DELETE : RowKind.INSERT);
                      })
                  .returns(
                      Types.ROW_NAMED(
                          new String[] {"k", "f", "d"}, Types.INT, Types.FLOAT, Types.DOUBLE));
          table.createTemporaryView(
              "src",
              retract
                  ? table.fromChangelogStream(stream, schema())
                  : table.fromDataStream(stream, schema()));
          configure(table);
          return table;
        };
    for (String reductions :
        List.of(ORDERED, ORDERED + ", COUNT(DISTINCT f), COUNT(DISTINCT d)", REDUCTIONS)) {
      String sql = "SELECT k, " + reductions + " FROM src GROUP BY k";
      assertTrue(NativePlanner.explain(setup.get(), sql).contains("NativeColumnarGroupAggregate"));
      NativeParity.assertOrderedKindedParity(setup, sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void restoredFloatingMembershipRetainsArrivalOrderAndMultiplicity(String backend)
      throws Exception {
    List<Row> input = new ArrayList<>();
    for (int id = 0; id < 96; id++) {
      input.add(row(id % 8, id < 64 ? RowKind.INSERT : RowKind.DELETE));
    }
    for (String reductions :
        List.of(ORDERED, ORDERED + ", COUNT(DISTINCT f), COUNT(DISTINCT d)", REDUCTIONS)) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  input,
                  Types.ROW_NAMED(
                      new String[] {"k", "f", "d"}, Types.INT, Types.FLOAT, Types.DOUBLE),
                  schema(),
                  true,
                  24,
                  72)
              .withExecutionMetrics()) {
        var result =
            NativeFailureParity.runRecovery(
                () -> configure(recovery.uninterrupted()),
                () -> configure(recovery.get()),
                "SELECT k, " + reductions + " FROM recovery_input GROUP BY k");
        result.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(
            recovery.nativeExecution(), "NativeColumnarGroupAggregate");
        SharedFlinkCluster.assertNativeMemoryReleased("distinct-floating-first-last-recovery");
      }
    }
  }

  private static TableEnvironment configure(TableEnvironment table) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
    return table;
  }

  private static Schema schema() {
    return Schema.newBuilder()
        .column("k", DataTypes.INT())
        .column("f", DataTypes.FLOAT())
        .column("d", DataTypes.DOUBLE())
        .build();
  }

  private static Row row(int slot, RowKind kind) {
    long[] bits = {
      Double.doubleToRawLongBits(-0.0),
      Double.doubleToRawLongBits(0.0),
      Double.doubleToRawLongBits(-0.0),
      Double.doubleToRawLongBits(3.5),
      0x7ff8000000000001L,
      0xfff8000000000002L,
      Double.doubleToRawLongBits(Double.POSITIVE_INFINITY),
      Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY)
    };
    double value = Double.longBitsToDouble(bits[slot]);
    float narrow =
        slot == 4
            ? Float.intBitsToFloat(0x7fc00001)
            : slot == 5 ? Float.intBitsToFloat(0xffc00002) : (float) value;
    return Row.ofKind(kind, slot < 4 ? 1 : 2, narrow, value);
  }
}
