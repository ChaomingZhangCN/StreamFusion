package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@org.junit.jupiter.api.condition.EnabledIfSystemProperty(
    named = "streamfusion.experimental.jvmGroupAggregates",
    matches = "true")
class FlinkJvmAggregateSqlHarnessTest {
  @Test
  void appendAndCdcRetainReleasedFunctionChangelogAndLifecycle() {
    for (String source : new String[] {"test_input", "cdc_input"}) {
      var comparison =
          NativeFailureParity.run(
              () -> PortableSqlFixtures.environment(RuntimeExecutionMode.STREAMING, Map.of()),
              "SELECT k, test_aggregate(v) FROM " + source + " GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(perKey(comparison.host().rows()), perKey(comparison.nativeRun().rows()));
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertTrue(comparison.nativeRun().plan().contains("NativeJvmGroup"));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,1",
    "false,true,1",
    "true,false,1",
    "true,true,1",
    "false,false,3",
    "false,true,3",
    "true,false,3",
    "true,true,3"
  })
  void numericKeysKeepNullExtremesRetractionsAndReinsertions(
      boolean wide, boolean updating, int parallelism) throws Exception {
    var rows = numericRows(wide, updating);
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), parallelism, 0);
                var table = StreamTableEnvironment.create(env);
                table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
                var input =
                    tech.streamfusion.compat.FlinkTestSources.fromData(
                        env,
                        Types.ROW_NAMED(
                            new String[] {"k", "v"}, wide ? Types.LONG : Types.INT, Types.LONG),
                        rows.toArray(new Row[0]));
                table.createTemporaryView(
                    "numeric_input", table.fromChangelogStream(input, numericSchema(wide)));
                return register(table);
              },
              "SELECT k, test_aggregate(v) FROM numeric_input GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(perKey(comparison.host().rows()), perKey(comparison.nativeRun().rows()));
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
      if (Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0) > 0)
        assertTrue(metrics.counterValue("jvmAccumulatorCacheHits") > 0);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void accumulatorsRestoreAcrossRepeatedFailures(String backend, boolean updating)
      throws Exception {
    var rows = numericRows(false, updating);
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
                numericSchema(false),
                updating,
                16,
                40)
            .withExecutionMetrics()) {
      var comparison =
          NativeFailureParity.runRecovery(
              () -> register(recovery.uninterrupted()),
              () -> register(recovery.get()),
              "SELECT k, test_aggregate(v) FROM recovery_input GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
      SharedFlinkCluster.assertNativeMemoryReleased("jvm-aggregate-recovery");
    }
  }

  @Test
  void keyReuseReadsNonleadingGroupingColumn() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var rows = numericRows(true, true);
      var comparison =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 3, 0);
                var table = StreamTableEnvironment.create(env);
                table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
                var input =
                    tech.streamfusion.compat.FlinkTestSources.fromData(
                        env,
                        Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
                        rows.toArray(new Row[0]));
                table.createTemporaryView(
                    "numeric_input", table.fromChangelogStream(input, numericSchema(true)));
                return register(table);
              },
              "SELECT v, test_aggregate(k) FROM numeric_input GROUP BY v");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(
          comparison.nativeRun().plan().contains("grouping=[[1]]"), comparison.nativeRun().plan());
      assertEquals(perKey(comparison.host().rows()), perKey(comparison.nativeRun().rows()));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
      if (Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0) > 0)
        assertTrue(metrics.counterValue("jvmAccumulatorCacheHits") > 0);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void managedDataViewsKeepTheirLogicalKeyAlongsideCachedAccumulators(boolean updating)
      throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var rows = numericRows(false, updating);
      var comparison =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 3, 0);
                var table = StreamTableEnvironment.create(env);
                table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
                var input =
                    tech.streamfusion.compat.FlinkTestSources.fromData(
                        env,
                        Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
                        rows.toArray(new Row[0]));
                table.createTemporaryView(
                    "numeric_input", table.fromChangelogStream(input, numericSchema(false)));
                return registerDataViews(table);
              },
              "SELECT k, view_aggregate(v), test_aggregate(v), COUNT(*) FROM numeric_input GROUP BY"
                  + " k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(perKey(comparison.host().rows()), perKey(comparison.nativeRun().rows()));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
      if (Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0) > 0)
        assertTrue(metrics.counterValue("jvmAccumulatorCacheHits") > 0);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void managedDataViewsRestoreWithCachedAccumulators(String backend, boolean updating)
      throws Exception {
    var rows = numericRows(false, updating);
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
                numericSchema(false),
                updating,
                16,
                40)
            .withExecutionMetrics()) {
      var comparison =
          NativeFailureParity.runRecovery(
              () -> registerDataViews(recovery.uninterrupted()),
              () -> registerDataViews(recovery.get()),
              "SELECT k, view_aggregate(v), test_aggregate(v), COUNT(*) FROM recovery_input GROUP"
                  + " BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
      SharedFlinkCluster.assertNativeMemoryReleased("jvm-aggregate-dataview-recovery");
    }
  }

  public static class ViewState {
    public org.apache.flink.table.api.dataview.ListView<Long> values =
        new org.apache.flink.table.api.dataview.ListView<>();
    public long count;
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("BIGINT"),
      output = @org.apache.flink.table.annotation.DataTypeHint("STRING"))
  public static class ViewAggregate
      extends org.apache.flink.table.functions.AggregateFunction<String, ViewState> {
    @Override
    public ViewState createAccumulator() {
      return new ViewState();
    }

    public void accumulate(ViewState state, Long value) throws Exception {
      if (value != null) {
        state.values.add(value);
        state.count++;
      }
    }

    public void retract(ViewState state, Long value) throws Exception {
      if (value != null) {
        state.values.remove(value);
        state.count--;
      }
    }

    public void resetAccumulator(ViewState state) {
      state.values.clear();
      state.count = 0;
    }

    public void merge(ViewState state, Iterable<ViewState> others) throws Exception {
      for (var other : others) {
        for (var value : other.values.get()) state.values.add(value);
        state.count += other.count;
      }
    }

    @Override
    public String getValue(ViewState state) {
      if (state.count == 0) return null;
      try {
        var values = new ArrayList<Long>();
        for (var value : state.values.get()) values.add(value);
        java.util.Collections.sort(values);
        return values.toString();
      } catch (Exception failure) {
        throw new IllegalStateException("DataView read failed", failure);
      }
    }
  }

  private static TableEnvironment registerDataViews(TableEnvironment table) {
    register(table);
    table.createTemporarySystemFunction("view_aggregate", new ViewAggregate());
    return table;
  }

  @Test
  void unsupportedStateModesAndOperatorDisableKeepWholeIslandFallback() {
    for (var settings :
        java.util.List.of(
            Map.of("table.exec.state.ttl", "5 s"),
            Map.of(
                "table.exec.mini-batch.enabled",
                "true",
                "table.exec.mini-batch.size",
                "100",
                "table.exec.mini-batch.allow-latency",
                "10 ms"),
            Map.of("streamfusion.operator.jvmGroupAggregate.enabled", "false"))) {
      var comparison =
          NativeFailureParity.run(
              () -> PortableSqlFixtures.environment(RuntimeExecutionMode.STREAMING, settings),
              "SELECT k, test_aggregate(v) FROM test_input GROUP BY k");
      if ("true".equals(settings.get("table.exec.mini-batch.enabled"))) {
        // The released host's processing-time trigger may emit different intermediate batches.
        assertNull(comparison.host().failure());
        assertNull(comparison.nativeRun().failure());
        assertEquals(NativeFailureParity.Route.FALLBACK, comparison.nativeRun().route());
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        var expected = Map.of(java.util.List.of("A", 27L), 1L, java.util.List.of("B", 8L), 1L);
        assertEquals(expected, SqlAuditHarness.materialized(comparison.host().rows()));
        assertEquals(expected, SqlAuditHarness.materialized(comparison.nativeRun().rows()));
      } else comparison.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertFalse(comparison.nativeRun().plan().contains("NativeJvmGroup"));
    }
  }

  @Test
  void distinctAndFilteredFunctionsKeepExplicitFallback() {
    for (String aggregate :
        new String[] {"test_aggregate(DISTINCT v)", "test_aggregate(v) FILTER (WHERE v > 0)"}) {
      var comparison =
          NativeFailureParity.run(
              () -> PortableSqlFixtures.environment(RuntimeExecutionMode.STREAMING, Map.of()),
              "SELECT k, " + aggregate + " FROM test_input GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertFalse(comparison.nativeRun().plan().contains("NativeJvmGroup"));
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"open", "eval", "close"})
  void lifecycleFailuresKeepTheirOriginalCause(String phase) {
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table = PortableSqlFixtures.environment(RuntimeExecutionMode.STREAMING, Map.of());
              table.createTemporarySystemFunction("failing_aggregate", new FailingAggregate(phase));
              return table;
            },
            "SELECT k, failing_aggregate(v) FROM test_input GROUP BY k");
    assertNotNull(comparison.host().failure());
    assertNotNull(comparison.nativeRun().failure());
    assertEquals(
        comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
    assertEquals(
        comparison.host().rootCause().getMessage(),
        comparison.nativeRun().rootCause().getMessage());
    assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
  }

  public static class FailingAggregate extends PortableSqlFixtures.Sum {
    private final String phase;

    public FailingAggregate(String phase) {
      this.phase = phase;
    }

    @Override
    public void open(org.apache.flink.table.functions.FunctionContext context) {
      super.open(context);
      if (phase.equals("open")) throw new IllegalStateException("aggregate open failure");
    }

    @Override
    public void accumulate(PortableSqlFixtures.SumState state, Long value) {
      if (phase.equals("eval")) throw new IllegalStateException("aggregate evaluation failure");
      super.accumulate(state, value);
    }

    @Override
    public void close() {
      super.close();
      if (phase.equals("close")) throw new IllegalStateException("aggregate close failure");
    }
  }

  private static Map<Object, java.util.List<java.util.List<Object>>> perKey(
      java.util.List<java.util.List<Object>> rows) {
    var result = new java.util.HashMap<Object, java.util.List<java.util.List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }

  private static Schema numericSchema(boolean wide) {
    return Schema.newBuilder()
        .column("k", wide ? DataTypes.BIGINT() : DataTypes.INT())
        .column("v", DataTypes.BIGINT())
        .build();
  }

  private static ArrayList<Row> numericRows(boolean wide, boolean updating) {
    Object[] keys;
    if (wide) keys = new Object[] {null, 0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 1L << 32};
    else keys = new Object[] {null, 0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 1 << 16};
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++)
      rows.add(Row.of(keys[(i / 2) % keys.length], i % 8 == 0 ? null : (long) i));
    if (updating) {
      for (int i = 0; i < 48; i++)
        rows.add(
            Row.ofKind(RowKind.DELETE, keys[(i / 2) % keys.length], i % 8 == 0 ? null : (long) i));
      for (int i = 0; i < 12; i++) rows.add(Row.of(keys[i % keys.length], (long) i));
      rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, keys[0], 0L));
      rows.add(Row.ofKind(RowKind.UPDATE_AFTER, keys[0], 99L));
    }
    return rows;
  }

  private static TableEnvironment register(TableEnvironment table) {
    table.createTemporarySystemFunction("test_aggregate", new PortableSqlFixtures.Sum());
    return table;
  }
}
