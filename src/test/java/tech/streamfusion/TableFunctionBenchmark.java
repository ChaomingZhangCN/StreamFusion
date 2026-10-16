package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TableFunctionBenchmark {
  @Test
  void completeJobsRetainSourceBothTransposesAndRowSink() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    int rows = Integer.getInteger("tablefunc.rows", 2_000_000);
    int keys = Integer.getInteger("tablefunc.keys", 16_384);
    int width = Integer.getInteger("tablefunc.width", 16);
    int warmups = Integer.getInteger("tablefunc.warmup", 2);
    int runs = Integer.getInteger("tablefunc.runs", 5);
    boolean expectedNative =
        Boolean.parseBoolean(System.getProperty("tablefunc.expectNative", "true"));
    for (String shape : System.getProperty("tablefunc.cases", "split,group").split(",")) {
      String lateral = " FROM src s, LATERAL TABLE(test_split(s.text_value)) AS t(v, pos)";
      String sql =
          shape.equals("split")
              ? "SELECT s.k, t.v, t.pos" + lateral
              : "SELECT s.k, t.pos, JSON_ARRAYAGG(t.v)" + lateral + " GROUP BY s.k, t.pos";
      double[][] elapsed = new double[2][runs];
      for (int trial = 0; trial < warmups + runs; trial++) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          for (int order = 0; order < 2; order++) {
            int engine = (trial + order) & 1;
            var table = environment(metrics, rows, keys, width);
            table.createTemporaryTable(
                "sink",
                TableDescriptor.forConnector("blackhole")
                    .schema(
                        Schema.newBuilder()
                            .fromRowDataType(
                                table.sqlQuery(sql).getResolvedSchema().toPhysicalRowDataType())
                            .build())
                    .build());
            String insert = "INSERT INTO sink " + sql;
            if (engine == 1) {
              var scan = NativePlanner.install(table);
              String plan = table.explainSql(insert);
              if (expectedNative)
                assertTrue(
                    plan.contains("NativeTableFunction")
                        && plan.contains("RowDataToArrow")
                        && plan.contains("ArrowToRowData"),
                    plan);
              else assertEquals(0, scan.substitutions(), plan);
            }
            long start = System.nanoTime();
            table.executeSql(insert).await();
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.printf(
                Locale.ROOT,
                "[table-function-trial] shape=%s engine=%s trial=%d warmup=%s rows=%d keys=%d"
                    + " width=%d seconds=%.6f%n",
                shape,
                engine == 0 ? "Flink" : "StreamFusion",
                trial,
                trial < warmups,
                rows,
                keys,
                width,
                seconds);
            if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
          }
          if (expectedNative)
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators(), "NativeTableFunction");
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[table-function] shape=%s rows=%d keys=%d width=%d stock=%.6f native=%.6f"
              + " stock_trials=%s native_trials=%s%n",
          shape,
          rows,
          keys,
          width,
          median(elapsed[0]),
          median(elapsed[1]),
          Arrays.toString(elapsed[0]),
          Arrays.toString(elapsed[1]));
    }
  }

  private static StreamTableEnvironment environment(
      RecoveryExecutionMetrics metrics, int rows, int keys, int width) {
    var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    String payload = "é" + "x".repeat(Math.max(0, width - 1));
    var source =
        env.fromSequence(0, Math.max(0, rows - 1))
            .filter(i -> i < rows)
            .map(i -> Row.of((int) (i % keys), i % 8 == 7 ? null : payload + "," + payload))
            .returns(Types.ROW_NAMED(new String[] {"k", "text_value"}, Types.INT, Types.STRING));
    table.createTemporaryView("src", source);
    table.createTemporarySystemFunction("test_split", new PortableSqlFixtures.Split());
    return table;
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
