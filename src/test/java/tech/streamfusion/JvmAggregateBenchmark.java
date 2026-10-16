package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class JvmAggregateBenchmark {
  @Test
  void completeJobsRetainKeyedStateAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    int rows = Integer.getInteger("jvmagg.rows", 2_000_000);
    int keys = Integer.getInteger("jvmagg.keys", 65_536);
    int warmups = Integer.getInteger("jvmagg.warmup", 2);
    int runs = Integer.getInteger("jvmagg.runs", 5);
    boolean expectedUdf = Boolean.parseBoolean(System.getProperty("jvmagg.expectNative", "false"));
    for (String shape : System.getProperty("jvmagg.cases", "udaf,builtin").split(",")) {
      boolean udf = shape.equals("udaf");
      String sql = "SELECT k, " + (udf ? "test_aggregate" : "SUM") + "(v) FROM src GROUP BY k";
      double[][] elapsed = new double[2][runs];
      for (int trial = 0; trial < warmups + runs; trial++) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          for (int order = 0; order < 2; order++) {
            int engine = (trial + order) & 1;
            var env = metrics.environment(new Configuration(), 1, 0);
            env.setParallelism(1);
            var table = StreamTableEnvironment.create(env);
            table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
            var source =
                env.fromSequence(0, Math.max(0, rows - 1))
                    .filter(i -> i < rows)
                    .map(i -> Row.of((int) (i % keys), i % 8 == 7 ? null : i))
                    .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG));
            table.createTemporaryView("src", source);
            table.createTemporarySystemFunction("test_aggregate", new PortableSqlFixtures.Sum());
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
              if (!udf || expectedUdf) {
                assertTrue(
                    plan.contains(udf ? "NativeJvmGroup" : "NativeColumnarGroupAggregate")
                        && plan.contains("RowDataToArrow")
                        && plan.contains("ArrowToRowData"),
                    plan);
              } else assertEquals(0, scan.substitutions(), plan);
            }
            long startMillis = System.currentTimeMillis();
            long start = System.nanoTime();
            table.executeSql(insert).await();
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.printf(
                Locale.ROOT,
                "[jvm-aggregate-trial] shape=%s engine=%s trial=%d warmup=%s rows=%d keys=%d"
                    + " start_ms=%d seconds=%.6f%n",
                shape,
                engine == 0 ? "Flink" : "StreamFusion",
                trial,
                trial < warmups,
                rows,
                keys,
                startMillis,
                seconds);
            if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
          }
          if (udf
              && expectedUdf
              && Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0)
                  > 0) {
            long hits = metrics.counterValue("jvmAccumulatorCacheHits");
            if (keys < rows) assertTrue(hits > 0, "accumulator cache did not execute");
            else assertEquals(0, hits, "distinct keys unexpectedly hit the cache");
            System.out.println("[jvm-aggregate-cache] hits=" + hits);
          }
          if (!udf || expectedUdf)
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators(), udf ? "NativeJvmGroup" : "NativeColumnarGroupAggregate");
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[jvm-aggregate] shape=%s rows=%d keys=%d stock=%.6f sf=%.6f stock_trials=%s"
              + " sf_trials=%s%n",
          shape,
          rows,
          keys,
          median(elapsed[0]),
          median(elapsed[1]),
          Arrays.toString(elapsed[0]),
          Arrays.toString(elapsed[1]));
    }
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
