package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class GroupedValueBenchmark {
  private static final long ROWS = Long.getLong("grouped.value.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("grouped.value.warmup", 2);
  private static final int RUNS = Integer.getInteger("grouped.value.runs", 5);
  private static final boolean DISTINCT = Boolean.getBoolean("grouped.value.distinct");
  private static final boolean SINGLE = Boolean.getBoolean("grouped.value.single");
  private static final boolean MIXED = Boolean.getBoolean("grouped.value.mixed");
  private static final boolean EXPECT_NATIVE =
      Boolean.parseBoolean(System.getProperty("grouped.value.expectNative", "true"));
  private static final boolean TWO_PHASE =
      Boolean.parseBoolean(System.getProperty("grouped.value.twoPhase", Boolean.toString(SINGLE)));
  private static final String SQL =
      SINGLE
          ? "INSERT INTO sink SELECT k, SINGLE_VALUE(v)"
              + (MIXED ? ", AVG(k), COUNT(DISTINCT k)" : "")
              + " FROM inputs GROUP BY k"
          : "INSERT INTO sink SELECT k, FIRST_VALUE("
              + (DISTINCT ? "DISTINCT " : "")
              + "v), LAST_VALUE("
              + (DISTINCT ? "DISTINCT " : "")
              + "v) FROM inputs GROUP BY k";

  @Test
  void groupedValues() throws Exception {
    if (DISTINCT && SINGLE)
      throw new IllegalArgumentException("Select DISTINCT first/last or SINGLE_VALUE");
    if (MIXED && !SINGLE) throw new IllegalArgumentException("Mixed mode requires SINGLE_VALUE");
    for (String type :
        System.getProperty("grouped.value.types", DISTINCT || SINGLE ? "STRING" : "BIGINT,STRING")
            .split(",")) {
      String plan = NativePlanner.explain(environment(type), SQL);
      if (EXPECT_NATIVE
          && (!plan.contains("NativeColumnarGroupAggregate")
              || !plan.contains("RowDataToArrow")
              || !plan.contains("ArrowToRowData")
              || (TWO_PHASE && !plan.contains("NativeColumnarLocalGroupAggregate")))) {
        throw new IllegalStateException("Expected native aggregate and both transposes: " + plan);
      }
      if (!EXPECT_NATIVE && plan.contains("NativeColumnarGroupAggregate"))
        throw new IllegalStateException("Expected previous aggregate fallback: " + plan);
      double[][] times = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          try (var metrics = new RecoveryExecutionMetrics()) {
            TableEnvironment table = environment(type, metrics);
            PhysicalPlanScan scan = engine == 1 ? NativePlanner.install(table) : null;
            long start = System.nanoTime();
            var result = table.executeSql(SQL);
            String jobId = result.getJobClient().orElseThrow().getJobID().toString();
            result.await();
            double seconds = (System.nanoTime() - start) / 1e9;
            if (scan != null && EXPECT_NATIVE && scan.substitutions() == 0)
              throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
            if (scan != null && !EXPECT_NATIVE && scan.substitutions() != 0)
              throw new IllegalStateException(
                  "Expected previous aggregate fallback: " + scan.explainSummary());
            if (scan != null && EXPECT_NATIVE) {
              var execution =
                  metrics.nativeOperators().stream()
                      .filter(row -> jobId.equals(row.get("jobId")))
                      .collect(java.util.stream.Collectors.toList());
              RecoveryExecutionMetrics.assertConsumed(execution, "NativeColumnarGroupAggregate");
              if (TWO_PHASE)
                RecoveryExecutionMetrics.assertConsumed(
                    execution, "NativeColumnarLocalGroupAggregate");
            }
            if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
            System.out.printf(
                Locale.ROOT,
                "[grouped-value-trial] type=%s engine=%d trial=%d warmup=%s seconds=%.6f%n",
                type,
                engine,
                trial,
                trial < WARMUP,
                seconds);
          }
        }
      }
      double host = median(times[0]);
      double nativeTime = median(times[1]);
      System.out.printf(
          Locale.ROOT,
          "[grouped-value] single=%s distinct=%s mixed=%s type=%s two_phase=%s rows=%d Flink=%.6fs"
              + " Native=%.6fs ratio=%.3fx flink_trials=%s native_trials=%s%n",
          SINGLE,
          DISTINCT,
          MIXED,
          type,
          TWO_PHASE,
          ROWS,
          host,
          nativeTime,
          host / nativeTime,
          Arrays.toString(times[0]),
          Arrays.toString(times[1]));
    }
  }

  private static double median(double[] times) {
    double[] sorted = times.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private static TableEnvironment environment(String type) {
    return environment(type, null);
  }

  private static TableEnvironment environment(String type, RecoveryExecutionMetrics metrics) {
    var env =
        metrics == null
            ? StreamExecutionEnvironment.getExecutionEnvironment()
            : metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/New_York"));
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", TWO_PHASE ? "TWO_PHASE" : "ONE_PHASE");
    if (TWO_PHASE) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.size", "1024");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "100 ms");
    }
    org.apache.flink.table.types.DataType dataType;
    switch (type) {
      case "STRING":
        dataType = DataTypes.STRING();
        break;
      case "BIGINT":
        dataType = DataTypes.BIGINT();
        break;
      case "TINYINT":
        dataType = DataTypes.TINYINT();
        break;
      case "SMALLINT":
        dataType = DataTypes.SMALLINT();
        break;
      case "INT":
        dataType = DataTypes.INT();
        break;
      case "FLOAT":
        dataType = DataTypes.FLOAT();
        break;
      case "DOUBLE":
        dataType = DataTypes.DOUBLE();
        break;
      case "CHAR":
        dataType = DataTypes.CHAR(16);
        break;
      case "TIME":
        dataType = DataTypes.TIME(3);
        break;
      case "BOOLEAN":
        dataType = DataTypes.BOOLEAN();
        break;
      case "DATE":
        dataType = DataTypes.DATE();
        break;
      case "DECIMAL":
      case "DECIMAL_WIDE":
        dataType = DataTypes.DECIMAL(20, 3);
        break;
      case "TIMESTAMP":
        dataType = DataTypes.TIMESTAMP(9);
        break;
      case "TIMESTAMP_LTZ":
        dataType = DataTypes.TIMESTAMP_LTZ(9);
        break;
      default:
        throw new IllegalArgumentException("Unknown grouped.value.types entry: " + type);
    }
    org.apache.flink.api.common.typeinfo.TypeInformation<?> valueType;
    switch (type) {
      case "STRING":
      case "CHAR":
        valueType = Types.STRING;
        break;
      case "TINYINT":
        valueType = Types.BYTE;
        break;
      case "SMALLINT":
        valueType = Types.SHORT;
        break;
      case "INT":
        valueType = Types.INT;
        break;
      case "FLOAT":
        valueType = Types.FLOAT;
        break;
      case "DOUBLE":
        valueType = Types.DOUBLE;
        break;
      case "TIME":
        valueType = Types.LOCAL_TIME;
        break;
      case "BOOLEAN":
        valueType = Types.BOOLEAN;
        break;
      case "DATE":
        valueType = Types.LOCAL_DATE;
        break;
      case "DECIMAL":
      case "DECIMAL_WIDE":
        valueType = Types.BIG_DEC;
        break;
      case "TIMESTAMP":
        valueType = Types.LOCAL_DATE_TIME;
        break;
      case "TIMESTAMP_LTZ":
        valueType = Types.INSTANT;
        break;
      default:
        valueType = Types.LONG;
        break;
    }
    table.createTemporaryView(
        "inputs",
        env.fromSequence(0, ROWS - 1)
            .map(
                i -> {
                  Object selectedValue;
                  if (i / 64 % 8 == 0) {
                    selectedValue = null;
                  } else {
                    switch (type) {
                      case "STRING":
                      case "CHAR":
                        selectedValue = "value-" + (i % 1024);
                        break;
                      case "TINYINT":
                        selectedValue = (byte) (i % 127);
                        break;
                      case "SMALLINT":
                        selectedValue = (short) (i % 1024);
                        break;
                      case "INT":
                        selectedValue = (int) (i % 1024);
                        break;
                      case "FLOAT":
                        selectedValue = (float) (i % 1024) / 8;
                        break;
                      case "DOUBLE":
                        selectedValue = (double) (i % 1024) / 8;
                        break;
                      case "TIME":
                        selectedValue = java.time.LocalTime.ofNanoOfDay(i % 1024 * 1_000_000);
                        break;
                      case "BOOLEAN":
                        selectedValue = i / 64 % 2 == 0;
                        break;
                      case "DATE":
                        selectedValue = java.time.LocalDate.of(2024, 1, 1).plusDays(i % 31);
                        break;
                      case "DECIMAL":
                        selectedValue = java.math.BigDecimal.valueOf(i % 1024, 3);
                        break;
                      case "DECIMAL_WIDE":
                        selectedValue =
                            new java.math.BigDecimal("12345678901234567.000")
                                .add(java.math.BigDecimal.valueOf(i % 1024, 3));
                        break;
                      case "TIMESTAMP":
                        selectedValue =
                            java.time.LocalDateTime.of(2024, 1, 1, 0, 0).plusNanos(i % 1024);
                        break;
                      case "TIMESTAMP_LTZ":
                        selectedValue = java.time.Instant.ofEpochSecond(-1, i % 1024);
                        break;
                      default:
                        selectedValue = i % 1024;
                        break;
                    }
                  }
                  return Row.of(SINGLE ? i.intValue() : (int) (i % 64), selectedValue);
                })
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, valueType)),
        Schema.newBuilder().column("k", DataTypes.INT()).column("v", dataType).build());
    String sqlType = dataType.getLogicalType().asSerializableString();
    table.executeSql(
        "CREATE TABLE sink (k INT, first_v "
            + sqlType
            + (SINGLE ? "" : ", last_v " + sqlType)
            + (MIXED ? ", avg_k INT, distinct_k BIGINT" : "")
            + ") WITH ('connector' = 'blackhole')");
    return table;
  }
}
