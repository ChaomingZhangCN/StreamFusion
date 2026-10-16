package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class LegacyCastBenchmark {
  @Test
  void legacyFloatingDecimalCasts() throws Exception {
    var type = org.apache.flink.table.api.DataTypes.ROW(
        org.apache.flink.table.api.DataTypes.FIELD("f", org.apache.flink.table.api.DataTypes.FLOAT()),
        org.apache.flink.table.api.DataTypes.FIELD("x", org.apache.flink.table.api.DataTypes.DOUBLE()));
    String selected = System.getProperty("basic.expression");
    for (String expression : selected == null
        ? new String[] {"CAST(f AS DECIMAL(10,1))", "CAST(x AS DECIMAL(15,4))"}
        : new String[] {selected}) {
      BasicTypeBenchmark.run(expression, "SELECT " + expression + " FROM src", true, false, "NativeCalc", type,
          i -> org.apache.flink.types.Row.of(i % 7 == 0 ? null : (float) (i % 9999) / 10,
              i % 7 == 0 ? null : (double) (i % 9999) / 10));
    }
  }

  @Test
  void legacyCharDoubleCasts() throws Exception {
    var type = org.apache.flink.table.api.DataTypes.ROW(
        org.apache.flink.table.api.DataTypes.FIELD("c", org.apache.flink.table.api.DataTypes.CHAR(8)));
    BasicTypeBenchmark.run("CHAR8_DOUBLE", "SELECT CAST(c AS DOUBLE) FROM src", true, false,
        "NativeCalc", type,
        i -> org.apache.flink.types.Row.of(i % 7 == 0 ? null : (i % 1000) + ".125"));
  }

  @Test
  void legacyBasicCasts() throws Exception {
    for (String expression : new String[] {"CAST(s AS DECIMAL(10,2))", "CAST(b AS VARCHAR)",
        "CAST(s AS CHAR(3))", "CAST(c AS CHAR(3))", "CAST(d AS VARCHAR)"}) {
      BasicTypeBenchmark.run(expression, "SELECT " + expression + " FROM src", true, false, "NativeCalc");
    }
  }
}
