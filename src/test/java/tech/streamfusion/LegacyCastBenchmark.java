package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class LegacyCastBenchmark {
  @Test
  void legacyBasicCasts() throws Exception {
    for (String expression : new String[] {"CAST(s AS DECIMAL(10,2))", "CAST(b AS VARCHAR)",
        "CAST(s AS CHAR(3))", "CAST(c AS CHAR(3))", "CAST(d AS VARCHAR)"}) {
      BasicTypeBenchmark.run(expression, "SELECT " + expression + " FROM src", true, false, "NativeCalc");
    }
  }
}
