package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkArrayExtremaSqlHarnessTest {
  @ParameterizedTest
  @MethodSource("integerCases")
  void integerExtremaUseReleasedGeneratedOrdering(String function, String type) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    BuiltinFunctionParity.assertParity(
        () -> FlinkArrayDistinctSqlHarnessTest.environment(type),
        "SELECT id, " + function + "(a), " + function + "(ARRAY_DISTINCT(a)) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> FlinkArrayDistinctSqlHarnessTest.environment(type),
        "SELECT " + function + "(a) FROM src WHERE " + function + "(a) < 0");
    BuiltinFunctionParity.assertParity(
        () -> FlinkArrayDistinctSqlHarnessTest.environment(type),
        "SELECT " + function + "(a) FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void longAndNonNullElementArraysKeepNullableResults(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    Integer[] values = new Integer[65];
    for (int i = 0; i < values.length; i++) values[i] = 32 - i;
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(INT().notNull()))),
                List.of(
                    Row.of((Object) values),
                    Row.of((Object) new Integer[] {}),
                    Row.of((Object) null))),
        "SELECT " + function + "(a) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void largeNullableArraysPreserveNulls(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    Long[] values = new Long[65];
    for (int i = 0; i < values.length; i++) values[i] = i % 7 == 0 ? null : (long) (32 - i);
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(BIGINT()))),
                List.of(Row.of((Object) values), Row.of((Object) new Long[65]))),
        "SELECT " + function + "(a) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void floatingExtremaUseReleasedGeneratedOrdering(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(DOUBLE()))),
                List.of(Row.of((Object) new Double[] {0.0, -0.0, Double.NaN, null}))),
        "SELECT " + function + "(a) FROM src");
  }

  static Stream<Arguments> integerCases() {
    return Stream.of("ARRAY_MIN", "ARRAY_MAX")
        .flatMap(
            function ->
                Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT")
                    .map(type -> Arguments.of(function, type)));
  }
}
