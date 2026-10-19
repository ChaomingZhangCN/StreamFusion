package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkOverNestedComplexCountSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void nestedNullableMapAndMultisetKeysSurviveRepeatedRecovery(String backend, boolean multisetOnly)
      throws Exception {
    var array =
        DataTypes.ARRAY(
            multisetOnly
                ? DataTypes.MULTISET(DataTypes.STRING())
                : DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()));
    var struct = DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.MULTISET(DataTypes.STRING())));
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("a", array),
            DataTypes.FIELD("r", struct),
            DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
    var schema =
        Schema.newBuilder().fromRowDataType(type).watermark("rt", "SOURCE_WATERMARK()").build();
    var rows = new ArrayList<Row>();
    for (long i = 0; i < 120; i++) {
      var map = new LinkedHashMap<String, Long>();
      map.put(null, i);
      map.put("nil", null);
      var multiset = new LinkedHashMap<String, Integer>();
      multiset.put(null, 2);
      multiset.put("present", 1);
      Object[] values = i % 3 == 0 ? new Map[0] : new Map[] {null, multisetOnly ? multiset : map};
      rows.add(
          Row.of(
              i,
              i % 7 == 0 ? null : values,
              i % 7 == 0 ? null : Row.of(multiset),
              java.time.LocalDateTime.of(1970, 1, 1, 0, 0)
                  .plusNanos((1000 + i / 12 * 100) * 1000000)));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend, rows, ExternalTypeInfo.<Row>of(type), schema, false, 40, 80)
            .withExecutionMetrics()
            .withWatermarks(Map.of(24, 1100L, 60, 1400L, 96, 1700L, 120, 9999L))) {
      var result =
          NativeFailureParity.runRecovery(
              recovery::uninterrupted,
              recovery::get,
              "SELECT id, COUNT(a) OVER w, COUNT(r) OVER w FROM recovery_input "
                  + "WINDOW w AS (ORDER BY rt ROWS UNBOUNDED PRECEDING)");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertFalse(result.host().rows().isEmpty());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeOverAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("nested-over-count-recovery");
    }
  }
}
