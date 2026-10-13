package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class WindowAggregateValueTypeTest {
  @Test
  void stringTypesAreAdmittedOnlyForExtrema() {
    for (var type : new SqlTypeName[] {SqlTypeName.CHAR, SqlTypeName.VARCHAR}) {
      assertTrue(
          WindowAggregateMatcher.supportedAggregateValueType(
              WindowAggregateMatcher.KIND_MIN, type));
      assertTrue(
          WindowAggregateMatcher.supportedAggregateValueType(
              WindowAggregateMatcher.KIND_MAX, type));
      assertFalse(WindowAggregateMatcher.supportedValueType(type));
      for (int kind :
          new int[] {
            WindowAggregateMatcher.KIND_SUM,
            WindowAggregateMatcher.KIND_AVG,
            WindowAggregateMatcher.KIND_COUNT
          }) {
        assertFalse(WindowAggregateMatcher.supportedAggregateValueType(kind, type));
      }
    }
  }

  @Test
  void extremaDoNotAdmitUnverifiedTypes() {
    for (var type :
        new SqlTypeName[] {
          SqlTypeName.BOOLEAN,
          SqlTypeName.DATE,
          SqlTypeName.TIMESTAMP,
          SqlTypeName.BINARY,
          SqlTypeName.ARRAY,
          SqlTypeName.MAP
        }) {
      assertFalse(
          WindowAggregateMatcher.supportedAggregateValueType(
              WindowAggregateMatcher.KIND_MIN, type));
      assertFalse(
          WindowAggregateMatcher.supportedAggregateValueType(
              WindowAggregateMatcher.KIND_MAX, type));
    }
  }
}
