package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkMapUnionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void rightEntriesWinAndKeepOrderWithDuplicateAndNullKeys(String type) throws Exception {
    String left="MAP_FROM_ARRAYS(a,a)",right="MAP_FROM_ARRAYS(b,b)";
    // Equal operands make each constructor total; different arrays still exercise map union.
    for(String expression:new String[]{"MAP_UNION("+left+","+right+")",
        "MAP_UNION("+right+","+left+")","MAP_UNION("+left+","+right+","+left+")"})
      FlinkArrayEditSqlHarnessTest.verify(type,expression,"MAP_UNION");
    FlinkArrayEditSqlHarnessTest.verify(type,"MAP_KEYS(MAP_UNION("+left+","+right+"))","MAP_UNION");
    FlinkArrayEditSqlHarnessTest.verify(type,"MAP_VALUES(MAP_UNION("+right+","+left+"))","MAP_UNION");
  }
}
