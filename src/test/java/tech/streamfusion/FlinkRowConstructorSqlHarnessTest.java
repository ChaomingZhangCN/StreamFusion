package tech.streamfusion;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkRowConstructorSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void nativeRowsKeepFieldTypesValuesAndNulls(String type) throws Exception {
    FlinkArrayEditSqlHarnessTest.verify(type,"ROW(n,n)","ROW");
    FlinkArrayEditSqlHarnessTest.verify(type,"(ROW(n,n)).EXPR$0","ROW");
  }
}
