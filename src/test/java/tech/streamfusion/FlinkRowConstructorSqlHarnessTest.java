package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkRowConstructorSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void nativeRowsKeepFieldTypesValuesAndNulls(String type) throws Exception {
    if (!tech.streamfusion.compat.FlinkCompat.NARROW_CONSTRUCTORS_COMPILE
        && java.util.Set.of("TINYINT","SMALLINT").contains(type)) {
      for(String expression:java.util.List.of("ROW(n,n)","(ROW(n,n)).EXPR$0")) {
        var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment(type),"SELECT "+expression+" FROM src");
        assertNotNull(runs.host().failure());
        runs.assertFailure(runs.host().rootCause().getClass(),runs.host().phase(),NativeFailureParity.Route.FALLBACK);
      }
      return;
    }
    FlinkArrayEditSqlHarnessTest.verify(type,"ROW(n,n)","ROW");
    FlinkArrayEditSqlHarnessTest.verify(type,"(ROW(n,n)).EXPR$0","ROW");
  }
}
