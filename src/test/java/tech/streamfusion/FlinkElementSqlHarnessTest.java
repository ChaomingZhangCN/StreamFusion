package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.table.api.TableRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkElementSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void singletonsEmptyAndNullArraysMatchFlink(String type) throws Exception {
    FlinkArrayEditSqlHarnessTest.verify(type,"ELEMENT(ARRAY_SLICE(a,1,1))","ELEMENT");
  }

  @Test
  void multipleElementsKeepTheExceptionTypeAndMessage() {
    var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT ELEMENT(a) FROM src");
    assertNotNull(runs.host().failure());
    runs.assertFailure(TableRuntimeException.class,"Array has more than one element.",
        runs.host().phase(),NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT ELEMENT(a) FROM src WHERE id<0").assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @Test
  void multipleFailureSitesKeepFlinkRowEvaluationOrder() {
    var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT 1/(id-1),ELEMENT(a) FROM src");
    assertNotNull(runs.host().failure());
    runs.assertFailure(runs.host().rootCause().getClass(),runs.host().phase(),NativeFailureParity.Route.NATIVE);
  }
}
