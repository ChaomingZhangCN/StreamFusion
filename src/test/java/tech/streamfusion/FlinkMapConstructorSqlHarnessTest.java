package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkMapConstructorSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void runtimeKeysAreNotDeduplicatedByValue(String type) throws Exception {
    String t=FlinkArrayEditSqlHarnessTest.sqlType(type);
    FlinkArrayEditSqlHarnessTest.verify(type,"MAP[n,n]","MAP");
    FlinkArrayEditSqlHarnessTest.verify(type,"MAP[n,n,CAST(NULL AS "+t+"),n]","MAP");
  }
  @ParameterizedTest
  @ValueSource(strings={"MAP[n,n,n,n]","MAP[n,n,n,1/n]","MAP[n,1/n,n,n]"})
  void repeatedKeyExpressionsKeepReleasedDeduplicationAndEvaluation(String projection) {
    var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),"SELECT "+projection+" FROM src");
    if(runs.host().failure()==null)runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    else runs.assertFailure(runs.host().rootCause().getClass(),runs.host().phase(),NativeFailureParity.Route.NATIVE);
  }
}
