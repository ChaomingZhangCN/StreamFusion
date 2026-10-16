package tech.streamfusion.suite;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Selects unchanged upstream streaming parameter combinations without editing their fixtures. */
public final class IcebergStreamingScope implements ExecutionCondition {
  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (context.getDisplayName().contains("isStreamingJob=false")
        || context.getDisplayName().contains("isStreaming=false")) {
      return ConditionEvaluationResult.disabled("Iceberg acceleration scope is streaming only");
    }
    return ConditionEvaluationResult.enabled(
        "Preserve upstream streaming parameters and assertions");
  }
}
