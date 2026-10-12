package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobCancellationException;
import org.junit.jupiter.api.Test;

class NativeFailureParityTest {
  @Test
  void optionalNpeDetailsKeepTheStableFailureContract() {
    for (String message : new String[] {null, "Cannot invoke Boolean.booleanValue()"}) {
      var comparison = new NativeFailureParity.Comparison(
          outcome(new NullPointerException(message), NativeFailureParity.Route.HOST),
          outcome(new NullPointerException(message), NativeFailureParity.Route.NATIVE));
      comparison.assertNullUnboxingFailure(
          NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
    }
  }

  @Test
  void optionalNpeDetailsCannotHideWrongExceptionOrFallback() {
    var host = outcome(new NullPointerException(), NativeFailureParity.Route.HOST);
    for (var nativeRun : List.of(
        outcome(new IllegalStateException(), NativeFailureParity.Route.NATIVE),
        outcome(new NullPointerException(), NativeFailureParity.Route.FALLBACK),
        outcome(null, NativeFailureParity.Route.NATIVE))) {
      assertThrows(AssertionError.class, () -> new NativeFailureParity.Comparison(host, nativeRun)
          .assertNullUnboxingFailure(
              NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE));
    }
  }

  @Test
  void diagnosticMessagesAreCapturedBeforeTheyBecomeUnavailable() {
    var details = new AtomicReference<>("unexpected null dereference");
    var failure = new NullPointerException() {
      @Override
      public String getMessage() {
        return details.get();
      }
    };
    var comparison = new NativeFailureParity.Comparison(
        outcome(failure, NativeFailureParity.Route.HOST),
        outcome(new NullPointerException(), NativeFailureParity.Route.NATIVE));
    details.set(null);
    assertThrows(AssertionError.class, () -> comparison.assertNullUnboxingFailure(
        NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE));
  }

  private static NativeFailureParity.Outcome outcome(Exception failure, NativeFailureParity.Route route) {
    return new NativeFailureParity.Outcome(List.of(), failure, NativeFailureParity.Phase.ROW_EVALUATION,
        route, List.of(), List.of("Calc"), route == NativeFailureParity.Route.NATIVE ? 1 : 0,
        List.of("BOOLEAN"), "");
  }

  @Test
  void terminalOperatorErrorTakesPrecedenceOverACollectorTransportRace() {
    ArithmeticException operatorFailure = new ArithmeticException("/ by zero");
    IOException collectorFailure = new IOException("Task is not running, but in state FAILED");
    CompletableFuture<?> job = CompletableFuture.failedFuture(operatorFailure);

    Exception failure = NativeFailureParity.terminalFailure(job, collectorFailure);

    assertInstanceOf(ExecutionException.class, failure);
    assertSame(operatorFailure, failure.getCause());
  }

  @Test
  void successfulJobDoesNotHideALocalCollectorFailure() {
    IOException collectorFailure = new IOException("invalid collected row");

    Exception failure =
        NativeFailureParity.terminalFailure(
            CompletableFuture.completedFuture(null), collectorFailure);

    assertSame(collectorFailure, failure);
  }

  @Test
  void iteratorCleanupCancellationDoesNotReplaceTheOriginalFailure() {
    IOException collectorFailure = new IOException("invalid collected row");
    var cancelled = new JobCancellationException(new JobID(), "iterator closed", null);

    Exception failure =
        NativeFailureParity.terminalFailure(
            CompletableFuture.failedFuture(cancelled), collectorFailure);

    assertSame(collectorFailure, failure);
  }

  @Test
  void interruptedFailureLookupPreservesTheInterruptAndOriginalFailure() {
    IOException collectorFailure = new IOException("collect interrupted");
    Thread.currentThread().interrupt();
    try {
      Exception failure =
          NativeFailureParity.terminalFailure(new CompletableFuture<>(), collectorFailure);

      assertSame(collectorFailure, failure);
      assertTrue(Thread.currentThread().isInterrupted());
      assertInstanceOf(InterruptedException.class, failure.getSuppressed()[0]);
    } finally {
      Thread.interrupted();
    }
  }
}
