package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.ColumnarShuffleOutput;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;

class ColumnarShuffleOutputTest {
  @Test
  void locatesAChainedOperatorsOutputAmongOtherTaskOutputs() throws Exception {
    var operator = new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      var unrelated = ColumnarShuffleTestOutput.output(1, 1, new RebalancePartitioner<>());
      var keyed = ColumnarShuffleTestOutput.output(5, 128, new ColumnarKeyGroupPartitioner(128));
      ColumnarShuffleTestOutput.configure(
          harness,
          List.of(keyed),
          List.of(unrelated, keyed),
          List.of(
              ColumnarShuffleTestOutput.writer(1, 1), ColumnarShuffleTestOutput.writer(5, 128)));
      harness.setup(new ArrowBatchSerializer());
      assertEquals(1, harness.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks());
      assertEquals(5, ColumnarShuffleOutput.parallelism(operator, 128, false, false));
    }
  }

  @Test
  void rejectsMissingOutputInsteadOfGuessingFromProducerParallelism() throws Exception {
    var operator = new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      harness.setup(new ArrowBatchSerializer());
      var error =
          assertThrows(
              IllegalStateException.class,
              () -> ColumnarShuffleOutput.parallelism(operator, 128, false, false));
      assertTrue(error.getMessage().contains("exactly one"));
    }
  }

  @Test
  void rejectsAmbiguousOutputs() throws Exception {
    var operator = new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      var first = ColumnarShuffleTestOutput.output(4, 128, new ColumnarKeyGroupPartitioner(128));
      var second = ColumnarShuffleTestOutput.output(4, 128, new ColumnarKeyGroupPartitioner(128));
      ColumnarShuffleTestOutput.configure(
          harness,
          List.of(first, second),
          List.of(first, second),
          List.of(
              ColumnarShuffleTestOutput.writer(4, 128), ColumnarShuffleTestOutput.writer(4, 128)));
      harness.setup(new ArrowBatchSerializer());
      var error =
          assertThrows(
              IllegalStateException.class,
              () -> ColumnarShuffleOutput.parallelism(operator, 128, false, false));
      assertTrue(error.getMessage().contains("exactly one"));
    }
  }

  @Test
  void rejectsAnOutputMissingFromTheContainingTask() throws Exception {
    var operator = new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      var output = ColumnarShuffleTestOutput.output(4, 128, new ColumnarKeyGroupPartitioner(128));
      ColumnarShuffleTestOutput.configure(harness, List.of(output), List.of(), List.of());
      harness.setup(new ArrowBatchSerializer());
      var error =
          assertThrows(
              IllegalStateException.class,
              () -> ColumnarShuffleOutput.parallelism(operator, 128, false, false));
      assertTrue(error.getMessage().contains("missing from the task output list"));
    }
  }

  @Test
  void rejectsAMissingNetworkWriter() throws Exception {
    var operator = new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      var output = ColumnarShuffleTestOutput.output(4, 128, new ColumnarKeyGroupPartitioner(128));
      ColumnarShuffleTestOutput.configure(harness, List.of(output), List.of(output), List.of());
      harness.setup(new ArrowBatchSerializer());
      var error =
          assertThrows(
              IllegalStateException.class,
              () -> ColumnarShuffleOutput.parallelism(operator, 128, false, false));
      assertTrue(error.getMessage().contains("writer 0 is missing"));
    }
  }

  @Test
  void rejectsAReplacedKeyedPartitioner() throws Exception {
    var operator = new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      var output = ColumnarShuffleTestOutput.output(4, 128, new RebalancePartitioner<>());
      ColumnarShuffleTestOutput.configure(
          harness,
          List.of(output),
          List.of(output),
          List.of(ColumnarShuffleTestOutput.writer(4, 128)));
      harness.setup(new ArrowBatchSerializer());
      var error =
          assertThrows(
              IllegalStateException.class,
              () -> ColumnarShuffleOutput.parallelism(operator, 128, false, false));
      assertTrue(error.getMessage().contains("key-group partitioner"));
    }
  }

  @Test
  void rejectsChangedMaxParallelismBeforeSplitting() throws Exception {
    var operator = new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      var output = ColumnarShuffleTestOutput.output(4, 256, new ColumnarKeyGroupPartitioner(128));
      ColumnarShuffleTestOutput.configure(
          harness,
          List.of(output),
          List.of(output),
          List.of(ColumnarShuffleTestOutput.writer(4, 256)));
      harness.setup(new ArrowBatchSerializer());
      var error =
          assertThrows(
              IllegalStateException.class,
              () -> ColumnarShuffleOutput.parallelism(operator, 128, false, false));
      assertTrue(error.getMessage().contains("expected 128 key groups, deployed 256"));
    }
  }

  @Test
  void rejectsMismatchedRecoveryProtocols() throws Exception {
    var operator = new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      ColumnarShuffleTestOutput.configure(harness, 4, 128, true);
      harness.setup(new ArrowBatchSerializer());
      var error =
          assertThrows(
              IllegalStateException.class,
              () -> ColumnarShuffleOutput.parallelism(operator, 128, false, false));
      assertTrue(error.getMessage().contains("recovery protocol"));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 129})
  void rejectsInvalidRuntimeChannelCounts(int channels) throws Exception {
    var operator = new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      ColumnarShuffleTestOutput.configure(harness, channels, 128, false);
      harness.setup(new ArrowBatchSerializer());
      assertThrows(
          IllegalStateException.class,
          () -> ColumnarShuffleOutput.parallelism(operator, 128, false, false));
    }
  }
}
