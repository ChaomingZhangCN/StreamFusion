package tech.streamfusion.compat;

import static org.apache.flink.util.Preconditions.checkState;

import java.util.List;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;

/** Resolves the deployed keyed output, including when its splitter is chained into a producer. */
public final class ColumnarShuffleOutput {
  private ColumnarShuffleOutput() {}

  public static int parallelism(
      AbstractStreamOperator<?> operator, int maxParallelism, boolean recoverable, boolean singleton) {
    ClassLoader loader = operator.getUserCodeClassloader();
    List<NonChainedOutput> outputs =
        operator.getOperatorConfig().getOperatorNonChainedOutputs(loader);
    checkState(
        outputs.size() == 1 && operator.getOperatorConfig().getChainedOutputs(loader).isEmpty(),
        "Native shuffle splitter requires exactly one non-chained keyed output");
    NonChainedOutput output = outputs.get(0);
    checkState(
        output.getPartitioner() instanceof ColumnarKeyGroupPartitioner
            && output.getPartitionType().mustBePipelinedConsumed(),
        "Native shuffle splitter requires a pipelined columnar key-group partitioner");
    ColumnarKeyGroupPartitioner partitioner = (ColumnarKeyGroupPartitioner) output.getPartitioner();
    checkState(
        partitioner.requiresSingleton() == singleton,
        "Native shuffle splitter and partitioner disagree on SINGLETON distribution");
    checkState(
        partitioner.maxParallelism() == maxParallelism && partitioner.recoverable() == recoverable,
        "Native shuffle splitter and partitioner disagree on key groups or recovery protocol");

    List<NonChainedOutput> taskOutputs =
        operator.getContainingTask().getConfiguration().getVertexNonChainedOutputs(loader);
    // Task writer order follows the vertex outputs, not this chained operator's output list.
    int writerIndex = taskOutputs.indexOf(output);
    checkState(writerIndex >= 0, "Native shuffle output is missing from the task output list");
    checkState(
        writerIndex < operator.getContainingTask().getEnvironment().getAllWriters().length,
        "Native shuffle output writer %s is missing from the task environment",
        writerIndex);
    ResultPartitionWriter writer =
        operator.getContainingTask().getEnvironment().getWriter(writerIndex);
    int targetKeyGroups = writer.getNumTargetKeyGroups();
    int channels = writer.getNumberOfSubpartitions();
    if (singleton) {
      checkState(
          channels == 1,
          "Native SINGLETON shuffle requires exactly one downstream channel, deployed %s",
          channels);
    }
    checkState(
        targetKeyGroups == maxParallelism,
        "Native shuffle max parallelism changed after planning: expected %s key groups, deployed"
            + " %s. Keep max parallelism stable for native keyed state; ordinary parallelism may"
            + " change.",
        maxParallelism,
        targetKeyGroups);
    checkState(
        channels > 0 && channels <= maxParallelism,
        "Native shuffle has invalid runtime channel count %s for %s key groups",
        channels,
        maxParallelism);
    checkState(
        output.getConsumerParallelism() == channels,
        "Native shuffle requires a finalized streaming topology: consumer parallelism %s, channels"
            + " %s",
        output.getConsumerParallelism(),
        channels);
    return channels;
  }
}
