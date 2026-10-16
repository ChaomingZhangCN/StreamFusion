package tech.streamfusion.iceberg;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ColumnarRecordMetrics;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Java chooses the host key groups; Arrow selections travel through the existing batch exchange.
 */
final class IcebergHashRouter extends tech.streamfusion.compat.FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private final RowType rowType;
  private final KeySelector<RowData, ?> selector;
  private final int writers;
  private final int maxParallelism;
  private final boolean recoverable;

  IcebergHashRouter(
      RowType rowType,
      KeySelector<RowData, ?> selector,
      int writers,
      int maxParallelism,
      boolean recoverable) {
    this.rowType = rowType;
    this.selector = selector;
    this.writers = writers;
    this.maxParallelism = maxParallelism;
    this.recoverable = recoverable;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    long route;
    try (VectorSchemaRoot root = element.getValue().root()) {
      int[] destinations = new int[root.getRowCount()];
      var reader = ArrowConversion.createArrowReader(root, rowType);
      for (int row = 0; row < root.getRowCount(); row++) {
        int group =
            KeyGroupRangeAssignment.assignToKeyGroup(
                selector.getKey(reader.read(row)), maxParallelism);
        if (recoverable) destinations[row] = group;
        else {
          int writer =
              KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                  maxParallelism, writers, group);
          destinations[row] =
              KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                      maxParallelism, writers, writer)
                  .getStartKeyGroup();
        }
      }
      var allocator =
          root.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : root.getVector(0).getAllocator();
      try (var array = ArrowArray.allocateNew(allocator);
          var schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        try {
          route = NativeIceberg.route(array.memoryAddress(), schema.memoryAddress(), destinations);
        } finally {
          if (array.snapshot().release != 0) array.release();
          if (schema.snapshot().release != 0) schema.release();
        }
      }
    }
    try {
      while (true) {
        try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
            var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          int group = NativeIceberg.nextRoute(route, array.memoryAddress(), schema.memoryAddress());
          if (group < 0) break;
          var root =
              Data.importVectorSchemaRoot(
                  NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES);
          output.collect(element.copy(new ArrowBatch(root, group)));
        }
      }
    } finally {
      NativeIceberg.closeRoute(route);
    }
  }
}
