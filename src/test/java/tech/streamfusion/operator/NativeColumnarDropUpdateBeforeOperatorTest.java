package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.misc.DropUpdateBeforeFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class NativeColumnarDropUpdateBeforeOperatorTest {
  private static final RowType TYPE = RowType.of(new BigIntType());

  @Test
  void filtersEveryKindLikeTheReleasedHostAndKeepsRecordTimestamp() throws Exception {
    try (var allocator = new RootAllocator();
        var harness = harness()) {
      List<RowData> rows =
          List.of(
              row(RowKind.INSERT, 10L),
              row(RowKind.UPDATE_BEFORE, 10L),
              row(RowKind.UPDATE_AFTER, 20L),
              row(RowKind.DELETE, 20L),
              row(RowKind.UPDATE_BEFORE, null),
              row(RowKind.INSERT, null));
      var in = RowDataArrowConverter.write(rows, TYPE, allocator, true);
      harness.processElement(new StreamRecord<>(new ArrowBatch(in), 123));
      var record = (StreamRecord<?>) harness.getOutput().poll();
      assertEquals(123, record.getTimestamp());
      try (VectorSchemaRoot result = ((ArrowBatch) record.getValue()).root()) {
        var expected = new ArrayList<String>();
        var host = new DropUpdateBeforeFunction();
        for (RowData value : rows) if (host.filter(value)) expected.add(text(value));
        assertEquals(
            expected,
            RowDataArrowConverter.read(result, TYPE).stream()
                .map(NativeColumnarDropUpdateBeforeOperatorTest::text)
                .collect(java.util.stream.Collectors.toList()));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void retainedBatchIsForwardedWithoutSpendingASharedConsumer() throws Exception {
    try (var allocator = new RootAllocator();
        var harness = harness()) {
      var in =
          RowDataArrowConverter.write(
              List.of(row(RowKind.UPDATE_AFTER, 4L)), TYPE, allocator, true);
      ArrowBatch batch = new ArrowBatch(in);
      batch.shareAcross(2);
      var record = new StreamRecord<>(batch, 17);
      harness.processElement(record);
      assertSame(batch, ((StreamRecord<?>) harness.getOutput().poll()).getValue());
      try (VectorSchemaRoot first = batch.root()) {
        assertNotSame(in, first);
        assertEquals(4L, RowDataArrowConverter.read(first, TYPE).get(0).getLong(0));
      }
      try (VectorSchemaRoot last = batch.root()) {
        assertSame(in, last);
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void emptyAndFullyRemovedBatchesDoNotSuppressWatermarks() throws Exception {
    try (var allocator = new RootAllocator();
        var harness = harness()) {
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(row(RowKind.UPDATE_BEFORE, 7L)), TYPE, allocator, true))));
      assertTrue(harness.getOutput().isEmpty());
      assertEquals(0, allocator.getAllocatedMemory());
      harness.processWatermark(new Watermark(99));
      assertEquals(new Watermark(99), harness.getOutput().poll());
      var empty = VectorSchemaRoot.of();
      empty.setRowCount(0);
      harness.processElement(new StreamRecord<>(new ArrowBatch(empty)));
      try (var result =
          ((ArrowBatch) ((StreamRecord<?>) harness.getOutput().poll()).getValue()).root()) {
        assertEquals(0, result.getRowCount());
      }
      harness.processWatermark(Watermark.MAX_WATERMARK);
      assertEquals(Watermark.MAX_WATERMARK, harness.getOutput().poll());
    }
  }

  @Test
  void filteredBatchRetainsKeyGroupsAndPerRowLineage() throws Exception {
    try (var allocator = new RootAllocator();
        var harness = harness()) {
      List<RowData> rows =
          List.of(row(RowKind.INSERT, 1L), row(RowKind.UPDATE_BEFORE, 2L), row(RowKind.DELETE, 3L));
      var batch =
          new ArrowBatch(
              RowDataArrowConverter.write(rows, TYPE, allocator, true),
              7,
              ArrowBatch.NO_HANDLE_OWNER,
              null,
              11,
              12,
              13,
              new int[] {10, 20, 30},
              new int[] {6, 7, 8});
      harness.processElement(new StreamRecord<>(batch));
      var out = (ArrowBatch) ((StreamRecord<?>) harness.getOutput().poll()).getValue();
      assertEquals(7, out.keyGroup());
      assertEquals(11, out.parentEpochHigh());
      assertEquals(12, out.parentEpochLow());
      assertEquals(13, out.parentSequence());
      assertArrayEquals(new int[] {10, 30}, out.rowOrdinals());
      assertArrayEquals(new int[] {6, 8}, out.parentKeyGroups());
      out.root().close();
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void filteringASharedBatchLeavesTheOtherConsumersInputIntact() throws Exception {
    try (var allocator = new RootAllocator();
        var harness = harness()) {
      var root =
          RowDataArrowConverter.write(
              List.of(row(RowKind.UPDATE_BEFORE, 1L), row(RowKind.UPDATE_AFTER, 2L)),
              TYPE,
              allocator,
              true);
      var batch = new ArrowBatch(root);
      batch.shareAcross(2);
      harness.processElement(new StreamRecord<>(batch));
      try (var out =
          ((ArrowBatch) ((StreamRecord<?>) harness.getOutput().poll()).getValue()).root()) {
        assertEquals(1, out.getRowCount());
        assertEquals(2L, RowDataArrowConverter.read(out, TYPE).get(0).getLong(0));
      }
      try (var other = batch.root()) {
        assertEquals(2, other.getRowCount());
        assertEquals(
            RowKind.UPDATE_BEFORE, RowDataArrowConverter.read(other, TYPE).get(0).getRowKind());
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void absentKindForwardsZeroColumnBatchWithItsLogicalRows() throws Exception {
    try (var harness = harness()) {
      var root = VectorSchemaRoot.of();
      root.setRowCount(3);
      ArrowBatch batch = new ArrowBatch(root);
      harness.processElement(new StreamRecord<>(batch));
      assertSame(batch, ((StreamRecord<?>) harness.getOutput().poll()).getValue());
      assertEquals(3, batch.rowCount());
      batch.root().close();
    }
  }

  private static OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness()
      throws Exception {
    var harness =
        new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
            new NativeColumnarDropUpdateBeforeOperator(), new ArrowBatchSerializer());
    harness.setup(new ArrowBatchSerializer());
    harness.open();
    return harness;
  }

  private static GenericRowData row(RowKind kind, Long value) {
    GenericRowData row = GenericRowData.of(value);
    row.setRowKind(kind);
    return row;
  }

  private static String text(RowData row) {
    return row.getRowKind().shortString() + ":" + (row.isNullAt(0) ? "null" : row.getLong(0));
  }
}
