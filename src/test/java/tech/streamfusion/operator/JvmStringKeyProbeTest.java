package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.junit.jupiter.api.Test;

class JvmStringKeyProbeTest {
  @Test
  void slicedNullEmptyAndUnicodeKeysUseReleasedStringEqualityAndHashing() {
    try (var allocator = new RootAllocator();
        var input = new VarCharVector("k", allocator);
        var slice = new VarCharVector("slice", allocator)) {
      String[] values = {"prefix", "", null, "é🙂\u0000".repeat(257), "last"};
      for (int i = 0; i < values.length; i++) {
        if (values[i] == null) input.setNull(i);
        else input.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
      }
      input.setValueCount(values.length);
      input.makeTransferPair(slice).splitAndTransfer(1, 4);
      var probe = new JvmAccumulatorCache.StringProbe(slice);
      for (int i = 0; i < 4; i++) {
        var expected = BinaryStringData.fromString(values[i + 1]);
        var actual = probe.value(i);
        assertEquals(expected, actual);
        if (actual != null) assertEquals(expected.hashCode(), actual.hashCode());
      }
      assertEquals(BinaryStringData.fromString(values[3]), probe.value(2));
    }
  }

  @Test
  void ownedKeysSurviveProbeMovementAndClosingTheBorrowedBatch() {
    var retained = new ArrayList<BinaryStringData>();
    try (var allocator = new RootAllocator();
        var input = new VarCharVector("k", allocator)) {
      String[] values = {"é🙂first", "last"};
      for (int i = 0; i < values.length; i++)
        input.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
      input.setValueCount(values.length);
      var probe = new JvmAccumulatorCache.StringProbe(input);
      retained.add(((BinaryStringData) probe.value(0)).copy());
      retained.add(((BinaryStringData) probe.value(1)).copy());
      assertEquals(BinaryStringData.fromString(values[0]), retained.get(0));
      input.setSafe(0, "changed!".getBytes(StandardCharsets.UTF_8));
      assertEquals(BinaryStringData.fromString(values[0]), retained.get(0));
    }
    assertEquals(BinaryStringData.fromString("é🙂first"), retained.get(0));
    assertEquals(BinaryStringData.fromString("last"), retained.get(1));
  }

  @Test
  void emptyDataBufferStillDistinguishesNullAndEmpty() {
    try (var allocator = new RootAllocator();
        var input = new VarCharVector("k", allocator)) {
      input.allocateNew(0, 3);
      input.setSafe(0, new byte[0]);
      input.setNull(1);
      input.setSafe(2, new byte[0]);
      input.setValueCount(3);
      assertEquals(0, input.getDataBuffer().capacity());
      var probe = new JvmAccumulatorCache.StringProbe(input);
      assertEquals(BinaryStringData.EMPTY_UTF8, probe.value(0));
      assertNull(probe.value(1));
      assertEquals(BinaryStringData.EMPTY_UTF8, probe.value(2));
    }
  }
}
