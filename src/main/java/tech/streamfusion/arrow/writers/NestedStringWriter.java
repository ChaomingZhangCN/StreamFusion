package tech.streamfusion.arrow.writers;

import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.table.data.binary.BinaryArrayData;

import java.util.function.BiFunction;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;

/** Preserves caches before UTF-8 encoding can materialize a lazy StringData. */
public final class NestedStringWriter<T> extends ArrowFieldWriter<T> {
  private final BiFunction<T, Integer, StringData> getter;
  private final ListVector cache;
  private final ArrowFieldWriter<RowData> utf8;
  private final GenericRowData row = new GenericRowData(1);
  private final TinyIntVector copyClass;

  private NestedStringWriter(StructVector vector, BiFunction<T, Integer, StringData> getter) {
    super(vector);
    this.getter = getter;
    cache = (ListVector) vector.getChild("java");
    utf8 = VarCharWriter.forRow((VarCharVector) vector.getChild("utf8"));
    copyClass = (TinyIntVector) vector.getChild("array_copy_class");
  }

  public static NestedStringWriter<RowData> forRow(StructVector vector) {
    return new NestedStringWriter<>(
        vector, (row, pos) -> row.isNullAt(pos) ? null : row.getString(pos));
  }

  public static NestedStringWriter<ArrayData> forArray(StructVector vector) {
    return new NestedStringWriter<>(
        vector, (row, pos) -> row.isNullAt(pos) ? null : row.getString(pos));
  }

  @Override
  public void doWrite(T input, int ordinal) {
    StringData value = getter.apply(input, ordinal);
    if (copyClass != null)
      copyClass.setSafe(
          getCount(), input instanceof BinaryArrayData ? 1 : 0);
    var parent = (StructVector) getValueVector();
    if (value == null) parent.setNull(getCount());
    else parent.setIndexDefined(getCount());
    String java =
        value instanceof BinaryStringData ? ((BinaryStringData) value).getJavaObject() : null;
    if (java == null) cache.setNull(getCount());
    else {
      int start = cache.startNewValue(getCount());
      var units = (UInt2Vector) cache.getDataVector();
      for (int i = 0; i < java.length(); i++) units.setSafe(start + i, java.charAt(i));
      cache.endValue(getCount(), java.length());
    }
    row.setField(0, value);
    utf8.write(row, 0);
    row.setField(0, null);
  }

  @Override
  public void finish() {
    utf8.finish();
    cache.setValueCount(getCount());
    if (copyClass != null) copyClass.setValueCount(getCount());
    super.finish();
  }

  @Override
  public void reset() {
    super.reset();
    utf8.reset();
  }
}
