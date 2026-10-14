package tech.streamfusion.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;

/** Original Java UTF-16 caches beside logical UTF-8 columns; null lists mean binary-only values. */
public final class StringCacheColumns {
  private StringCacheColumns() {}

  public static final String METADATA_KEY = "streamfusion.java_string_cache";

  public static ListVector[] readers(org.apache.arrow.vector.VectorSchemaRoot root, int fields) {
    ListVector[] caches = null;
    for (FieldVector vector : root.getFieldVectors()) {
      var metadata = vector.getField().getMetadata();
      if (metadata != null && "v1".equals(metadata.get(METADATA_KEY))) {
        String name = vector.getName();
        int field = Integer.parseInt(name.substring("$string_java_".length(), name.length() - 1));
        if (field < 0 || field >= fields)
          throw new IllegalArgumentException("Invalid Java string-cache field index");
        if (caches == null) caches = new ListVector[fields];
        caches[field] = (ListVector) vector;
      }
    }
    return caches;
  }

  public static String name(int field) {
    return "$string_java_" + field + "$";
  }

  public static final class Writer implements AutoCloseable {
    private final List<Integer> fields = new ArrayList<>();
    private final List<ListVector> vectors = new ArrayList<>();

    public Writer(RowType type, BufferAllocator allocator) {
      try {
        for (int field = 0; field < type.getFieldCount(); field++) {
          switch (type.getTypeAt(field).getTypeRoot()) {
            case CHAR:
            case VARCHAR:
              ListVector vector =
                  new ListVector(
                      name(field),
                      allocator,
                      new FieldType(
                          true,
                          ArrowType.List.INSTANCE,
                          null,
                          java.util.Map.of(METADATA_KEY, "v1")),
                      null);
              vectors.add(vector);
              fields.add(field);
              vector.initializeChildrenFromFields(
                  List.of(
                      new Field(
                          "item", FieldType.notNullable(new ArrowType.Int(16, false)), null)));
              vector.allocateNew();
              break;
            default:
              break;
          }
        }
      } catch (RuntimeException | Error failure) {
        close();
        throw failure;
      }
    }

    public void write(RowData row, int position) {
      for (int slot = 0; slot < fields.size(); slot++) {
        int field = fields.get(slot);
        ListVector vector = vectors.get(slot);
        String cache =
            row.isNullAt(field) ? null : ((BinaryStringData) row.getString(field)).getJavaObject();
        if (cache == null) {
          vector.setNull(position);
        } else {
          int start = vector.startNewValue(position);
          UInt2Vector units = (UInt2Vector) vector.getDataVector();
          for (int unit = 0; unit < cache.length(); unit++)
            units.setSafe(start + unit, cache.charAt(unit));
          vector.endValue(position, cache.length());
        }
      }
    }

    public List<FieldVector> finish(int rows) {
      for (ListVector vector : vectors) vector.setValueCount(rows);
      return new ArrayList<>(vectors);
    }

    @Override
    public void close() {
      for (ListVector vector : vectors) vector.close();
      vectors.clear();
    }
  }

  static BinaryStringData read(ListVector cache, int row, BinaryStringData binary) {
    if (cache == null || cache.isNull(row)) return null;
    int start = cache.getElementStartIndex(row);
    int end = cache.getElementEndIndex(row);
    UInt2Vector units = (UInt2Vector) cache.getDataVector();
    char[] text = new char[end - start];
    for (int unit = start; unit < end; unit++) text[unit - start] = (char) units.get(unit);
    return new BinaryStringData(
        binary.getSegments(), binary.getOffset(), binary.getSizeInBytes(), new String(text));
  }
}
