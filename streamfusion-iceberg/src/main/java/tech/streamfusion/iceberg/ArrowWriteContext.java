package tech.streamfusion.iceberg;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;

/** Batch-scoped selections must drain before the task writer releases borrowed buffers. */
final class ArrowWriteContext {
  interface PendingWrite {
    void drain() throws IOException;

    default void abort() throws IOException {}
  }

  private final Set<PendingWrite> pending = new LinkedHashSet<>();
  private VectorSchemaRoot root;
  private RowData row;
  private int position;

  void begin(VectorSchemaRoot root) {
    if (this.root != null) throw new IllegalStateException("Nested Iceberg Arrow write");
    this.root = root;
  }

  void position(int position, RowData row) {
    this.position = position;
    this.row = row;
  }

  VectorSchemaRoot root() {
    if (root == null) throw new IllegalStateException("Iceberg appender outside Arrow write");
    return root;
  }

  int position(RowData accepted) {
    if (row != accepted || root == null) {
      throw new IllegalArgumentException("Iceberg data appender requires the current Arrow view");
    }
    return position;
  }

  void pending(PendingWrite write) {
    pending.add(write);
  }

  void drain() throws IOException {
    for (PendingWrite write : pending) write.drain();
    pending.clear();
  }

  void clear() {
    pending.clear();
    root = null;
    row = null;
  }

  void fail(Throwable failure) {
    for (PendingWrite write : pending) {
      try {
        write.abort();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
    }
  }
}
