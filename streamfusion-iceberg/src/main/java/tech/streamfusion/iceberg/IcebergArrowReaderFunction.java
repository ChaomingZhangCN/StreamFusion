package tech.streamfusion.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.source.reader.ReaderFunction;
import org.apache.iceberg.flink.source.reader.RecordAndPosition;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import tech.streamfusion.operator.ArrowBatch;

/** Preserves Java split ordinals and logical row offsets while fetching Arrow batches. */
final class IcebergArrowReaderFunction implements ReaderFunction<ArrowBatch> {
  private final Schema schema;
  private final List<Integer> projection;
  private final FileIO io;
  private final int batchRows;

  IcebergArrowReaderFunction(Schema schema, List<Integer> projection, FileIO io, int batchRows) {
    this.schema = schema;
    this.projection = List.copyOf(projection);
    this.io = io;
    this.batchRows = batchRows;
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<ArrowBatch>>> apply(
      IcebergSourceSplit split) {
    List<FileScanTask> tasks = List.copyOf(split.task().files());
    if (split.fileOffset() < 0 || split.fileOffset() > tasks.size() || split.recordOffset() < 0) {
      throw new IllegalArgumentException("Invalid restored Iceberg split position");
    }
    return new CloseableIterator<>() {
      private int fileOffset = split.fileOffset();
      private long rowOffset = split.recordOffset();
      private IcebergNativeTaskReader reader;
      private VectorSchemaRoot pending;
      private boolean closed;

      @Override
      public boolean hasNext() {
        if (closed) return false;
        if (pending != null) return true;
        try {
          while (fileOffset < tasks.size()) {
            if (reader == null) {
              reader =
                  new IcebergNativeTaskReader(
                      tasks.get(fileOffset), schema, projection, io, batchRows, rowOffset);
            }
            pending = reader.next();
            if (pending != null) return true;
            reader.close();
            reader = null;
            fileOffset++;
            rowOffset = 0;
          }
          return false;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public RecordsWithSplitIds<RecordAndPosition<ArrowBatch>> next() {
        if (!hasNext()) throw new NoSuchElementException();
        var root = pending;
        pending = null;
        rowOffset = Math.addExact(rowOffset, root.getRowCount());
        return new Batch(
            split.splitId(), new RecordAndPosition<>(new ArrowBatch(root), fileOffset, rowOffset));
      }

      @Override
      public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
          if (pending != null) {
            pending.close();
            pending = null;
          }
        } finally {
          if (reader != null) {
            reader.close();
            reader = null;
          }
        }
      }
    };
  }

  private static final class Batch implements RecordsWithSplitIds<RecordAndPosition<ArrowBatch>> {
    private String split;
    private RecordAndPosition<ArrowBatch> record;

    private Batch(String split, RecordAndPosition<ArrowBatch> record) {
      this.split = split;
      this.record = record;
    }

    @Override
    public String nextSplit() {
      String result = split;
      split = null;
      return result;
    }

    @Override
    public RecordAndPosition<ArrowBatch> nextRecordFromSplit() {
      var result = record;
      record = null;
      return result;
    }

    @Override
    public Set<String> finishedSplits() {
      return Set.of();
    }

    @Override
    public void recycle() {
      if (record != null) {
        record.record().root().close();
        record = null;
      }
    }
  }
}
