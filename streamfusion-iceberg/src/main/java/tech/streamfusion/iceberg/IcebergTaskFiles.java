package tech.streamfusion.iceberg;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SeekableInputStream;

/** Task-local positional reads retain the host's filesystem and credential configuration. */
final class IcebergTaskFiles implements AutoCloseable {
  private final FileIO io;
  private final Map<String, Long> lengths;
  private final Map<String, SeekableInputStream> streams = new HashMap<>();
  private final byte[] transfer = new byte[64 * 1024];
  private boolean closed;

  IcebergTaskFiles(FileIO io, Map<String, Long> lengths) {
    this.io = io;
    this.lengths = Map.copyOf(lengths);
  }

  public synchronized long length(String path) throws IOException {
    if (closed) throw new IOException("Iceberg task input is closed");
    Long length = lengths.get(path);
    if (length == null) throw new IOException("File is outside the Java-planned Iceberg task");
    return length;
  }

  /** The direct buffer is borrowed for this synchronous callback only. */
  public synchronized void readFully(String path, long position, ByteBuffer target)
      throws IOException {
    long length = length(path);
    if (position < 0 || position > length || target.remaining() > length - position) {
      throw new EOFException("Iceberg read exceeds the planned file size");
    }
    var stream = streams.get(path);
    if (stream == null) {
      stream = io.newInputFile(path, length).newStream();
      streams.put(path, stream);
    }
    stream.seek(position);
    while (target.hasRemaining()) {
      int count = stream.read(transfer, 0, Math.min(transfer.length, target.remaining()));
      if (count < 0) throw new EOFException("Truncated Iceberg file");
      if (count == 0) throw new IOException("Iceberg input made no progress");
      target.put(transfer, 0, count);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) return;
    closed = true;
    IOException failure = null;
    for (var stream : streams.values()) {
      try {
        stream.close();
      } catch (IOException e) {
        if (failure == null) failure = e;
        else failure.addSuppressed(e);
      }
    }
    streams.clear();
    if (failure != null) throw failure;
  }
}
