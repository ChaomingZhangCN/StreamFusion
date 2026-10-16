package tech.streamfusion.iceberg;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Cancels host IO without racing the fetcher's native reader ownership. */
final class IcebergReadCancellation implements AutoCloseable {
  private final Set<IcebergTaskFiles> active = new HashSet<>();
  private boolean closed;

  void add(IcebergTaskFiles files) throws IOException {
    synchronized (this) {
      if (!closed) {
        active.add(files);
        return;
      }
    }
    files.close();
    throw new IOException("Iceberg source reader is closed");
  }

  synchronized void remove(IcebergTaskFiles files) {
    active.remove(files);
  }

  @Override
  public void close() throws IOException {
    List<IcebergTaskFiles> files;
    synchronized (this) {
      if (closed) return;
      closed = true;
      files = List.copyOf(active);
      active.clear();
    }
    IOException failure = null;
    for (var input : files) {
      try {
        input.close();
      } catch (IOException e) {
        if (failure == null) failure = e;
        else failure.addSuppressed(e);
      }
    }
    if (failure != null) throw failure;
  }
}
