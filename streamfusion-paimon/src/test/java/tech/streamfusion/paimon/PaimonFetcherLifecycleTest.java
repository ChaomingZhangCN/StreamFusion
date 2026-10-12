package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.NativeSourceRecord;

class PaimonFetcherLifecycleTest {
  @Test
  void retirementBetweenMapCheckAndIterationDoesNotFailNewAssignments() throws Exception {
    var stock = stockManager();
    var nativeManager = new ProbeFetcher();
    try {
      var fetcher = createFetcher(nativeManager);
      var retired = new RetiringMap();
      retired.put(0, fetcher);
      installMap(stock, retired);
      var method = SingleThreadFetcherManager.class.getDeclaredMethod("getRunningFetcher");
      method.setAccessible(true);
      var failure = assertThrows(InvocationTargetException.class, () -> method.invoke(stock));
      assertInstanceOf(NoSuchElementException.class, failure.getCause());

      retired.put(0, fetcher);
      installMap(nativeManager, retired);
      assertNull(nativeManager.current());
      assertTrue(retired.isEmpty());
    } finally {
      stock.close(1000);
      nativeManager.close(1000);
    }
  }

  @Test
  void iteratorRetainsAnActiveFetcherAndReturnsNullAfterRetirement() throws Exception {
    var manager = new ProbeFetcher();
    try {
      var active = new ConcurrentHashMap<Integer, SplitFetcher<NativeSourceRecord, FileStoreSourceSplit>>();
      var fetcher = createFetcher(manager);
      active.put(7, fetcher);
      installMap(manager, active);
      assertSame(fetcher, manager.current());
      active.clear();
      assertNull(manager.current());
    } finally {
      manager.close(1000);
    }
  }

  // Force the I/O-thread retirement at the actual collection boundary, without timing sleeps.
  private static final class RetiringMap
      extends ConcurrentHashMap<Integer, SplitFetcher<NativeSourceRecord, FileStoreSourceSplit>> {
    @Override
    public Collection<SplitFetcher<NativeSourceRecord, FileStoreSourceSplit>> values() {
      clear();
      return super.values();
    }
  }

  private static void installMap(
      SplitFetcherManager<?, ?> manager,
      Map<Integer, SplitFetcher<NativeSourceRecord, FileStoreSourceSplit>> fetchers)
      throws Exception {
    var field = SplitFetcherManager.class.getDeclaredField("fetchers");
    field.setAccessible(true);
    field.set(manager, fetchers);
  }

  private static SplitFetcherManager<?, ?> stockManager() throws Exception {
    Supplier<SplitReader<NativeSourceRecord, FileStoreSourceSplit>> supplier =
        StubReader::new;
    try {
      return SingleThreadFetcherManager.class
          .getConstructor(Supplier.class, Configuration.class)
          .newInstance(supplier, new Configuration());
    } catch (NoSuchMethodException legacy) {
      return SingleThreadFetcherManager.class
          .getConstructor(FutureCompletingBlockingQueue.class, Supplier.class, Configuration.class)
          .newInstance(
              new FutureCompletingBlockingQueue<RecordsWithSplitIds<NativeSourceRecord>>(),
              supplier,
              new Configuration());
    }
  }

  @SuppressWarnings("unchecked")
  private static SplitFetcher<NativeSourceRecord, FileStoreSourceSplit> createFetcher(
      SplitFetcherManager<?, ?> manager) throws Exception {
    var method = SplitFetcherManager.class.getDeclaredMethod("createSplitFetcher");
    method.setAccessible(true);
    return (SplitFetcher<NativeSourceRecord, FileStoreSourceSplit>) method.invoke(manager);
  }

  private static final class StubReader
      implements SplitReader<NativeSourceRecord, FileStoreSourceSplit> {
    @Override
    public RecordsWithSplitIds<NativeSourceRecord> fetch() {
      throw new UnsupportedOperationException("The fixture never starts an I/O thread");
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FileStoreSourceSplit> changes) {}

    @Override
    public void wakeUp() {}

    @Override
    public void close() {}
  }

  private static final class ProbeFetcher extends PaimonSourceReaderCompat.Fetcher {
    ProbeFetcher() {
      super(StubReader::new, new Configuration());
    }

    SplitFetcher<NativeSourceRecord, FileStoreSourceSplit> current() {
      return getRunningFetcher();
    }
  }
}
