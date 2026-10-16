package tech.streamfusion.operator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.shaded.netty4.io.netty.util.collection.LongObjectHashMap;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;

/** Bounded accumulator views, flushed into managed state before checkpoint snapshots. */
final class JvmAccumulatorCache {
  private final int capacity;
  private final boolean intKey;
  private final Supplier<Object> currentKey;
  private final Counter hits;
  private final Consumer<Object> selectKey;
  private final List<CachedState> states = new ArrayList<>();

  JvmAccumulatorCache(
      int capacity,
      boolean intKey,
      Supplier<Object> currentKey,
      Consumer<Object> selectKey,
      Counter hits) {
    this.capacity = capacity;
    this.intKey = intKey;
    this.currentKey = currentKey;
    this.hits = hits;
    this.selectKey = selectKey;
  }

  RuntimeContext wrap(RuntimeContext delegate) {
    return (RuntimeContext)
        Proxy.newProxyInstance(
            RuntimeContext.class.getClassLoader(),
            new Class<?>[] {RuntimeContext.class},
            (proxy, method, args) -> {
              if (method.getDeclaringClass() == Object.class) {
                switch (method.getName()) {
                  case "equals":
                    return proxy == args[0];
                  case "hashCode":
                    return System.identityHashCode(proxy);
                  default:
                    return "AccumulatorContext[" + delegate + "]";
                }
              }
              Object value;
              try {
                value = method.invoke(delegate, args);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }
              // This is the released grouped runtime's accumulator state. DataViews remain direct.
              if (method.getName().equals("getState")
                  && args[0] instanceof ValueStateDescriptor
                  && ((ValueStateDescriptor<?>) args[0]).getName().equals("accState")) {
                @SuppressWarnings("unchecked")
                var state = new CachedState((ValueState<RowData>) value);
                states.add(state);
                return state;
              }
              return value;
            });
  }

  RowData keyFor(RowData input, int field, RowDataKeySelector selector) throws Exception {
    if (states.size() == 1) {
      RowData key = states.get(0).keyFor(input, field);
      if (key != null) return key;
    }
    return selector.getKey(input);
  }

  void flush() throws IOException {
    for (CachedState state : states) state.flush();
  }

  void invalidate() {
    states.forEach(CachedState::invalidate);
  }

  private static final class Entry {
    private final RowData key;
    private RowData value;
    private boolean dirty;

    private Entry(RowData key, RowData value) {
      this.key = key;
      this.value = value;
    }
  }

  private final class CachedState implements ValueState<RowData> {
    private final ValueState<RowData> delegate;
    private final LongObjectHashMap<Entry> values = new LongObjectHashMap<>();
    private Entry nullEntry;
    private RowData lastKey;
    private Entry lastEntry;

    private CachedState(ValueState<RowData> delegate) {
      this.delegate = delegate;
    }

    private RowData keyFor(RowData input, int field) {
      Entry entry =
          input.isNullAt(field)
              ? nullEntry
              : values.get(intKey ? input.getInt(field) : input.getLong(field));
      return entry == null ? null : entry.key;
    }

    private long keyValue(RowData key) {
      return intKey ? key.getInt(0) : key.getLong(0);
    }

    private void makeRoom() throws IOException {
      if (values.size() + (nullEntry == null ? 0 : 1) >= capacity) {
        flush();
        invalidate();
      }
    }

    private void flush() throws IOException {
      if (values.isEmpty() && nullEntry == null) return;
      Object original = currentKey.get();
      try {
        for (Entry entry : values.values()) flush(entry);
        if (nullEntry != null) flush(nullEntry);
      } finally {
        if (original != null) selectKey.accept(original);
      }
    }

    private void flush(Entry entry) throws IOException {
      if (!entry.dirty) return;
      selectKey.accept(entry.key);
      delegate.update(entry.value);
      entry.dirty = false;
    }

    private void put(RowData key, Entry entry) {
      if (key.isNullAt(0)) nullEntry = entry;
      else values.put(keyValue(key), entry);
    }

    @Override
    public RowData value() throws IOException {
      RowData key = (RowData) currentKey.get();
      Entry entry = key.isNullAt(0) ? nullEntry : values.get(keyValue(key));
      if (entry == null) {
        makeRoom();
        entry = new Entry(key, delegate.value());
        put(key, entry);
      } else hits.inc();
      lastKey = key;
      lastEntry = entry;
      return entry.value;
    }

    @Override
    public void update(RowData value) throws IOException {
      RowData key = (RowData) currentKey.get();
      if (key == lastKey && lastEntry != null) lastEntry.value = value;
      else {
        makeRoom();
        lastKey = key;
        lastEntry = new Entry(key, value);
        put(key, lastEntry);
      }
      lastEntry.dirty = true;
    }

    @Override
    public void clear() {
      delegate.clear();
      RowData key = (RowData) currentKey.get();
      if (key.isNullAt(0)) nullEntry = null;
      else values.remove(keyValue(key));
      lastKey = null;
      lastEntry = null;
    }

    private void invalidate() {
      values.clear();
      nullEntry = null;
      lastKey = null;
      lastEntry = null;
    }
  }
}
