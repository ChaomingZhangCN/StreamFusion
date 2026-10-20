package tech.streamfusion.iceberg;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.sink.PartitionKeySelector;

/** Reuses exact host writer keys only for verified bounded bucket selectors. */
final class IcebergDynamicRouting {
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void install(Object operator) throws IllegalAccessException {
    if (!(operator instanceof AbstractUdfStreamOperator<?, ?> udf)) return;
    Object processor = udf.getUserFunction();
    if (!processor
        .getClass()
        .getName()
        .equals("org.apache.iceberg.flink.sink.dynamic.DynamicRecordProcessor")) return;
    Object generator =
        IcebergSinkAdapter.field(processor.getClass(), "hashKeyGenerator").get(processor);
    var field = IcebergSinkAdapter.field(generator.getClass(), "keySelectorCache");
    Map<Object, KeySelector<RowData, Integer>> cache = (Map) field.get(generator);
    field.set(
        generator,
        new AbstractMap<Object, KeySelector<RowData, Integer>>() {
          @Override
          public Set<Entry<Object, KeySelector<RowData, Integer>>> entrySet() {
            return cache.entrySet();
          }

          @Override
          public KeySelector<RowData, Integer> computeIfAbsent(
              Object key,
              Function<? super Object, ? extends KeySelector<RowData, Integer>> create) {
            return cache.computeIfAbsent(key, missing -> memoize(create.apply(missing)));
          }
        });
  }

  static KeySelector<RowData, Integer> memoize(KeySelector<RowData, Integer> selector) {
    if (!selector
        .getClass()
        .getName()
        .equals("org.apache.iceberg.flink.sink.dynamic.HashKeyGenerator$TargetLimitedKeySelector"))
      return selector;
    try {
      Object wrapped = IcebergSinkAdapter.field(selector.getClass(), "wrapped").get(selector);
      if (!wrapped.getClass().isSynthetic()) return selector;
      for (var field : wrapped.getClass().getDeclaredFields()) {
        if (!KeySelector.class.isAssignableFrom(field.getType())) continue;
        field.setAccessible(true);
        Object value = field.get(wrapped);
        if (value != null && value.getClass() == PartitionKeySelector.class)
          return IcebergPartitionGroups.memoize((PartitionKeySelector) value, selector);
      }
      return selector;
    } catch (IllegalAccessException failure) {
      throw new IllegalStateException(
          "Cannot inspect released dynamic partition selector", failure);
    }
  }
}
