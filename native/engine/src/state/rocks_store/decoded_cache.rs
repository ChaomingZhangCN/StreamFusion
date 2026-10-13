use super::*;
use std::collections::VecDeque;

struct Entry<V> {
    value: Option<V>,
    bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Codec(Arc<AtomicUsize>);

    impl RocksStateCodec for Codec {
        type Value = i64;
        fn supported(&self) -> bool {
            true
        }
        fn value_fields(&self) -> Vec<(String, DataType)> {
            vec![("value".into(), DataType::Int64)]
        }
        fn value_bytes(&self, _: &i64) -> usize {
            8
        }
        fn raw(&self) -> bool {
            true
        }
        fn raw_write(&self, value: &i64, output: &mut Vec<u8>) {
            self.0.fetch_add(1, Ordering::Relaxed);
            output.extend_from_slice(&value.to_le_bytes());
        }
        fn from_raw(&self, bytes: &[u8]) -> i64 {
            i64::from_le_bytes(bytes.try_into().unwrap())
        }
    }

    fn fixture(keys: usize) -> (RecordBatch, Vec<ByteKey>) {
        let input = RecordBatch::try_from_iter(vec![(
            "key",
            Arc::new(Int64Array::from_iter_values(
                (0..keys).map(|key| key as i64),
            )) as ArrayRef,
        )])
        .unwrap();
        let mut encoder = BinaryRowBatchEncoder::new(&input, &[0], &[-1]);
        let keys = (0..keys)
            .map(|row| ByteKey::from(encoder.encode(row)))
            .collect();
        (input, keys)
    }

    fn store(directory: &str, writes: Arc<AtomicUsize>) -> RocksStore<Codec> {
        RocksStore::create(
            RocksStoreConfig {
                table_dir: directory.into(),
                max_parallelism: 1,
                options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            Codec(writes),
        )
        .unwrap()
    }

    #[test]
    fn fresh_keys_bypass_cache_rejected_hits_stay_clean_and_checkpoints_persist_changes() {
        let directory = tempfile::tempdir().unwrap();
        let writes = Arc::new(AtomicUsize::new(0));
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
        let mut store = store(
            directory.path().join("db").to_str().unwrap(),
            writes.clone(),
        );
        store.enable_resident_cache(
            64 * 1024,
            Some(MemoryConsumer::new("winner cache").register(&pool)),
        );
        let (input, keys) = fixture(2);
        store.begin_batch(&input, &[0], &[-1]).unwrap();
        for key in &keys {
            store.insert(key.clone(), 7);
        }
        store.end_bundle().unwrap();
        assert_eq!(store.decoded_cache_stats()[0], 0);
        assert_eq!(pool.reserved(), 0);
        store.begin_batch(&input, &[0], &[-1]).unwrap();
        store.end_bundle().unwrap();
        assert_eq!(store.decoded_cache_stats()[0], 2);
        assert_eq!(pool.reserved(), store.decoded_cache_stats()[3] as usize);
        store.begin_batch(&input, &[0], &[-1]).unwrap();
        assert_eq!(store.get(&keys[0].0), Some(&7));
        assert!(store.get_mut_if(&keys[0].0, |_| false).is_none());
        store.end_bundle().unwrap();
        assert_eq!(store.decoded_cache_stats()[1], 2);
        assert_eq!(writes.load(Ordering::Relaxed), 2);
        store.begin_batch(&input, &[0], &[-1]).unwrap();
        *store.get_mut(&keys[0].0).unwrap() = 11;
        store.end_bundle().unwrap();
        assert_eq!(writes.load(Ordering::Relaxed), 3);
        store.checkpoint("").unwrap();
        assert_eq!(store.decoded_cache_stats()[0], 0);
        assert_eq!(pool.reserved(), 0);
        store.begin_batch(&input, &[0], &[-1]).unwrap();
        assert_eq!(store.get(&keys[0].0), Some(&11));
        store.end_bundle().unwrap();
        drop(store);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn cache_capacity_and_failed_reservations_do_not_change_persistent_results() {
        for budget in [0, 1024] {
            let directory = tempfile::tempdir().unwrap();
            let writes = Arc::new(AtomicUsize::new(0));
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
            let mut store = store(directory.path().join("db").to_str().unwrap(), writes);
            store.enable_resident_cache(
                1024,
                Some(MemoryConsumer::new("bounded winner cache").register(&pool)),
            );
            let (input, keys) = fixture(32);
            store.begin_batch(&input, &[0], &[-1]).unwrap();
            for (value, key) in keys.iter().enumerate() {
                store.insert(key.clone(), value as i64);
            }
            store.end_bundle().unwrap();
            for _ in 0..3 {
                store.begin_batch(&input, &[0], &[-1]).unwrap();
                for (value, key) in keys.iter().enumerate() {
                    assert_eq!(store.get(&key.0), Some(&(value as i64)));
                }
                store.end_bundle().unwrap();
                assert!(store.decoded_cache_stats()[3] <= budget as i64);
                assert_eq!(pool.reserved(), store.decoded_cache_stats()[3] as usize);
            }
            assert_eq!(store.decoded_cache_stats()[0] == 0, budget == 0);
            store.canonical_keys_by_group().unwrap();
            assert_eq!(pool.reserved(), 0);
            store.finish_canonical_scan();
            drop(store);
            assert_eq!(pool.reserved(), 0);
        }
    }
}

pub(super) struct DecodedCache<V> {
    entries: ahash::HashMap<ByteKey, Entry<V>>,
    order: VecDeque<ByteKey>,
    admitted: ahash::HashSet<ByteKey>,
    reservation: Option<MemoryReservation>,
    limit: usize,
    bytes: usize,
    enabled: bool,
    hits: u64,
    misses: u64,
}

impl<V> DecodedCache<V> {
    pub(super) fn new(limit: usize, reservation: Option<MemoryReservation>) -> Self {
        Self {
            entries: ahash::HashMap::default(),
            order: VecDeque::new(),
            admitted: ahash::HashSet::default(),
            reservation,
            limit,
            bytes: 0,
            enabled: true,
            hits: 0,
            misses: 0,
        }
    }

    pub(super) fn take(&mut self, key: &[u8]) -> Option<V> {
        let value = self
            .entries
            .get_mut(key)
            .and_then(|entry| entry.value.take());
        if value.is_some() {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        value
    }

    pub(super) fn admit_existing(&mut self, key: &ByteKey) {
        if self.enabled {
            self.admitted.insert(key.clone());
        }
    }

    pub(super) fn active(&self) -> bool {
        self.enabled && (!self.entries.is_empty() || !self.admitted.is_empty())
    }

    pub(super) fn complete<C: RocksStateCodec<Value = V>>(
        &mut self,
        working: &mut ahash::HashMap<ByteKey, Slot<V>>,
        codec: &C,
    ) {
        if !self.active() {
            working.clear();
            return;
        }
        if let Some(reservation) = &mut self.reservation {
            let desired = working.iter().fold(self.bytes, |total, (key, slot)| {
                let previous = self.entries.get(key);
                if previous.is_none() && !self.admitted.contains(key) {
                    return total;
                }
                let Slot::Present { state, .. } = slot else {
                    return total;
                };
                let bytes = cache_entry_bytes(codec, key, state);
                if bytes > self.limit {
                    return total;
                }
                let old_bytes = previous.map_or(0, |entry| entry.bytes);
                total
                    .saturating_add(bytes.saturating_sub(old_bytes))
                    .min(self.limit)
            });
            if reservation.try_resize(desired).is_err() {
                self.clear();
                self.enabled = false;
                working.clear();
                return;
            }
        }
        for (key, slot) in working.drain() {
            if !self.entries.contains_key(&key) && !self.admitted.contains(&key) {
                continue;
            }
            let Slot::Present { state, .. } = slot else {
                self.remove(&key.0);
                continue;
            };
            let bytes = cache_entry_bytes(codec, &key, &state);
            if bytes > self.limit {
                self.remove(&key.0);
                continue;
            }
            if let Some(entry) = self.entries.get_mut(&key) {
                if self.bytes - entry.bytes + bytes <= self.limit {
                    self.bytes = self.bytes - entry.bytes + bytes;
                    entry.bytes = bytes;
                    entry.value = Some(state);
                    continue;
                }
                self.remove(&key.0);
            }
            while self.bytes + bytes > self.limit {
                let victim = self.order.pop_front().expect("cache eviction key");
                let entry = self.entries.remove(&victim).expect("cache eviction value");
                self.bytes -= entry.bytes;
            }
            self.order.push_back(key.clone());
            self.bytes += bytes;
            self.entries.insert(
                key,
                Entry {
                    value: Some(state),
                    bytes,
                },
            );
        }
        self.admitted.clear();
        if let Some(reservation) = &mut self.reservation {
            reservation.resize(self.bytes);
        }
    }

    fn remove(&mut self, key: &[u8]) {
        if let Some(entry) = self.entries.remove(key) {
            self.bytes -= entry.bytes;
            self.order.retain(|candidate| candidate.0.as_ref() != key);
        }
    }

    pub(super) fn clear(&mut self) {
        self.entries = ahash::HashMap::default();
        self.order = VecDeque::new();
        self.admitted = ahash::HashSet::default();
        self.bytes = 0;
        if let Some(reservation) = &mut self.reservation {
            reservation.resize(0);
        }
    }

    pub(super) fn stats(&self) -> [i64; 4] {
        [
            self.entries.len() as i64,
            self.hits as i64,
            self.misses as i64,
            self.bytes as i64,
        ]
    }
}

fn cache_entry_bytes<C: RocksStateCodec>(codec: &C, key: &ByteKey, state: &C::Value) -> usize {
    codec.resident_bytes(state)
        + 2 * byte_key_bytes(&key.0)
        + 2 * GROUP_ENTRY_OVERHEAD
        + std::mem::size_of::<Entry<C::Value>>()
        - std::mem::size_of::<C::Value>()
}
