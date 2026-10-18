use super::string_first_last::{string_hash, TextValue};
use super::*;

#[derive(Clone)]
struct CountedValue {
    value: TextValue,
    hash: i32,
    count: i64,
    order_key: i64,
}

#[derive(Clone)]
pub(super) struct StringExtreme {
    maximum: bool,
    persistent: bool,
    heap_order: HeapRangeOrder,
    next_key: u32,
    free_keys: Vec<u32>,
    map_size: i64,
    selected: Option<TextValue>,
    entries: Vec<CountedValue>,
}

impl StringExtreme {
    pub fn new(maximum: bool) -> Self {
        Self {
            maximum,
            persistent: false,
            heap_order: HeapRangeOrder::default(),
            next_key: 0,
            free_keys: Vec::new(),
            map_size: 0,
            selected: None,
            entries: Vec::new(),
        }
    }
    pub fn set_persistent(&mut self, persistent: bool) {
        self.persistent = persistent;
    }
    pub fn copy_heap(&mut self) {
        self.heap_order = HeapRangeOrder::restored(&self.heap_order.keys());
    }
    pub fn restore_canonical_order(&mut self) {
        let keys = self
            .entries
            .iter()
            .map(|entry| entry.order_key)
            .collect::<Vec<_>>();
        self.heap_order = HeapRangeOrder::canonical(&keys);
    }
    pub fn selected(&self) -> Option<&TextValue> {
        (self.map_size > 0)
            .then_some(self.selected.as_ref())
            .flatten()
    }
    fn improves(&self, value: &TextValue, current: &TextValue) -> bool {
        let ordering = value.compare(current);
        if self.maximum {
            ordering.is_gt()
        } else {
            ordering.is_lt()
        }
    }
    fn index(&self, value: &TextValue, hash: i32) -> Option<usize> {
        self.entries
            .iter()
            .position(|entry| entry.hash == hash && value.equals(&entry.value))
    }
    fn put(&mut self, value: TextValue, hash: i32, count: i64) {
        if let Some(index) = self.index(&value, hash) {
            self.entries[index].count = count;
        } else {
            // Long.hashCode(high=id, low=id XOR hash) gives the string's raw hash.
            // Distinct hashes therefore share the existing JDK bucket/tree implementation.
            let id = self.free_keys.pop().unwrap_or_else(|| {
                let id = self.next_key;
                self.next_key = id.checked_add(1).expect("STRING map membership size");
                id
            });
            let order_key = ((id as i64) << 32) | (id ^ hash as u32) as i64;
            self.heap_order.insert(order_key);
            self.entries.push(CountedValue {
                value,
                hash,
                count,
                order_key,
            });
        }
    }
    fn remove(&mut self, index: usize) {
        self.free_keys
            .push((self.entries[index].order_key as u64 >> 32) as u32);
        self.heap_order.remove(self.entries[index].order_key);
        self.entries.remove(index);
    }
    fn iteration(&self) -> Vec<usize> {
        let mut order = (0..self.entries.len()).collect::<Vec<_>>();
        if self.persistent {
            order.sort_by(|&left, &right| {
                let left = self.entries[left].value.text.as_bytes();
                let right = self.entries[right].value.text.as_bytes();
                left.len().cmp(&right.len()).then_with(|| left.cmp(right))
            });
        } else {
            order = self
                .heap_order
                .keys()
                .iter()
                .map(|key| {
                    self.entries
                        .iter()
                        .position(|entry| entry.order_key == *key)
                        .expect("STRING map iteration membership")
                })
                .collect();
        }
        order
    }
    pub fn add(&mut self, text: &Arc<str>, cache: Option<&Arc<[u16]>>) {
        let value = TextValue {
            text: text.clone(),
            cache: cache.cloned(),
        };
        if self.map_size == 0
            || self
                .selected
                .as_ref()
                .is_none_or(|current| self.improves(&value, current))
        {
            self.selected = Some(value.clone());
        }
        let hash = string_hash(value.text.as_bytes());
        let count = self
            .index(&value, hash)
            .map_or(0, |index| self.entries[index].count)
            .wrapping_add(1);
        if count == 0 {
            if let Some(index) = self.index(&value, hash) {
                self.remove(index);
            }
        } else {
            self.put(value, hash, count);
        }
        if count == 1 {
            self.map_size = self.map_size.wrapping_add(1);
        }
    }
    pub fn retract(&mut self, text: &Arc<str>, cache: Option<&Arc<[u16]>>) {
        let value = TextValue {
            text: text.clone(),
            cache: cache.cloned(),
        };
        let hash = string_hash(value.text.as_bytes());
        let count = self
            .index(&value, hash)
            .map_or(0, |index| self.entries[index].count)
            .wrapping_sub(1);
        if count == 0 {
            if let Some(index) = self.index(&value, hash) {
                self.remove(index);
            }
            self.map_size = self.map_size.wrapping_sub(1);
            if self.map_size == 0 {
                self.selected = None;
                return;
            }
            if self
                .selected
                .as_ref()
                .is_some_and(|selected| value.equals(selected))
            {
                self.selected = None;
                for index in self.iteration() {
                    let candidate = &self.entries[index].value;
                    if self
                        .selected
                        .as_ref()
                        .is_none_or(|current| self.improves(candidate, current))
                    {
                        self.selected = Some(candidate.clone());
                    }
                }
                if self.selected.is_none() {
                    self.map_size = 0;
                }
            }
        } else {
            self.put(value, hash, count);
        }
    }
    pub fn cold(&mut self) {
        if let Some(value) = &mut self.selected {
            *value = value.cold();
        }
        let old = self
            .iteration()
            .into_iter()
            .map(|index| self.entries[index].clone())
            .collect::<Vec<_>>();
        self.entries.clear();
        self.heap_order = HeapRangeOrder::with_expected_size(old.len());
        let mut entries: Vec<CountedValue> = Vec::new();
        for entry in old {
            let value = entry.value.cold();
            if let Some(previous) = entries
                .iter_mut()
                .find(|existing| existing.hash == entry.hash && value.equals(&existing.value))
            {
                previous.count = entry.count;
                self.free_keys.push((entry.order_key as u64 >> 32) as u32);
            } else {
                entries.push(CountedValue { value, ..entry });
            }
        }
        for entry in &entries {
            self.heap_order.insert(entry.order_key);
        }
        self.entries = entries;
    }
    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.heap_order.bytes()
            - std::mem::size_of::<HeapRangeOrder>()
            + self.free_keys.capacity() * std::mem::size_of::<u32>()
            + self.entries.capacity() * std::mem::size_of::<CountedValue>()
            + self
                .entries
                .iter()
                .map(|entry| entry.value.bytes())
                .sum::<usize>()
            + self.selected.as_ref().map_or(0, TextValue::bytes)
    }
    pub fn snapshot(&self) -> Vec<u8> {
        let mut kinds = vec![0i8];
        let mut values = vec![self.selected.as_ref().map(|value| value.text.as_ref())];
        let mut counts = vec![self.map_size];
        for index in self.iteration() {
            let entry = &self.entries[index];
            kinds.push(1);
            values.push(Some(entry.value.text.as_ref()));
            counts.push(entry.count);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(kinds)),
            Arc::new(StringArray::from(values)),
            Arc::new(Int64Array::from(counts)),
        ];
        let fields = columns
            .iter()
            .zip(["kind", "value", "count"])
            .map(|(array, name)| Field::new(name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap())
    }
    pub fn restore(maximum: bool, bytes: &[u8]) -> Self {
        let mut state = Self::new(maximum);
        for batch in read_ipc(bytes) {
            let kinds = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let size = batch.num_rows().saturating_sub(1);
            state.heap_order = HeapRangeOrder::with_expected_size(size);
            for row in 0..batch.num_rows() {
                let value = (!values.is_null(row)).then(|| TextValue {
                    text: Arc::from(values.value(row)),
                    cache: None,
                });
                if kinds.value(row) == 0 {
                    state.selected = value;
                    state.map_size = counts.value(row);
                } else {
                    let value = value.unwrap();
                    let hash = string_hash(value.text.as_bytes());
                    state.put(value, hash, counts.value(row));
                }
            }
        }
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_membership_reuses_identifiers_after_eviction() {
        let text: Arc<str> = Arc::from("value");
        let mut state = StringExtreme::new(false);
        state.next_key = u32::MAX - 1;
        for _ in 0..4 {
            state.add(&text, None);
            state.retract(&text, None);
        }
        assert!(state.entries.is_empty());
        assert_eq!(state.free_keys.len(), 1);
    }

    #[test]
    fn retiring_a_winner_rescans_distinct_hash_tree_bins_in_jdk_order() {
        let mut state = StringExtreme::new(false);
        let first: Arc<str> = Arc::from("a");
        let first_cache: Arc<[u16]> = first.encode_utf16().collect();
        state.add(&first, Some(&first_cache));
        let mut values = 0;
        for candidate in 3000.. {
            let text: Arc<str> = Arc::from(format!(
                "{}{candidate}",
                ["😀", "\u{e000}", "\u{e100}"][candidate % 3]
            ));
            let hash = string_hash(text.as_bytes()) as u32;
            if (hash ^ (hash >> 16)) & 63 != 10 {
                continue;
            }
            let cache: Arc<[u16]> = text.encode_utf16().collect();
            state.add(&text, (!text.starts_with('\u{e100}')).then_some(&cache));
            values += 1;
            if values == 24 {
                break;
            }
        }
        state.retract(&first, Some(&first_cache));
        // Released Flink 1.18/2.2 operator controls select this cached value.
        assert_eq!(state.selected().unwrap().text.as_ref(), "😀3258");
    }

    #[test]
    fn negative_membership_balances_without_emitting_the_selected_scalar() {
        let text: Arc<str> = Arc::from("value");
        for maximum in [false, true] {
            let mut state = StringExtreme::new(maximum);
            state.retract(&text, None);
            state.add(&text, None);
            assert_eq!(state.map_size, 0);
            assert!(state.entries.is_empty());
            assert!(state.selected().is_none());
            let mut restored = StringExtreme::restore(maximum, &state.snapshot());
            assert!(restored.selected().is_none());
            restored.add(&text, None);
            assert_eq!(restored.selected().unwrap().text.as_ref(), "value");
        }
    }

    #[test]
    fn serialized_duplicate_keys_keep_the_independent_positive_membership_count() {
        let text: Arc<str> = Arc::from("?");
        let first: Arc<[u16]> = Arc::from([0xd800]);
        let second: Arc<[u16]> = Arc::from([0xd801]);
        for maximum in [false, true] {
            let mut state = StringExtreme::new(maximum);
            state.add(&text, Some(&first));
            state.add(&text, Some(&second));
            assert_eq!(state.entries.len(), 2);
            let mut restored = StringExtreme::restore(maximum, &state.snapshot());
            assert_eq!(restored.entries.len(), 1);
            assert_eq!(restored.map_size, 2);
            assert_eq!(restored.entries[0].count, 1);
            restored.retract(&text, None);
            assert_eq!(restored.map_size, 0);
            assert!(restored.selected().is_none());
        }
    }

    #[test]
    fn non_transitive_comparison_keeps_the_incremental_winner_until_retirement() {
        let emoji: Arc<str> = Arc::from("😀");
        let bmp: Arc<str> = Arc::from("\u{e000}");
        let binary: Arc<str> = Arc::from("\u{e100}");
        let emoji_cache: Arc<[u16]> = emoji.encode_utf16().collect();
        let bmp_cache: Arc<[u16]> = bmp.encode_utf16().collect();
        let mut state = StringExtreme::new(false);
        state.add(&emoji, Some(&emoji_cache));
        state.add(&bmp, Some(&bmp_cache));
        state.add(&binary, None);
        assert_eq!(state.selected().unwrap().text, binary);
        state.retract(&emoji, Some(&emoji_cache));
        assert_eq!(state.selected().unwrap().text, binary);
        state.retract(&binary, None);
        assert_eq!(state.selected().unwrap().text, bmp);
    }
}
