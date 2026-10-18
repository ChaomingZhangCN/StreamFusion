use super::*;
use std::collections::VecDeque;

#[derive(Clone)]
pub(super) struct TextValue {
    pub text: Arc<str>,
    pub cache: Option<Arc<[u16]>>,
}

impl TextValue {
    pub(super) fn equals(&self, other: &Self) -> bool {
        match (&self.cache, &other.cache) {
            (Some(left), Some(right)) => left == right,
            _ => self.text == other.text,
        }
    }

    pub(super) fn compare(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.cache, &other.cache) {
            (Some(left), Some(right)) => left.cmp(right),
            _ => self.text.cmp(&other.text),
        }
    }

    pub(super) fn cold(&self) -> Self {
        Self {
            text: self.text.clone(),
            cache: None,
        }
    }

    pub(super) fn bytes(&self) -> usize {
        self.text.len()
            + 2 * std::mem::size_of::<usize>()
            + self.cache.as_ref().map_or(0, |units| {
                units.len() * 2 + 2 * std::mem::size_of::<usize>()
            })
    }
}

#[derive(Clone)]
struct ValueOrders {
    value: TextValue,
    hash: i32,
    orders: VecDeque<i64>,
}

/// Flink's selected value and two retractable views remain independent of the physical frame.
#[derive(Clone)]
pub(super) struct StringFirstLast {
    last: bool,
    next_order: i64,
    selected: Option<(i64, TextValue)>,
    value_orders: Vec<ValueOrders>,
    order_values: Vec<(i64, Vec<TextValue>)>,
}

impl StringFirstLast {
    pub fn new(last: bool) -> Self {
        Self {
            last,
            next_order: 0,
            selected: None,
            value_orders: Vec::new(),
            order_values: Vec::new(),
        }
    }

    pub fn selected(&self) -> Option<&TextValue> {
        self.selected.as_ref().map(|(_, value)| value)
    }

    pub fn add(&mut self, text: &Arc<str>, cache: Option<&Arc<[u16]>>) {
        let value = TextValue {
            text: text.clone(),
            cache: cache.cloned(),
        };
        let hash = string_hash(value.text.as_bytes());
        let order = self.next_order;
        self.next_order = self.next_order.checked_add(1).expect("STRING OVER order");
        if let Some(entry) = self
            .value_orders
            .iter_mut()
            .find(|entry| entry.hash == hash && value.equals(&entry.value))
        {
            entry.orders.push_back(order);
        } else {
            self.value_orders.push(ValueOrders {
                value: value.clone(),
                hash,
                orders: VecDeque::from([order]),
            });
        }
        if self.last || self.selected.is_none() {
            self.selected = Some((order, value.clone()));
        }
        self.order_values.push((order, vec![value]));
    }

    pub fn retract(&mut self, text: &Arc<str>, cache: Option<&Arc<[u16]>>) {
        let value = TextValue {
            text: text.clone(),
            cache: cache.cloned(),
        };
        let hash = string_hash(value.text.as_bytes());
        let Some(index) = self
            .value_orders
            .iter()
            .position(|entry| entry.hash == hash && value.equals(&entry.value))
        else {
            return;
        };
        let Some(order) = self.value_orders[index].orders.pop_front() else {
            return;
        };
        if self.value_orders[index].orders.is_empty() {
            self.value_orders.remove(index);
        }
        let Some(index) = self
            .order_values
            .iter()
            .position(|(candidate, _)| *candidate == order)
        else {
            return;
        };
        let values = &mut self.order_values[index].1;
        if let Some(index) = values.iter().position(|candidate| value.equals(candidate)) {
            values.remove(index);
        }
        if values.is_empty() {
            self.order_values.remove(index);
        }
        if let Some((selected_order, selected)) = &self.selected {
            if value.equals(selected) {
                let next = if self.last {
                    self.order_values
                        .iter()
                        .rev()
                        .find(|(order, _)| order <= selected_order)
                } else {
                    self.order_values
                        .iter()
                        .find(|(order, _)| order >= selected_order)
                };
                self.selected = next.map(|(order, values)| {
                    (
                        *order,
                        if self.last {
                            values.last().unwrap()
                        } else {
                            &values[0]
                        }
                        .clone(),
                    )
                });
            }
        }
    }

    pub fn cold(&mut self) {
        if let Some((_, value)) = &mut self.selected {
            *value = value.cold();
        }
        for (_, values) in &mut self.order_values {
            for value in values {
                *value = value.cold();
            }
        }
        let mut restored: Vec<ValueOrders> = Vec::new();
        for mut entry in self.value_orders.drain(..) {
            entry.value = entry.value.cold();
            if let Some(previous) = restored
                .iter_mut()
                .find(|value| value.hash == entry.hash && value.value.equals(&entry.value))
            {
                previous.orders = entry.orders;
            } else {
                restored.push(entry);
            }
        }
        self.value_orders = restored;
    }

    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.value_orders.capacity() * std::mem::size_of::<ValueOrders>()
            + self
                .value_orders
                .iter()
                .map(|entry| entry.value.bytes() + entry.orders.capacity() * 8)
                .sum::<usize>()
            + self.order_values.capacity() * std::mem::size_of::<(i64, Vec<TextValue>)>()
            + self
                .order_values
                .iter()
                .map(|(_, values)| {
                    values.capacity() * std::mem::size_of::<TextValue>()
                        + values.iter().map(TextValue::bytes).sum::<usize>()
                })
                .sum::<usize>()
            + self.selected.as_ref().map_or(0, |(_, value)| value.bytes())
    }

    pub fn snapshot(&self) -> Vec<u8> {
        let mut kinds = Vec::new();
        let mut orders = Vec::new();
        let mut texts = Vec::new();
        let mut lists: Vec<Option<Vec<i64>>> = Vec::new();
        let selected = self.selected.as_ref();
        kinds.push(0i8);
        orders.push(selected.map(|(order, _)| *order));
        texts.push(selected.map(|(_, value)| value.text.as_ref()));
        lists.push(None);
        for entry in &self.value_orders {
            kinds.push(1);
            orders.push(None);
            texts.push(Some(entry.value.text.as_ref()));
            lists.push(Some(entry.orders.iter().copied().collect()));
        }
        for (order, values) in &self.order_values {
            for value in values {
                kinds.push(2);
                orders.push(Some(*order));
                texts.push(Some(value.text.as_ref()));
                lists.push(None);
            }
        }
        let list = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(
            lists
                .into_iter()
                .map(|list| list.map(|values| values.into_iter().map(Some).collect::<Vec<_>>())),
        );
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(kinds)),
            Arc::new(Int64Array::from(orders)),
            Arc::new(StringArray::from(texts)),
            Arc::new(list),
        ];
        let fields = columns
            .iter()
            .zip(["kind", "order", "value", "orders"])
            .map(|(array, name)| Field::new(name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let metadata = std::collections::HashMap::from([(
            "next_order".to_owned(),
            self.next_order.to_string(),
        )]);
        write_ipc(
            &RecordBatch::try_new(
                Arc::new(Schema::new(fields).with_metadata(metadata)),
                columns,
            )
            .unwrap(),
        )
    }

    pub fn restore(last: bool, bytes: &[u8]) -> Self {
        let mut state = Self::new(last);
        for batch in read_ipc(bytes) {
            state.next_order = batch.schema().metadata()["next_order"]
                .parse()
                .expect("STRING order");
            let kinds = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            let orders = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let lists = batch
                .column(3)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let value = (!values.is_null(row)).then(|| TextValue {
                    text: Arc::from(values.value(row)),
                    cache: None,
                });
                match kinds.value(row) {
                    0 => state.selected = value.map(|value| (orders.value(row), value)),
                    1 => {
                        let value = value.unwrap();
                        let hash = string_hash(value.text.as_bytes());
                        let array = lists.value(row);
                        let times = array.as_any().downcast_ref::<Int64Array>().unwrap();
                        let times = times.values().iter().copied().collect::<VecDeque<_>>();
                        if let Some(previous) = state
                            .value_orders
                            .iter_mut()
                            .find(|entry| entry.hash == hash && entry.value.equals(&value))
                        {
                            previous.orders = times;
                        } else {
                            state.value_orders.push(ValueOrders {
                                value,
                                hash,
                                orders: times,
                            });
                        }
                    }
                    2 => {
                        let order = orders.value(row);
                        let value = value.unwrap();
                        if let Some((_, values)) = state
                            .order_values
                            .last_mut()
                            .filter(|(key, _)| *key == order)
                        {
                            values.push(value);
                        } else {
                            state.order_values.push((order, vec![value]));
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        state
    }
}

/// BinaryStringData hashes its raw bytes, including signed-byte tails, independently of its cache.
pub(super) fn string_hash(bytes: &[u8]) -> i32 {
    fn mix(hash: u32, value: u32) -> u32 {
        let value = value
            .wrapping_mul(0xcc9e2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b873593);
        (hash ^ value)
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe6546b64)
    }
    let mut hash = 42;
    let mut words = bytes.chunks_exact(4);
    for word in &mut words {
        hash = mix(hash, u32::from_ne_bytes(word.try_into().unwrap()));
    }
    for &byte in words.remainder() {
        hash = mix(hash, byte as i8 as i32 as u32);
    }
    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    (hash ^ (hash >> 16)) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialized_key_collision_keeps_independent_selected_and_order_views() {
        let text: Arc<str> = Arc::from("?");
        let first: Arc<[u16]> = Arc::from([0xd800]);
        let second: Arc<[u16]> = Arc::from([0xd801]);
        for last in [false, true] {
            let mut before = StringFirstLast::new(last);
            before.add(&text, Some(&first));
            before.add(&text, Some(&second));
            let mut restored = StringFirstLast::restore(last, &before.snapshot());
            assert_eq!(restored.value_orders.len(), 1);
            assert_eq!(restored.order_values.len(), 2);
            restored.retract(&text, None);
            restored.add(&text, None);
            restored.retract(&text, None);
            restored.add(&text, Some(&first));
            restored.retract(&text, None);
            let winner = restored.selected().unwrap();
            assert_eq!(winner.text.as_ref(), "?");
            assert!(winner.cache.is_none());
            assert!(restored.order_values.iter().any(|(order, _)| *order == 0));
        }
    }

    #[test]
    fn persistent_cold_maps_keep_ordinary_frame_eviction_and_null_independence() {
        for last in [false, true] {
            let mut state = StringFirstLast::new(last);
            let a: Arc<str> = Arc::from("😀");
            let b: Arc<str> = Arc::from("\u{e000}");
            let cache: Arc<[u16]> = Arc::from([0xd83d, 0xde00]);
            state.add(&a, Some(&cache));
            state.cold();
            state.add(&b, None);
            state.retract(&a, None);
            assert_eq!(state.selected().unwrap().text.as_ref(), "\u{e000}");
            assert_eq!(state.order_values.len(), 1);
            state.retract(&b, None);
            assert!(state.selected().is_none());
            assert!(state.value_orders.is_empty());
            assert!(state.order_values.is_empty());
        }
    }
}
