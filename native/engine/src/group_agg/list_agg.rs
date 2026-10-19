use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub(super) fn is_list_agg(kind: i64) -> bool {
    matches!(kind, 24 | 25 | 32 | 33)
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ListAggState {
    append_only: bool,
    prefix: Option<String>,
    separator: Option<String>,
    values: VecDeque<String>,
    pending_retractions: VecDeque<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    distinct: Option<Box<ahash::HashMap<String, i64>>>,
    #[serde(skip)]
    distinct_bytes: usize,
    #[serde(skip)]
    element_bytes: usize,
}

impl ListAggState {
    pub(super) fn new(kind: i64) -> Self {
        Self {
            append_only: matches!(kind, 24 | 32),
            prefix: None,
            separator: None,
            values: VecDeque::new(),
            pending_retractions: VecDeque::new(),
            distinct: matches!(kind, 32 | 33).then(Box::default),
            distinct_bytes: 0,
            element_bytes: 0,
        }
    }

    pub(super) fn update(&mut self, value: Option<&str>, separator: &str, retract: bool) {
        let Some(value) = value else { return };
        if let Some(counts) = &mut self.distinct {
            assert!(
                !self.append_only || !retract,
                "insert-only LISTAGG cannot retract"
            );
            let next = if let Some(count) = counts.get_mut(value) {
                if self.append_only {
                    return;
                }
                *count = count.wrapping_add(if retract { -1 } else { 1 });
                *count
            } else {
                let key = value.to_owned();
                self.distinct_bytes += key.capacity();
                let next = if retract { -1 } else { 1 };
                counts.insert(key, next);
                next
            };
            if next == 0 {
                let (key, _) = counts.remove_entry(value).expect("existing LISTAGG member");
                self.distinct_bytes -= key.capacity();
            }
            if (retract && next != 0) || (!retract && next != 1) {
                return;
            }
        }
        if self.separator.as_deref() != Some(separator) {
            self.separator = Some(separator.to_owned());
        }
        if self.append_only {
            assert!(!retract, "insert-only LISTAGG cannot retract");
            match &mut self.prefix {
                Some(prefix) => {
                    prefix.push_str(separator);
                    prefix.push_str(value);
                }
                None => self.prefix = Some(value.to_owned()),
            }
        } else if retract {
            if let Some(index) = self.values.iter().position(|old| old == value) {
                self.element_bytes -= self.values.remove(index).unwrap().capacity();
            } else {
                let value = value.to_owned();
                self.element_bytes += value.capacity();
                self.pending_retractions.push_back(value);
            }
        } else {
            let value = value.to_owned();
            self.element_bytes += value.capacity();
            self.values.push_back(value);
        }
    }

    pub(super) fn emit(&self) -> ScalarValue {
        let value = if self.append_only {
            self.prefix.clone()
        } else if self.values.is_empty() {
            None
        } else {
            let separator = self.separator.as_deref().unwrap_or(",");
            let mut result = String::new();
            for (index, value) in self.values.iter().enumerate() {
                if index != 0 {
                    result.push_str(separator);
                }
                result.push_str(value);
            }
            Some(result)
        };
        ScalarValue::Utf8(value)
    }

    pub(super) fn snapshot(&self) -> ScalarValue {
        ScalarValue::Binary(Some(
            serde_json::to_vec(&(1_u8, self)).expect("serialize LISTAGG state"),
        ))
    }

    pub(super) fn restore(&mut self, value: ScalarValue) -> Result<(), DataFusionError> {
        let ScalarValue::Binary(Some(bytes)) = value else {
            return Err(DataFusionError::Execution("missing LISTAGG state".into()));
        };
        let (version, mut restored): (u8, Self) =
            serde_json::from_slice(&bytes).map_err(|error| {
                DataFusionError::Execution(format!("invalid LISTAGG state: {error}"))
            })?;
        if version != 1
            || restored.append_only != self.append_only
            || restored.distinct.is_some() != self.distinct.is_some()
        {
            return Err(DataFusionError::Execution(
                "incompatible LISTAGG state".into(),
            ));
        }
        restored.element_bytes = restored
            .values
            .iter()
            .chain(&restored.pending_retractions)
            .map(String::capacity)
            .sum();
        restored.distinct_bytes = restored
            .distinct
            .as_ref()
            .map_or(0, |counts| counts.keys().map(String::capacity).sum());
        *self = restored;
        Ok(())
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.distinct_bytes
            + self.distinct.as_ref().map_or(0, |counts| {
                std::mem::size_of_val(counts.as_ref())
                    + counts.capacity() * (std::mem::size_of::<(String, i64)>() + 1)
            })
            + self.element_bytes
            + self.prefix.as_ref().map_or(0, String::capacity)
            + self.separator.as_ref().map_or(0, String::capacity)
            + (self.values.capacity() + self.pending_retractions.capacity())
                * std::mem::size_of::<String>()
    }
}

pub(super) enum StringColumn<'a> {
    Utf8(&'a StringArray),
    Large(&'a arrow::array::LargeStringArray),
    View(&'a arrow::array::StringViewArray),
}

impl<'a> StringColumn<'a> {
    pub(super) fn new(array: &'a ArrayRef) -> Self {
        match array.data_type() {
            DataType::Utf8 => Self::Utf8(array.as_any().downcast_ref().unwrap()),
            DataType::LargeUtf8 => Self::Large(array.as_any().downcast_ref().unwrap()),
            DataType::Utf8View => Self::View(array.as_any().downcast_ref().unwrap()),
            other => panic!("LISTAGG requires a string column, got {other}"),
        }
    }
    pub(super) fn at(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Large(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::View(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_duplicates_retractions_reinsertion_and_restore_keep_arrival_order() {
        for kind in [32, 33] {
            let mut state = ListAggState::new(kind);
            for value in [
                None,
                Some(""),
                Some("a"),
                Some("b"),
                Some("a"),
                Some("é🚀\0"),
            ] {
                state.update(value, "::", false);
            }
            assert_eq!(
                state.emit(),
                ScalarValue::Utf8(Some("::a::b::é🚀\0".into()))
            );
            let mut restored = ListAggState::new(kind);
            restored.restore(state.snapshot()).unwrap();
            assert_eq!(state.emit(), restored.emit());
            assert_eq!(
                restored.distinct_bytes,
                restored
                    .distinct
                    .as_ref()
                    .unwrap()
                    .keys()
                    .map(String::capacity)
                    .sum::<usize>()
            );
            if kind == 33 {
                restored.update(Some("a"), "::", true);
                assert_eq!(state.emit(), restored.emit());
                restored.update(Some("a"), "::", true);
                restored.update(Some("a"), "::", false);
                assert_eq!(
                    restored.emit(),
                    ScalarValue::Utf8(Some("::b::é🚀\0::a".into()))
                );
                let mut second = ListAggState::new(kind);
                second.restore(restored.snapshot()).unwrap();
                for value in ["", "b", "é🚀\0", "a"] {
                    second.update(Some(value), "::", true);
                }
                assert_eq!(second.emit(), ScalarValue::Utf8(None));
                assert_eq!(second.distinct_bytes, 0);
            }
            assert!(ListAggState::new(if kind == 32 { 24 } else { 25 })
                .restore(state.snapshot())
                .is_err());
        }
    }

    #[test]
    fn distinct_negative_multiplicity_does_not_replay_a_pending_retraction() {
        let mut state = ListAggState::new(33);
        state.update(Some("a"), ",", true);
        let mut restored = ListAggState::new(33);
        restored.restore(state.snapshot()).unwrap();
        restored.update(Some("a"), ",", false);
        assert_eq!(restored.emit(), ScalarValue::Utf8(None));
        restored.update(Some("a"), ",", false);
        assert_eq!(restored.emit(), ScalarValue::Utf8(Some("a".into())));
    }

    #[test]
    fn append_preserves_empty_values_nulls_and_separators() {
        for separator in ["", ",", "::", "é🚀", "'"] {
            let mut state = ListAggState::new(24);
            assert_eq!(state.emit(), ScalarValue::Utf8(None));
            state.update(None, separator, false);
            for value in ["", "a", "a", "\0\n🚀"] {
                state.update(Some(value), separator, false);
            }
            assert_eq!(
                state.emit(),
                ScalarValue::Utf8(Some(["", "a", "a", "\0\n🚀"].join(separator)))
            );
            let mut restored = ListAggState::new(24);
            restored.restore(state.snapshot()).unwrap();
            assert_eq!(state.emit(), restored.emit());
            assert!(restored.bytes() >= std::mem::size_of::<ListAggState>());
        }
    }

    #[test]
    fn retract_removes_first_equal_occurrence_and_retains_pending_retractions() {
        let mut state = ListAggState::new(25);
        for value in ["a", "b", "a", "c"] {
            state.update(Some(value), "::", false);
        }
        state.update(Some("a"), "::", true);
        state.update(Some("absent"), "::", true);
        state.update(Some("absent"), "::", false);
        assert_eq!(
            state.emit(),
            ScalarValue::Utf8(Some("b::a::c::absent".into()))
        );
        let mut restored = ListAggState::new(25);
        restored.restore(state.snapshot()).unwrap();
        assert_eq!(restored.pending_retractions, ["absent"]);
        assert_eq!(restored.emit(), state.emit());
        for value in ["b", "a", "c", "absent"] {
            restored.update(Some(value), "::", true);
        }
        assert_eq!(restored.emit(), ScalarValue::Utf8(None));
        assert_eq!(restored.element_bytes, "absent".len());
    }
}
