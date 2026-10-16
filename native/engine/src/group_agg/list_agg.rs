use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub(super) fn is_list_agg(kind: i64) -> bool {
    matches!(kind, 24 | 25)
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ListAggState {
    append_only: bool,
    prefix: Option<String>,
    separator: Option<String>,
    values: VecDeque<String>,
    pending_retractions: VecDeque<String>,
    #[serde(skip)]
    element_bytes: usize,
}

impl ListAggState {
    pub(super) fn new(kind: i64) -> Self {
        Self {
            append_only: kind == 24,
            prefix: None,
            separator: None,
            values: VecDeque::new(),
            pending_retractions: VecDeque::new(),
            element_bytes: 0,
        }
    }

    pub(super) fn update(&mut self, value: Option<&str>, separator: &str, retract: bool) {
        let Some(value) = value else { return };
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
        if version != 1 || restored.append_only != self.append_only {
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
        *self = restored;
        Ok(())
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
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
