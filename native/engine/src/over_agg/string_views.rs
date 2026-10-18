use super::string_extreme::StringExtreme;
use super::string_first_last::{StringFirstLast, TextValue};
use super::*;

#[derive(Clone)]
pub(super) enum StringView {
    FirstLast(StringFirstLast),
    Extreme(StringExtreme),
}

impl StringView {
    pub fn new(kind: i64) -> Self {
        match kind {
            1 | 2 => Self::Extreme(StringExtreme::new(kind == 2)),
            5 | 6 => Self::FirstLast(StringFirstLast::new(kind == 6)),
            _ => unreachable!(),
        }
    }
    pub fn selected(&self) -> Option<&TextValue> {
        match self {
            Self::FirstLast(view) => view.selected(),
            Self::Extreme(view) => view.selected(),
        }
    }
    pub fn set_persistent(&mut self, persistent: bool) {
        if let Self::Extreme(view) = self {
            view.set_persistent(persistent);
        }
    }
    pub fn copy_heap(&mut self) {
        if let Self::Extreme(view) = self {
            view.copy_heap();
        }
    }
    pub fn restore_canonical_order(&mut self) {
        if let Self::Extreme(view) = self {
            view.restore_canonical_order();
        }
    }
    pub fn add(&mut self, text: &Arc<str>, cache: Option<&Arc<[u16]>>) {
        match self {
            Self::FirstLast(view) => view.add(text, cache),
            Self::Extreme(view) => view.add(text, cache),
        }
    }
    pub fn retract(&mut self, text: &Arc<str>, cache: Option<&Arc<[u16]>>) {
        match self {
            Self::FirstLast(view) => view.retract(text, cache),
            Self::Extreme(view) => view.retract(text, cache),
        }
    }
    pub fn cold(&mut self) {
        match self {
            Self::FirstLast(view) => view.cold(),
            Self::Extreme(view) => view.cold(),
        }
    }
    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::FirstLast(view) => view.bytes() - std::mem::size_of::<StringFirstLast>(),
                Self::Extreme(view) => view.bytes() - std::mem::size_of::<StringExtreme>(),
            }
    }
    pub fn snapshot(&self) -> Vec<u8> {
        match self {
            Self::FirstLast(view) => view.snapshot(),
            Self::Extreme(view) => view.snapshot(),
        }
    }
    pub fn restore(kind: i64, bytes: &[u8]) -> Self {
        match kind {
            1 | 2 => Self::Extreme(StringExtreme::restore(kind == 2, bytes)),
            5 | 6 => Self::FirstLast(StringFirstLast::restore(kind == 6, bytes)),
            _ => unreachable!(),
        }
    }
}
