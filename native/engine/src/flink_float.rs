//! Java boxed floating equality canonicalizes NaN payloads but preserves signed zero.
//! Scalar grouping keys intentionally keep their raw bits instead.

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion::common::ScalarValue;
use std::sync::Arc;

pub(crate) fn canonical_f32(value: f32) -> f32 {
    if value.is_nan() {
        f32::from_bits(0x7fc00000)
    } else {
        value
    }
}

pub(crate) fn canonical_f64(value: f64) -> f64 {
    if value.is_nan() {
        f64::from_bits(0x7ff8000000000000)
    } else {
        value
    }
}

pub(crate) fn canonical_scalar(value: &ScalarValue) -> Option<ScalarValue> {
    match value {
        ScalarValue::Float32(Some(v)) if v.is_nan() => {
            Some(ScalarValue::Float32(Some(canonical_f32(*v))))
        }
        ScalarValue::Float64(Some(v)) if v.is_nan() => {
            Some(ScalarValue::Float64(Some(canonical_f64(*v))))
        }
        _ => None,
    }
}

pub(crate) fn boxed_equal(left: &ScalarValue, right: &ScalarValue) -> bool {
    match (left, right) {
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
            canonical_f32(*a).to_bits() == canonical_f32(*b).to_bits()
        }
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
            canonical_f64(*a).to_bits() == canonical_f64(*b).to_bits()
        }
        _ => left == right,
    }
}

/// Flink's generated aggregate-result equaliser uses primitive floating comparison.
pub(crate) fn record_equal(left: &ScalarValue, right: &ScalarValue) -> bool {
    match (left, right) {
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => a == b,
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => a == b,
        _ => left == right,
    }
}

pub(crate) fn record_row_equal(left: &[ScalarValue], right: &[ScalarValue]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(a, b)| record_equal(a, b))
}

pub(crate) fn record_row_reflexive(values: &[ScalarValue]) -> bool {
    values.iter().all(|value| match value {
        ScalarValue::Float32(Some(value)) => !value.is_nan(),
        ScalarValue::Float64(Some(value)) => !value.is_nan(),
        ScalarValue::Null
        | ScalarValue::Boolean(_)
        | ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::Utf8(_)
        | ScalarValue::Date32(_)
        | ScalarValue::Time32Millisecond(_)
        | ScalarValue::Decimal128(..) => true,
        value => record_equal(value, value),
    })
}

pub(crate) fn canonical_array(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float32 => Arc::new(
            array
                .as_primitive::<Float32Type>()
                .unary::<_, Float32Type>(canonical_f32),
        ),
        DataType::Float64 => Arc::new(
            array
                .as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(canonical_f64),
        ),
        _ => array.clone(),
    }
}
