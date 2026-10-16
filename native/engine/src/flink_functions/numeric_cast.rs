use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Float32Array, Float64Array, StringArray};
use arrow::datatypes::DataType;

/// Only batches whose complete result is independent of host formatting/parsing take this path.
/// All other batches keep the released Flink expression, including its failures and row order.
pub(crate) fn evaluate(arrays: &[ArrayRef], target: &DataType) -> Option<ArrayRef> {
    let [input] = arrays else { return None };
    match target {
        DataType::Decimal128(precision, scale) if (0..=18).contains(scale) => {
            let factor = 10_u64.pow(*scale as u32) as f64;
            let limit = 10_i128.pow(*precision as u32);
            let convert = |value| decimal(value, factor).map(|v| (v.abs() < limit).then_some(v));
            let values: Option<Vec<Option<i128>>> = match input.data_type() {
                DataType::Float32 => input
                    .as_any()
                    .downcast_ref::<Float32Array>()?
                    .iter()
                    .map(|value| value.map(|v| convert(f64::from(v))).unwrap_or(Some(None)))
                    .collect(),
                DataType::Float64 => input
                    .as_any()
                    .downcast_ref::<Float64Array>()?
                    .iter()
                    .map(|value| value.map(convert).unwrap_or(Some(None)))
                    .collect(),
                _ => return None,
            };
            Some(Arc::new(
                Decimal128Array::from(values?)
                    .with_precision_and_scale(*precision, *scale)
                    .ok()?,
            ))
        }
        DataType::Float64 => {
            let values = input.as_any().downcast_ref::<StringArray>()?;
            let values: Option<Vec<Option<f64>>> = values
                .iter()
                .map(|value| value.map(parse_decimal).transpose())
                .collect::<Result<_, _>>()
                .ok();
            Some(Arc::new(Float64Array::from(values?)))
        }
        _ => None,
    }
}

fn decimal(value: f64, factor: f64) -> Option<i128> {
    if !value.is_finite() {
        return None;
    }
    // BigDecimal.valueOf uses the running JDK's round-tripping Double.toString. Every such
    // decimal lies between the adjacent binary values. Outward-rounded products bound its
    // scaled value; agreement under HALF_UP proves the answer without choosing a renderer.
    // Powers 10^0 through 10^18 are exact, and this bound keeps half-integers representable.
    let lower = (value.next_down() * factor).next_down();
    let upper = (value.next_up() * factor).next_up();
    if lower.abs().max(upper.abs()) >= (1_u64 << 52) as f64 {
        return None;
    }
    let result = lower.round();
    (result == upper.round()).then_some(result as i128)
}

fn parse_decimal(value: &str) -> Result<f64, ()> {
    let value = value.trim_matches(|c: char| c <= '\u{20}');
    // Decimal literals have the same correctly-rounded binary64 result in Java and Rust.
    // Hexadecimal, suffixes, special values, Unicode and malformed text stay with Flink.
    if !value
        .bytes()
        .all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'+' | b'-' | b'e' | b'E'))
    {
        return Err(());
    }
    value.parse().map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn rounding_is_certified_without_selecting_a_jdk_renderer() {
        assert_eq!(decimal(f64::from(1.55_f32), 10.0), Some(15));
        assert_eq!(decimal(f64::from(-1.55_f32), 10.0), Some(-15));
        for value in [1.55, -1.55, 0.05, -0.05, f64::NAN, f64::INFINITY, f64::MAX] {
            assert_eq!(decimal(value, 10.0), None, "{value}");
        }
        for value in [0.0, -0.0, f64::MIN_POSITIVE, f64::from_bits(1)] {
            assert_eq!(decimal(value, 10.0), Some(0));
        }
        for value in [1.2, -1.2, 100.125, -100.125] {
            assert_eq!(decimal(value, 10000.0), Some((value * 10000.0) as i128));
        }
    }

    #[test]
    fn nullable_and_sliced_batches_keep_precision_overflow() {
        let input: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(123.0),
            None,
            Some(1.55),
            Some(-1.55),
            Some(100.0),
        ]));
        let result = evaluate(&[input.slice(1, 4)], &DataType::Decimal128(2, 1)).unwrap();
        assert_eq!(
            result.as_ref(),
            &Decimal128Array::from(vec![None, Some(15), Some(-15), None])
                .with_precision_and_scale(2, 1)
                .unwrap()
        );
    }

    #[test]
    fn one_uncertain_value_retains_the_complete_flink_batch() {
        for value in [1.55, f64::NAN, f64::INFINITY, 1e30] {
            let input =
                Arc::new(Float64Array::from(vec![Some(1.2), None, Some(value)])) as ArrayRef;
            assert!(evaluate(&[input], &DataType::Decimal128(10, 1)).is_none());
        }
        let input = Arc::new(Float64Array::from(vec![1.2])) as ArrayRef;
        assert!(evaluate(&[input], &DataType::Decimal128(38, 19)).is_none());
    }

    #[test]
    fn decimal_text_preserves_signed_zero_and_uses_flink_for_other_forms() {
        let input = Arc::new(StringArray::from(vec![
            Some("ignored"),
            None,
            Some("  +1.25\t"),
            Some("-0.0"),
            Some("1e999"),
            Some("-1e-999"),
        ])) as ArrayRef;
        let output = evaluate(&[input.slice(1, 5)], &DataType::Float64).unwrap();
        let output = output.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(output.is_null(0));
        assert_eq!(output.value(1), 1.25);
        assert_eq!(output.value(2).to_bits(), (-0.0_f64).to_bits());
        assert_eq!(output.value(3), f64::INFINITY);
        assert_eq!(output.value(4).to_bits(), (-0.0_f64).to_bits());
        for value in [
            "", " ", "1_2", "bad", "NaN", "Infinity", "0x1.0p2", "1.0f", "\u{a0}1",
        ] {
            assert!(parse_decimal(value).is_err(), "{value}");
        }
    }
}
