use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, MapArray};
use arrow::compute::kernels::length::length;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(Cardinality {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Cardinality {
    signature: Signature,
}

impl ScalarUDFImpl for Cardinality {
    fn name(&self) -> &str {
        "flink_cardinality"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::List(_) | DataType::Map(_, _)] => Ok(DataType::Int32),
            _ => exec_err!("CARDINALITY requires an ARRAY or MAP"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(cardinality, vec![])(&args.args)
    }
}

fn cardinality(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("CARDINALITY requires one argument");
    };
    match input.data_type() {
        DataType::List(_) => Ok(length(input.as_ref())?),
        DataType::Map(_, _) => {
            let map = input.as_any().downcast_ref::<MapArray>().unwrap();
            let sizes = map.offsets().windows(2).map(|pair| pair[1] - pair[0]);
            Ok(Arc::new(Int32Array::new(
                sizes.collect::<Vec<_>>().into(),
                map.nulls().cloned(),
            )))
        }
        _ => exec_err!("CARDINALITY requires an ARRAY or MAP"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};

    #[test]
    fn nested_arrays_count_outer_elements_and_keep_sliced_nulls() {
        let mut lists = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        for row in 0..4 {
            for width in [2, 3] {
                for value in 0..width {
                    lists.values().values().append_value(value);
                }
                lists.values().append(true);
            }
            lists.append(row != 2);
        }
        lists.append(true);
        let input: ArrayRef = Arc::new(lists.finish());
        let result = cardinality(&[input.slice(1, 4)]).unwrap();
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), None, Some(2), Some(0)]));
        assert_eq!(result.as_ref(), expected.as_ref());
        assert_eq!(cardinality(&[input.slice(1, 0)]).unwrap().len(), 0);
    }

    #[test]
    fn maps_count_entries_including_null_values_and_duplicates() {
        let mut maps = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for row in 0..4 {
            for _ in 0..row {
                maps.keys().append_value("duplicate");
                maps.values().append_null();
            }
            maps.append(row != 2).unwrap();
        }
        maps.append(true).unwrap();
        let input: ArrayRef = Arc::new(maps.finish());
        let result = cardinality(&[input.slice(1, 4)]).unwrap();
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(0)]));
        assert_eq!(result.as_ref(), expected.as_ref());
        assert_eq!(cardinality(&[input.slice(0, 0)]).unwrap().len(), 0);
    }
}
