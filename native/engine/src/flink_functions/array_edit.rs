use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayRef, ListArray, MutableArrayData};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_list_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(ArrayAppend {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayAppend {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayAppend {
    fn name(&self) -> &str {
        "flink_array_append"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::List(field), element] if field.data_type() == element => {
                Ok(DataType::new_list(element.clone(), true))
            }
            _ => exec_err!("ARRAY_APPEND requires an ARRAY and a matching element"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(append, vec![])(&args.args)
    }
}

fn append(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = as_list_array(&args[0])?;
    let original = array.values().to_data();
    let elements = args[1].to_data();
    let mut values = MutableArrayData::new(
        vec![&original, &elements],
        false,
        array.values().len() + array.len(),
    );
    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(0);
    for row in 0..array.len() {
        if array.is_valid(row) {
            values.extend(
                0,
                array.value_offsets()[row] as usize,
                array.value_offsets()[row + 1] as usize,
            );
            values.extend(1, row, row + 1);
        }
        offsets.push(i32::try_from(values.len()).map_err(|_| {
            datafusion::common::DataFusionError::Execution("ARRAY exceeds Arrow offsets".into())
        })?);
    }
    Ok(Arc::new(ListArray::new(
        Arc::new(arrow::datatypes::Field::new_list_field(
            array.value_type(),
            true,
        )),
        OffsetBuffer::new(offsets.into()),
        make_array(values.freeze()),
        array.nulls().cloned(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Int64Type;

    #[test]
    fn appending_preserves_null_elements_slices_and_null_containers() {
        let input: ArrayRef = Arc::new(
            ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(99)]),
                Some(vec![Some(1), None, Some(1)]),
                None,
                Some(vec![]),
                Some(vec![None]),
            ])
            .slice(1, 4),
        );
        let values: ArrayRef =
            Arc::new(Int64Array::from(vec![None, Some(9), Some(i64::MAX), None]));
        let output = append(&[input, values]).unwrap();
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1), None, Some(1), None]),
            None,
            Some(vec![Some(i64::MAX)]),
            Some(vec![None, None]),
        ]);
        assert_eq!(output.as_ref(), &expected);
    }
}
