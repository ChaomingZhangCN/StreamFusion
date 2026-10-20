use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn fields(ty: &DataType) -> Result<&Fields> {
    match ty {
        DataType::Map(entry, _) | DataType::List(entry) => match entry.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => Ok(fields),
            _ => exec_err!("MAP requires key/value entries"),
        },
        _ => exec_err!("MAP requires an Arrow map or verified map-list"),
    }
}

pub(super) fn parts(
    array: &ArrayRef,
) -> Result<(&StructArray, &OffsetBuffer<i32>, Option<&NullBuffer>)> {
    if let Some(map) = array.as_any().downcast_ref::<MapArray>() {
        return Ok((map.entries(), map.offsets(), map.nulls()));
    }
    if let Some(map) = array.as_any().downcast_ref::<ListArray>() {
        if let Some(entries) = map.values().as_any().downcast_ref::<StructArray>() {
            return Ok((entries, map.offsets(), map.nulls()));
        }
    }
    exec_err!("MAP requires key/value entries")
}

pub(super) fn function(index: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(MapProjection {
        index,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MapProjection {
    index: usize,
    signature: Signature,
}
impl ScalarUDFImpl for MapProjection {
    fn name(&self) -> &str {
        if self.index == 0 {
            "flink_map_keys"
        } else {
            "flink_map_values"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        Ok(DataType::new_list(
            fields(&types[0])?[self.index].data_type().clone(),
            true,
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let index = self.index;
        datafusion::functions::utils::make_scalar_function(move |args| project(args, index), vec![])(
            &args.args,
        )
    }
}
fn project(args: &[ArrayRef], index: usize) -> Result<ArrayRef> {
    let (entries, offsets, nulls) = parts(&args[0])?;
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new(
            "item",
            entries.column(index).data_type().clone(),
            true,
        )),
        offsets.clone(),
        entries.column(index).clone(),
        nulls.cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    #[test]
    fn keys_keep_null_duplicates_and_share_sliced_buffers() {
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![Some(9), None, Some(1), Some(1)]));
        let map = super::super::map_arrays::build(
            keys.clone(),
            keys.clone(),
            OffsetBuffer::new(vec![0, 1, 4, 4, 4].into()),
            Some(NullBuffer::from(vec![true, true, false, true])),
        )
        .unwrap();
        let out = super::project(&[map.slice(1, 3)], 0).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.value_offsets(), &[1, 4, 4, 4]);
        assert!(out.is_null(1));
        let values = super::project(&[map.slice(1, 3)], 1).unwrap();
        assert_eq!(out as &dyn Array, values.as_ref());
        assert_eq!(
            out.values().to_data().buffers()[0].as_ptr(),
            keys.to_data().buffers()[0].as_ptr()
        );
        assert_eq!(
            out.value(0).as_ref(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
    }
}
