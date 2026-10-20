use super::*;

// Build result and membership columns without retaining a scalar allocation for every row.
pub(super) enum ScalarValues {
    I64(arrow::array::Int64Builder),
    I32(arrow::array::Int32Builder),
    I16(arrow::array::Int16Builder),
    I8(arrow::array::Int8Builder),
    F32(arrow::array::Float32Builder),
    F64(arrow::array::Float64Builder),
    Date(arrow::array::Date32Builder),
    Time(arrow::array::Time32MillisecondBuilder),
    Decimal(arrow::array::Decimal128Builder),
    Boolean(arrow::array::BooleanBuilder),
    Timestamp(streamfusion_bridge::timestamp::TimestampBuilder),
    String(arrow::array::StringBuilder),
    Scalar(Vec<ScalarValue>, DataType),
}

impl ScalarValues {
    pub(super) fn new(ty: &DataType) -> Self {
        match ty {
            DataType::Int64 => Self::I64(Default::default()),
            DataType::Int32 => Self::I32(Default::default()),
            DataType::Int16 => Self::I16(Default::default()),
            DataType::Int8 => Self::I8(Default::default()),
            DataType::Float32 => Self::F32(Default::default()),
            DataType::Float64 => Self::F64(Default::default()),
            DataType::Date32 => Self::Date(Default::default()),
            DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
                Self::Time(Default::default())
            }
            DataType::Decimal128(_, _) => {
                Self::Decimal(arrow::array::Decimal128Builder::new().with_data_type(ty.clone()))
            }
            DataType::Boolean => Self::Boolean(Default::default()),
            DataType::Utf8 => Self::String(Default::default()),
            ty if streamfusion_bridge::timestamp::is_component_timestamp(ty) => {
                Self::Timestamp(streamfusion_bridge::timestamp::TimestampBuilder::with_capacity(0))
            }
            _ => Self::Scalar(Vec::new(), ty.clone()),
        }
    }

    pub(super) fn push(&mut self, scalar: ScalarValue) {
        if let Self::Scalar(values, _) = self {
            values.push(scalar);
        } else {
            self.push_ref(&scalar);
        }
    }

    pub(super) fn push_ref(&mut self, scalar: &ScalarValue) {
        match (self, scalar) {
            (Self::I64(b), ScalarValue::Int64(v)) => b.append_option(*v),
            (Self::I32(b), ScalarValue::Int32(v)) => b.append_option(*v),
            (Self::I16(b), ScalarValue::Int16(v)) => b.append_option(*v),
            (Self::I8(b), ScalarValue::Int8(v)) => b.append_option(*v),
            (Self::F32(b), ScalarValue::Float32(v)) => b.append_option(*v),
            (Self::F64(b), ScalarValue::Float64(v)) => b.append_option(*v),
            (Self::Date(b), ScalarValue::Date32(v)) => b.append_option(*v),
            (Self::Time(b), ScalarValue::Time32Millisecond(v)) => b.append_option(*v),
            (Self::Decimal(b), ScalarValue::Decimal128(v, _, _)) => b.append_option(*v),
            (Self::Boolean(b), ScalarValue::Boolean(v)) => b.append_option(*v),
            (Self::String(b), ScalarValue::Utf8(v)) => b.append_option(v.as_deref()),
            (Self::Timestamp(b), ScalarValue::Struct(a)) => {
                let column = streamfusion_bridge::timestamp::TimestampColumn::try_new(a.as_ref())
                    .expect("timestamp output");
                if column.is_null(0) {
                    b.append_null();
                } else {
                    b.append_value(column.value(0).expect("timestamp value"));
                }
            }
            (Self::Scalar(values, _), scalar) => values.push(scalar.clone()),
            (builder, scalar) => {
                // Compact planner types may promote to a generic runtime representation.
                let previous =
                    std::mem::replace(builder, Self::Scalar(Vec::new(), DataType::Null)).finish();
                let mut values: Vec<_> = (0..previous.len())
                    .map(|row| ScalarValue::try_from_array(&previous, row).expect("previous value"))
                    .collect();
                values.push(scalar.clone());
                *builder = Self::Scalar(values, previous.data_type().clone());
            }
        }
    }

    pub(super) fn finish(self) -> ArrayRef {
        match self {
            Self::I64(mut b) => Arc::new(b.finish()),
            Self::I32(mut b) => Arc::new(b.finish()),
            Self::I16(mut b) => Arc::new(b.finish()),
            Self::I8(mut b) => Arc::new(b.finish()),
            Self::F32(mut b) => Arc::new(b.finish()),
            Self::F64(mut b) => Arc::new(b.finish()),
            Self::Date(mut b) => Arc::new(b.finish()),
            Self::Time(mut b) => Arc::new(b.finish()),
            Self::Decimal(mut b) => Arc::new(b.finish()),
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::String(mut b) => Arc::new(b.finish()),
            Self::Timestamp(mut b) => Arc::new(b.finish()),
            Self::Scalar(values, ty) => scalars_to_array(values, &ty),
        }
    }
}
