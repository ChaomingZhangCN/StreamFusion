use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(TypeOf {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TypeOf {
    signature: Signature,
}

impl ScalarUDFImpl for TypeOf {
    fn name(&self) -> &str {
        "flink_typeof"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [_, DataType::Utf8] => Ok(DataType::Utf8),
            _ => exec_err!("TYPEOF requires an evaluated value and its resolved type string"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Both children have already evaluated, retaining errors and volatile/UDF calls.
        Ok(args.args[1].clone())
    }
}
