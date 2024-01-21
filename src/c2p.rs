//! Clickhouse to Polars conversions
use polars::prelude::*;

use super::{ClickhouseType, Error};

impl TryFrom<&ClickhouseType> for DataType {
    type Error = Error;
    fn try_from(source: &ClickhouseType) -> Result<Self, Self::Error> {
        Ok(match source {
            ClickhouseType::Native(klickhouse::Type::String) => DataType::String,

            ClickhouseType::Native(klickhouse::Type::UInt8) => DataType::UInt8,
            ClickhouseType::Native(klickhouse::Type::UInt16) => DataType::UInt16,
            ClickhouseType::Native(klickhouse::Type::UInt32) => DataType::UInt32,
            ClickhouseType::Native(klickhouse::Type::UInt64) => DataType::UInt64,

            ClickhouseType::Native(klickhouse::Type::Int8) => DataType::Int8,
            ClickhouseType::Native(klickhouse::Type::Int16) => DataType::Int16,
            ClickhouseType::Native(klickhouse::Type::Int32) => DataType::Int32,
            ClickhouseType::Native(klickhouse::Type::Int64) => DataType::Int64,

            ClickhouseType::Native(klickhouse::Type::Float32) => DataType::Float32,
            ClickhouseType::Native(klickhouse::Type::Float64) => DataType::Float64,

            ClickhouseType::Bool => DataType::Boolean,

            ClickhouseType::Native(klickhouse::Type::LowCardinality(s))
                if s == &Box::new(klickhouse::Type::String) =>
            {
                DataType::Categorical(None, CategoricalOrdering::Physical)
            }
            _ => return Err(Error::UnsupportedClickhouseType(source.clone())),
        })
    }
}

macro_rules! extract {
    ($values: ident, $t:ident) => {
        $values
            .into_iter()
            .map(|val| {
                let klickhouse::Value::$t(val) = val else {
                    unreachable!()
                };

                val
            })
            .collect()
    };
}
pub(crate) fn values_to_series(
    values: Vec<klickhouse::Value>,
    type_: ClickhouseType,
) -> Result<Series, Error> {
    let mut type_k = klickhouse::Type::from(type_.clone());
    if type_k == klickhouse::Type::LowCardinality(Box::new(klickhouse::Type::String)) {
        type_k = klickhouse::Type::String;
    }
    for val in &values {
        let type_ = val.guess_type();
        if type_ != type_k {
            return Err(Error::MismatchingValueType(type_, type_k));
        }
    }

    let extract_string = |values: Vec<klickhouse::Value>| -> Series {
        values
            .into_iter()
            .map(|val| {
                let klickhouse::Value::String(val) = val else {
                    unreachable!()
                };
                String::from_utf8_lossy(&val).to_string()
            })
            .collect()
    };

    let series = match type_ {
        ClickhouseType::Native(klickhouse::Type::String) => extract_string(values),

        ClickhouseType::Bool => values
            .into_iter()
            .map(|val| {
                let klickhouse::Value::UInt8(val) = val else {
                    unreachable!()
                };
                val > 0
            })
            .collect(),

        ClickhouseType::Native(klickhouse::Type::UInt8) => extract!(values, UInt8),
        ClickhouseType::Native(klickhouse::Type::UInt16) => extract!(values, UInt16),
        ClickhouseType::Native(klickhouse::Type::UInt32) => extract!(values, UInt32),
        ClickhouseType::Native(klickhouse::Type::UInt64) => extract!(values, UInt64),

        ClickhouseType::Native(klickhouse::Type::Int8) => extract!(values, Int8),
        ClickhouseType::Native(klickhouse::Type::Int16) => extract!(values, Int16),
        ClickhouseType::Native(klickhouse::Type::Int32) => extract!(values, Int32),

        ClickhouseType::Native(klickhouse::Type::Float32) => extract!(values, Float32),
        ClickhouseType::Native(klickhouse::Type::Float64) => extract!(values, Float64),

        ClickhouseType::Native(klickhouse::Type::LowCardinality(s))
            if *s == klickhouse::Type::String =>
        {
            extract_string(values).cast(&DataType::Categorical(None, Default::default()))?
        }
        _ => {
            return Err(Error::UnsupportedClickhouseType(type_));
        }
    };
    Ok(series)
}
