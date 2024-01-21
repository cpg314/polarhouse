//! Polars to Clickhouse conversions

use polars::prelude::*;

use super::{ClickhouseType, Error};

impl TryFrom<&DataType> for ClickhouseType {
    type Error = Error;
    fn try_from(source: &DataType) -> Result<Self, Self::Error> {
        Ok(match source {
            DataType::String => Self::Native(klickhouse::Type::String),

            DataType::UInt8 => Self::Native(klickhouse::Type::UInt8),
            DataType::UInt16 => Self::Native(klickhouse::Type::UInt16),
            DataType::UInt32 => Self::Native(klickhouse::Type::UInt32),
            DataType::UInt64 => Self::Native(klickhouse::Type::UInt64),

            DataType::Int8 => Self::Native(klickhouse::Type::Int8),
            DataType::Int16 => Self::Native(klickhouse::Type::Int16),
            DataType::Int32 => Self::Native(klickhouse::Type::Int32),
            DataType::Int64 => Self::Native(klickhouse::Type::Int64),

            DataType::Float32 => Self::Native(klickhouse::Type::Float32),
            DataType::Float64 => Self::Native(klickhouse::Type::Float64),

            DataType::Boolean => Self::Bool,

            DataType::Categorical(_, _) => Self::Native(klickhouse::Type::LowCardinality(
                Box::new(klickhouse::Type::String),
            )),
            _ => return Err(Error::UnsupportedPolarsType(source.clone())),
        })
    }
}

macro_rules! extract_vals {
    ($series:ident, $t: ident, $f: ident) => {
        Box::new(
            $series
                .$f()
                .map_err(|_| Error::MismatchingSeriesType($series.dtype().clone()))?
                .into_iter()
                .map(|x| klickhouse::Value::$t(x.expect("Nullable are not yet supported").into())),
        )
    };
}
/// Convert a polars [Series] into an iterator of [klickhouse::Value].
pub(crate) fn series_to_values<'a>(
    series: &'a Series,
    type_: &ClickhouseType,
) -> Result<Box<dyn Iterator<Item = klickhouse::Value> + 'a>, Error> {
    Ok(match type_ {
        ClickhouseType::Native(klickhouse::Type::String) => {
            extract_vals!(series, String, str)
        }

        ClickhouseType::Native(klickhouse::Type::UInt8) => {
            extract_vals!(series, UInt8, u8)
        }
        ClickhouseType::Native(klickhouse::Type::UInt16) => {
            extract_vals!(series, UInt16, u16)
        }
        ClickhouseType::Native(klickhouse::Type::UInt32) => {
            extract_vals!(series, UInt32, u32)
        }
        ClickhouseType::Native(klickhouse::Type::UInt64) => {
            extract_vals!(series, UInt64, u64)
        }

        ClickhouseType::Native(klickhouse::Type::Int8) => extract_vals!(series, Int8, i8),
        ClickhouseType::Native(klickhouse::Type::Int16) => {
            extract_vals!(series, Int16, i16)
        }
        ClickhouseType::Native(klickhouse::Type::Int32) => {
            extract_vals!(series, Int32, i32)
        }
        ClickhouseType::Native(klickhouse::Type::Int64) => {
            extract_vals!(series, Int64, i64)
        }

        ClickhouseType::Native(klickhouse::Type::Float32) => {
            extract_vals!(series, Float32, f32)
        }
        ClickhouseType::Native(klickhouse::Type::Float64) => {
            extract_vals!(series, Float64, f64)
        }

        ClickhouseType::Bool => extract_vals!(series, UInt8, bool),

        ClickhouseType::Native(klickhouse::Type::LowCardinality(s))
            if s == &Box::new(klickhouse::Type::String) =>
        {
            Box::new(
                series
                    .categorical()
                    .unwrap()
                    .iter_str()
                    .map(|x| klickhouse::Value::String(x.unwrap().into())),
            )
        }
        _ => todo!("{:?}", type_),
    })
}
