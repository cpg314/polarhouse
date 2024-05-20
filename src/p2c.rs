//! Polars to Clickhouse conversions

use itertools::Itertools;
use klickhouse::IndexMap;
use polars::prelude::*;

use super::{ClickhouseType, Error};

pub(crate) struct BlockIterator<'a> {
    info: klickhouse::block::BlockInfo,
    column_types: IndexMap<String, klickhouse::Type>,
    iters:
        IndexMap<String, Box<dyn ExactSizeIterator<Item = klickhouse::Value> + Send + Sync + 'a>>,
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = klickhouse::block::Block;
    fn next(&mut self) -> Option<Self::Item> {
        let column_data: IndexMap<String, Vec<klickhouse::Value>> = self
            .iters
            .iter_mut()
            .map(|(k, it)| (k.clone(), it.take(200_000).collect_vec()))
            .collect();
        let rows = column_data
            .values()
            .map(|v| v.len() as u64)
            .next()
            .unwrap_or_default();
        if rows == 0 {
            return None;
        }
        Some(klickhouse::block::Block {
            info: self.info.clone(),
            rows,
            column_types: self.column_types.clone(),
            column_data,
        })
    }
}
pub(crate) struct BlockIntoIterator {
    pub(crate) df: DataFrame,
    pub(crate) cols: IndexMap<String, ClickhouseType>,
}
impl BlockIntoIterator {
    pub(crate) fn try_into_iter(&self) -> Result<BlockIterator, Error> {
        let info = klickhouse::block::BlockInfo {
            is_overflows: false,
            bucket_num: 0,
        };
        // This covers all columns.
        let column_types: IndexMap<String, klickhouse::Type> = self
            .cols
            .clone()
            .into_iter()
            .map(|(col, type_)| (col, type_.into()))
            .collect();
        // This only contains the columns from the dataframe, not the defaults.
        let iters: IndexMap<String, _> = self
            .df
            .get_columns()
            .iter()
            .map(|col| -> Result<_, Error> {
                let values = series_to_values(col, self.cols.get(col.name()).unwrap().clone())?;
                Ok((col.name().to_string(), values))
            })
            .try_collect()?;
        Ok(BlockIterator {
            info,
            column_types,
            iters,
        })
    }
}

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

            DataType::List(t) => Self::Native(klickhouse::Type::Array(Box::new(
                ClickhouseType::try_from(t.as_ref())?.into(),
            ))),

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
                .map(|x| match x {
                    Some(x) => klickhouse::Value::$t(x.into()),
                    None => klickhouse::Value::Null,
                }),
        )
    };
}
/// Convert a polars [Series] into an iterator of [klickhouse::Value].
pub(crate) fn series_to_values<'a>(
    series: &'a Series,
    type_: ClickhouseType,
) -> Result<Box<dyn ExactSizeIterator<Item = klickhouse::Value> + Send + Sync + 'a>, Error> {
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
            if s.as_ref() == &klickhouse::Type::String =>
        {
            Box::new(
                series
                    .categorical()
                    .unwrap()
                    .iter_str()
                    .map(|x| klickhouse::Value::String(x.unwrap().into())),
            )
        }

        ClickhouseType::Native(klickhouse::Type::Array(type_)) => {
            Box::new(
                series
                    .list()
                    .map_err(|_| Error::MismatchingSeriesType(series.dtype().clone()))?
                    .into_iter()
                    .map(move |v| match v {
                        Some(v) => klickhouse::Value::Array(
                            series_to_values(&v, ClickhouseType::from(*type_.clone()))
                                // TODO: Handle the error without allocating
                                .unwrap()
                                .collect(),
                        ),
                        None => klickhouse::Value::Null,
                    }),
            )
        }

        //Nulls
        ClickhouseType::Native(klickhouse::Type::Nullable(s)) => {
            series_to_values(series, ClickhouseType::from(*s))?
        }
        ClickhouseType::Nullable(type_) => series_to_values(series, *type_)?,

        _ => {
            return Err(Error::UnsupportedClickhouseType(type_.clone()));
        }
    })
}
