//! Polars to Clickhouse conversions
use std::collections::HashSet;

use futures::stream::{self, TryStreamExt};
use itertools::Itertools;
use klickhouse::IndexMap;
use polars::prelude::*;
use tracing::*;

use super::{structs, ClickhouseType, Error};

#[derive(derivative::Derivative)]
#[derivative(Debug)]
#[derivative(PartialEq)]
/// Clickhouse table schema.
pub struct ClickhouseTable {
    name: String,
    cols: IndexMap<String, ClickhouseType>,
    primary_keys: Vec<String>,
}

impl ClickhouseTable {
    /// Deduce the table schema from a polars schema (e.g. from [DataFrame::schema]).
    /// The primary keys must be provided.
    pub fn from_polars_schema<T: Into<String>>(
        name: &str,
        schema: Schema,
        primary_keys: impl IntoIterator<Item = T>,
    ) -> Result<Self, Error> {
        debug!(name, "Decoding table from schema");
        let schema = structs::flatten_schema(&schema)?;
        let cols: IndexMap<_, _> = schema
            .into_iter()
            .map(|(col, type_)| {
                ClickhouseType::try_from(&type_).map(|type_| (col.to_string(), type_))
            })
            .try_collect()?;

        let primary_keys: Vec<String> = primary_keys.into_iter().map(|x| x.into()).collect();
        for key in &primary_keys {
            if !cols.contains_key(key) {
                return Err(Error::InvalidPrimaryKey(key.into()));
            }
        }
        Ok(Self {
            name: name.to_string(),
            primary_keys,
            cols,
        })
    }
    /// Create the corresponding table.
    pub async fn create(&self, client: &klickhouse::Client) -> Result<(), Error> {
        debug!(self.name, "Creating table");
        let query = format!(
            "CREATE TABLE {} (
{}
)
ENGINE = MergeTree()
PRIMARY KEY({})
",
            self.name,
            self.cols
                .iter()
                .map(|(name, type_)| format!("  `{}` {},", name, type_))
                .join("\n"),
            self.primary_keys.join(", ")
        );
        Ok(client.execute(query).await?)
    }
    /// Insert a [DataFrame] in Clickhouse. The schema must match.
    // TODO: Chunk the insert
    pub async fn insert_df(&self, df: DataFrame, client: &klickhouse::Client) -> Result<(), Error> {
        debug!(self.name, shape = ?df.shape(), "Inserting dataframe",);
        let df = structs::flatten(df)?;
        if df.should_rechunk() {
            return Err(Error::ShouldRechunk);
        }
        let block = self.block_from_df(df)?;
        client
            .insert_native_raw(
                format!("INSERT INTO `{}` FORMAT native", self.name),
                stream::iter([block]),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
    fn block_from_df(&self, df: DataFrame) -> Result<klickhouse::block::Block, Error> {
        let df_cols: HashSet<_> = df.get_column_names().into_iter().collect();
        let table_cols: HashSet<_> = self.cols.keys().map(String::as_str).collect();
        if df_cols != table_cols {
            return Err(Error::MismatchingColumns(format!(
                "{:?}",
                table_cols.symmetric_difference(&df_cols)
            )));
        }
        let iters: IndexMap<&String, _> = self
            .cols
            .iter()
            .map(|(col, type_)| {
                series_to_values(df.column(col).unwrap(), type_).map(|vals| (col, vals))
            })
            .try_collect()?;
        let block = klickhouse::block::Block {
            info: klickhouse::block::BlockInfo {
                is_overflows: false,
                bucket_num: 0,
            },
            rows: df.shape().0 as u64,
            column_types: self
                .cols
                .clone()
                .into_iter()
                .map(|(col, type_)| (col, type_.into()))
                .collect(),
            column_data: iters
                .into_iter()
                .map(|(col, it)| (col.clone(), it.collect_vec()))
                .collect(),
        };
        Ok(block)
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
