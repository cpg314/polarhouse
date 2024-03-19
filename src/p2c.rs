//! Polars to Clickhouse conversions
use std::collections::HashSet;

use futures::stream::{self, TryStreamExt};
use itertools::Itertools;
use klickhouse::IndexMap;
use polars::prelude::*;
use tracing::*;

use super::{structs, ClickhouseType, Error};

pub type ValueMap = IndexMap<String, klickhouse::Value>;

#[derive(derivative::Derivative)]
#[derivative(Debug)]
#[derivative(PartialEq)]
/// Clickhouse table schema.
pub struct ClickhouseTable {
    pub name: String,
    pub types: IndexMap<String, ClickhouseType>,
}

#[derive(Default)]
pub struct TableCreationOptions<'a> {
    pub primary_keys: &'a [&'a str],
    pub suffix: &'a str,
    pub if_not_exists: bool,
}

impl ClickhouseTable {
    pub fn types_all(&self) -> String {
        self.types
            .iter()
            .map(|(name, type_)| format!("  `{}` {},", name, type_))
            .join("\n")
    }
    /// Retrieve the table schema from the Clickhouse server.
    ///
    /// The output can be passed to [get_df_query](crate::get_df_query) to get an exact mapping of types.
    /// Indeed, Clickhouse returns for example booleans as the internal storage type ([u8]).
    pub async fn from_server(table: &str, client: &klickhouse::Client) -> Result<Self, Error> {
        debug!(table, "Retrieving table information");
        #[derive(klickhouse::Row, Debug)]
        struct SchemaRow {
            name: String,
            #[klickhouse(rename = "type")]
            type_: String,
        }
        Ok(Self {
            name: table.into(),
            types: client
                .query_collect::<SchemaRow>(format!("DESCRIBE TABLE {}", table))
                .await?
                .into_iter()
                .map(|row| {
                    row.type_
                        .parse::<ClickhouseType>()
                        .map(|type_| (row.name, type_))
                })
                .try_collect()?,
        })
    }
    pub async fn get_df_query(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
        client: &klickhouse::Client,
    ) -> Result<DataFrame, Error> {
        crate::get_df_query(query, self.types.clone(), client).await
    }
    /// Deduce the table schema from a polars schema (e.g. from [DataFrame::schema]).
    /// The primary keys must be provided.
    pub fn from_polars_schema<T: Into<String>>(
        name: &str,
        schema: Schema,
        defaults: IndexMap<String, ClickhouseType>,
        nullables: impl IntoIterator<Item = T>,
    ) -> Result<Self, Error> {
        debug!(name, "Decoding table from schema");
        let nullables: HashSet<String> = nullables.into_iter().map(|col| col.into()).collect();

        let schema = structs::flatten_schema(&schema)?;

        let cols: IndexMap<_, _> = schema
            .into_iter()
            .map(|(col, type_)| -> Result<_, Error> {
                Ok((col.to_string(), ClickhouseType::try_from(&type_)?))
            })
            .chain(defaults.into_iter().map(Ok))
            .map(|res| match res {
                Ok((col, mut type_)) => {
                    if nullables.contains(col.as_str())
                        || nullables
                            .iter()
                            .any(|n| col.starts_with(&format!("{}.", n)))
                    {
                        type_ = type_.nullable();
                    }
                    Ok((col, type_))
                }
                Err(e) => Err(e),
            })
            .try_collect()?;

        Ok(Self {
            name: name.to_string(),
            types: cols,
        })
    }
    pub fn create_query(&self, options: TableCreationOptions<'_>) -> Result<String, Error> {
        let primary_keys: Vec<String> =
            options.primary_keys.iter().map(|x| x.to_string()).collect();
        for key in &primary_keys {
            if !self.types.contains_key(key) {
                return Err(Error::InvalidPrimaryKey(key.into()));
            }
        }
        Ok(format!(
            "CREATE TABLE {} `{}` (
{}
)
ENGINE = MergeTree()
PRIMARY KEY({})
",
            if options.if_not_exists {
                "IF NOT EXISTS"
            } else {
                ""
            },
            self.name,
            self.types_all(),
            primary_keys.join(", "),
        ))
    }
    /// Create the corresponding table.
    pub async fn create<'a>(
        &self,
        options: TableCreationOptions<'a>,
        client: &klickhouse::Client,
    ) -> Result<(), Error> {
        debug!(self.name, "Creating table");
        let suffix = options.suffix.to_string();
        Ok(client
            .execute([self.create_query(options)?, suffix].join("\n"))
            .await?)
    }
    /// Insert a [DataFrame] in Clickhouse.
    /// The schemas must match.
    /// The [defaults] argument specifies constant values for columns present in the table but not
    /// in the dataframe.
    pub async fn insert_df(
        &self,
        df: DataFrame,
        defaults: ValueMap,
        client: &klickhouse::Client,
    ) -> Result<(), Error> {
        debug!(self.name, shape = ?df.shape(), "Inserting dataframe",);
        let df = structs::flatten(df)?;
        if df.should_rechunk() {
            return Err(Error::ShouldRechunk);
        }
        let blocks = self.blocks_from_df(df, &defaults)?;

        let query = format!("INSERT INTO `{}` FORMAT native", self.name);
        for mut block in blocks.try_into_iter()? {
            debug!(rows = block.rows, "Inserting block");
            for (k, v) in defaults.clone() {
                block
                    .column_data
                    .entry(k)
                    .or_insert_with(|| std::iter::repeat(v).take(block.rows as usize).collect());
            }
            client
                .insert_native_raw(query.clone(), stream::iter([block]))
                .await?
                .try_collect::<Vec<_>>()
                .await?;
        }
        debug!(self.name, "Finished inserting dataframe");
        Ok(())
    }
    fn blocks_from_df(
        &self,
        df: DataFrame,
        defaults: &ValueMap,
    ) -> Result<BlockIntoIterator, Error> {
        let mut df_cols: HashSet<_> = df.get_column_names().into_iter().collect();
        let table_cols: HashSet<_> = self.types.keys().map(String::as_str).collect();

        if !df_cols.is_subset(&table_cols) {
            return Err(Error::MismatchingColumns(format!(
                "{:?} are not table columns",
                df_cols.difference(&table_cols)
            )));
        }
        df_cols.extend(defaults.keys().map(|x| x.as_str()));
        if df_cols != table_cols {
            return Err(Error::MismatchingColumns(format!(
                "Missing table columns: {:?}",
                table_cols.difference(&df_cols)
            )));
        }
        Ok(BlockIntoIterator {
            df,
            cols: self.types.clone(),
        })
    }
}
struct BlockIterator<'a> {
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
struct BlockIntoIterator {
    df: DataFrame,
    cols: IndexMap<String, ClickhouseType>,
}
impl BlockIntoIterator {
    fn try_into_iter(&self) -> Result<BlockIterator, Error> {
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
