#![doc = include_str!("../README.md")]

mod c2p;
mod errors;
pub use errors::*;
mod p2c;

use std::collections::HashSet;
use std::str::FromStr;

use futures::stream::{self, TryStreamExt};
use itertools::Itertools;
use klickhouse::IndexMap;
use polars::prelude::*;
use tracing::*;

#[derive(Clone, Debug, PartialEq)]
pub enum ClickhouseType {
    Native(klickhouse::Type),
    Bool,
}
impl FromStr for ClickhouseType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "Bool" {
            return Ok(Self::Bool);
        }
        Ok(Self::Native(klickhouse::Type::from_str(s)?))
    }
}

impl From<ClickhouseType> for klickhouse::Type {
    fn from(source: ClickhouseType) -> Self {
        match source {
            ClickhouseType::Native(n) => n,
            ClickhouseType::Bool => klickhouse::Type::UInt8,
        }
    }
}

impl std::fmt::Display for ClickhouseType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ClickhouseType::Native(n) => write!(f, "{}", n),
            ClickhouseType::Bool => write!(f, "Bool"),
        }
    }
}

#[derive(derivative::Derivative)]
#[derivative(Debug)]
#[derivative(PartialEq)]
/// Clickhouse table schema.
pub struct ClickhouseTable {
    name: String,
    cols: IndexMap<String, ClickhouseType>,
    primary_keys: Vec<String>,
    #[derivative(Debug = "ignore", PartialEq = "ignore")]
    client: Option<klickhouse::Client>,
}
impl ClickhouseTable {
    /// Add a [klickhouse::Client].
    pub fn with_client(self, client: klickhouse::Client) -> Self {
        Self {
            client: Some(client),
            ..self
        }
    }
    /// Deduce the table schema from a polars schema (e.g. from [DataFrame::schema]).
    /// The primary keys must be provided.
    pub fn from_polars_schema<T: Into<String>>(
        name: &str,
        schema: Schema,
        primary_keys: impl IntoIterator<Item = T>,
    ) -> Result<Self, Error> {
        debug!(name, "Decuding table from schema");
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
            client: None,
        })
    }
    /// Retrieve the table schema from the Clickhouse server.
    pub async fn from_table(table: &str, client: klickhouse::Client) -> Result<Self, Error> {
        debug!(table, "Retrieving table information");
        #[derive(klickhouse::Row, Debug)]
        struct SchemaRow {
            name: String,
            #[klickhouse(rename = "type")]
            type_: String,
        }
        let cols = client
            .query_collect::<SchemaRow>(format!("DESCRIBE TABLE {}", table))
            .await?
            .into_iter()
            .map(|row| {
                row.type_
                    .parse::<ClickhouseType>()
                    .map(|type_| (row.name, type_))
            })
            .try_collect()?;

        let klickhouse::UnitValue::<String>(primary_keys) = client
            .query_one(
                klickhouse::SelectBuilder::new("system.tables")
                    .select("primary_key")
                    .where_(klickhouse::QueryBuilder::new("name=$1").arg(table)),
            )
            .await?;

        Ok(Self {
            cols,
            name: table.into(),
            primary_keys: primary_keys.split(", ").map(String::from).collect(),
            client: Some(client),
        })
    }
    fn client(&self) -> Result<&klickhouse::Client, Error> {
        self.client.as_ref().ok_or_else(|| Error::MissingClient)
    }
    /// Create the corresponding table.
    pub async fn create(&self) -> Result<(), Error> {
        debug!(self.name, "Creating table");
        let client = self.client()?;
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
                .map(|(name, type_)| format!("  {} {},", name, type_))
                .join("\n"),
            self.primary_keys.join(", ")
        );
        Ok(client.execute(query).await?)
    }
    /// Insert a [DataFrame] in Clickhouse. The schema must match.
    // TODO: Chunk the insert
    pub async fn insert_df(&self, df: &DataFrame) -> Result<(), Error> {
        debug!(self.name, shape = ?df.shape(), "Inserting dataframe",);
        if df.should_rechunk() {
            return Err(Error::ShouldRechunk);
        }
        let block = self.block_from_df(df)?;
        let client = self.client()?;
        client
            .insert_native_raw("INSERT INTO test FORMAT native", stream::iter([block]))
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
    /// Retrieve this table as a [DataFrame]
    pub async fn get_df(&self) -> Result<DataFrame, Error> {
        let query = klickhouse::SelectBuilder::new(&self.name).select("*");
        self.get_df_query(query).await
    }
    /// Retrieve part of this table as a [DataFrame], using a [klickhouse::SelectBuilder].
    pub async fn get_df_query(&self, query: klickhouse::SelectBuilder) -> Result<DataFrame, Error> {
        debug!(self.name, "Retrieving dataframe from Clickhouse",);
        let client = self.client()?;
        let mut series: IndexMap<&String, Series> = self
            .cols
            .iter()
            .map(|(col, type_)| -> Result<_, Error> {
                let type_ = DataType::try_from(type_)?;
                Ok((col, Series::new_empty(col, &type_)))
            })
            .try_collect()?;
        client
            .query_raw(query)
            .await?
            .map_err(Error::from)
            .try_filter(|block| futures::future::ready(block.rows > 0))
            .and_then(|block| {
                futures::future::ready(
                    block
                        .column_data
                        .into_iter()
                        .map(|(col, values)| {
                            let series = series
                                .get_mut(&col)
                                .ok_or_else(|| Error::MissingColumnLocal(col.clone()))?;
                            series.extend(&c2p::values_to_series(
                                values,
                                self.cols.get(&col).unwrap().clone(),
                            )?)?;
                            Ok(())
                        })
                        .collect::<Result<Vec<_>, Error>>(),
                )
            })
            .try_collect::<Vec<_>>()
            .await?;
        // Remove 0-length series that were not selected
        series.retain(|_, vals| !vals.is_empty());

        let lengths: HashSet<usize> = series.values().map(|s| s.len()).collect();
        if lengths.len() != 1 {
            return Err(Error::MismatchingLengths(lengths));
        }

        Ok(series.into_values().collect())
    }
    fn block_from_df(&self, df: &DataFrame) -> Result<klickhouse::block::Block, Error> {
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
                p2c::series_to_values(df.column(col).unwrap(), type_).map(|vals| (col, vals))
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
