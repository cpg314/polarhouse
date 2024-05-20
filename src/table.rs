use std::collections::HashSet;

use futures::stream::{self, TryStreamExt};
use itertools::Itertools;
use klickhouse::IndexMap;
use polars::prelude::*;
use tracing::*;

use super::{structs, ClickhouseType, Error};
use crate::p2c::BlockIntoIterator;

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
        crate::get_df_query(
            query,
            crate::GetOptions {
                types: self.types.clone(),
                ..Default::default()
            },
            client,
        )
        .await
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
    /// Create blocks to send to Clickhouse from a DataFrame.
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
