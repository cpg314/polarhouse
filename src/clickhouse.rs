use itertools::Itertools;
use klickhouse::IndexMap;
use tracing::*;

use crate::{ClickhouseType, Error};

/// Retrieve the table schema from the Clickhouse server.
///
/// The output can be passed to [get_df_query](crate::get_df_query) to get an exact mapping of types. Indeed, Clickhouse returns for example booleans asthe internal storage type ([u8]).
pub async fn table_types(
    table: &str,
    client: &klickhouse::Client,
) -> Result<IndexMap<String, ClickhouseType>, Error> {
    debug!(table, "Retrieving table information");
    #[derive(klickhouse::Row, Debug)]
    struct SchemaRow {
        name: String,
        #[klickhouse(rename = "type")]
        type_: String,
    }
    client
        .query_collect::<SchemaRow>(format!("DESCRIBE TABLE {}", table))
        .await?
        .into_iter()
        .map(|row| {
            row.type_
                .parse::<ClickhouseType>()
                .map(|type_| (row.name, type_))
        })
        .try_collect()
}
