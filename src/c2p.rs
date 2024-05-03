//! Clickhouse to Polars conversions

use std::collections::HashSet;

use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use klickhouse::IndexMap;
use polars::prelude::*;
use tracing::*;

use super::{structs, ClickhouseType, Error};

async fn get_df_stream(
    resp: impl Stream<Item = Result<klickhouse::block::Block, Error>>,
    ch_types: IndexMap<String, ClickhouseType>,
) -> Result<DataFrame, Error> {
    debug!(?ch_types, "Building dataframe from stream");
    let mut series: IndexMap<String, Series> = ch_types
        .iter()
        .map(|(col, type_)| -> Result<_, Error> {
            let type_ = DataType::try_from(type_)?;
            let series = Series::new_empty(col, &type_);
            Ok((col.clone(), series))
        })
        .try_collect()?;
    resp.and_then(|block| {
        futures::future::ready(
            block
                .column_data
                .into_iter()
                .map(|(col, values)| {
                    let series = series
                        .get_mut(&col)
                        .ok_or_else(|| Error::MissingColumnLocal(col.clone()))?;
                    series.extend(&values_to_series(
                        values,
                        ch_types.get(&col).unwrap().clone(),
                    )?)?;
                    Ok(())
                })
                .collect::<Result<Vec<_>, Error>>(),
        )
    })
    .try_collect::<Vec<_>>()
    .await?;

    // Remove 0-length series that were present in the `ch_types` but not returned.
    series.retain(|_, vals| !vals.is_empty());

    let lengths: HashSet<usize> = series.values().map(|s| s.len()).collect();
    if lengths.is_empty() {
        return Ok(DataFrame::default());
    }
    if lengths.len() != 1 {
        return Err(Error::MismatchingLengths(lengths));
    }
    Ok(structs::unflatten(series)?.into_values().collect())
}

/// Retrieve Clickhouse query results as a [DataFrame].
///
/// The schema is inferred from the query for columns not present in the `types` argument, which can be used to correct e.g. booleans returned by Clickhouse as their internal [u8] representation.
/// See also the [table_types_from_clickhouse](crate::table_types_from_clickhouse) method.
pub async fn get_df_query(
    query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
    types: IndexMap<String, ClickhouseType>,
    client: &klickhouse::Client,
) -> Result<DataFrame, Error> {
    debug!("Retrieving data from Clickhouse",);

    let mut resp = client.query_raw(query).await?.map_err(Error::from);
    let initial = resp.next().await.ok_or_else(|| {
        klickhouse::KlickhouseError::ProtocolError("Missing initial block".into())
    })??;
    debug!(?initial, "Received initial block");
    let mut ch_types: IndexMap<String, ClickhouseType> = initial
        .column_types
        .into_iter()
        .map(|(col, type_)| -> Result<_, Error> { Ok((col, ClickhouseType::from(type_))) })
        .try_collect()?;
    ch_types.extend(types);

    get_df_stream(resp, ch_types).await
}

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

            ClickhouseType::Native(klickhouse::Type::Uuid) => DataType::String,

            // Lists
            ClickhouseType::Native(klickhouse::Type::Array(inner)) => {
                let inner = ClickhouseType::from(*inner.clone());
                DataType::List(Box::new(DataType::try_from(&inner)?))
            }

            // Categoricals
            ClickhouseType::Native(klickhouse::Type::LowCardinality(s))
                if s == &Box::new(klickhouse::Type::String) =>
            {
                DataType::Categorical(None, CategoricalOrdering::Physical)
            }

            // Nulls
            ClickhouseType::Native(klickhouse::Type::Nullable(s)) => {
                DataType::try_from(&ClickhouseType::from(*s.clone()).nullable())?
            }
            ClickhouseType::Nullable(s) => DataType::try_from(s.as_ref())?,

            _ => return Err(Error::UnsupportedClickhouseType(source.clone())),
        })
    }
}

macro_rules! extract {
    ($values: ident, $t:ident) => {
        extract!($values, $t, (|x| x))
    };
    ($values: ident, $t:ident, $f: expr) => {{
        let f = $f;
        $values
            .into_iter()
            .map(|val| match val {
                klickhouse::Value::$t(val) => Some(f(val)),
                klickhouse::Value::Null => None,
                _ => {
                    unreachable!("expected {}, got {:?}", stringify!($t), val);
                }
            })
            .collect()
    }};
}
pub(crate) fn values_to_series(
    values: Vec<klickhouse::Value>,
    type_: ClickhouseType,
) -> Result<Series, Error> {
    let type_k = klickhouse::Type::from(type_.clone())
        .strip_null()
        .strip_low_cardinality()
        .clone();
    for val in &values {
        if val == &klickhouse::Value::Null {
            continue;
        }
        let type_ = val.guess_type();
        if type_ != type_k {
            return Err(Error::MismatchingValueType(type_, type_k));
        }
    }

    let extract_string = |values: Vec<klickhouse::Value>| -> Series {
        let vals: Vec<_> = extract!(values, String, |val: Vec<u8>| String::from_utf8_lossy(&val)
            .to_string());
        // Series does not implement `FromIterator<Option<String>>`.
        Series::new("", vals)
    };

    let series = match type_ {
        ClickhouseType::Native(klickhouse::Type::String) => extract_string(values),

        ClickhouseType::Bool => extract!(values, UInt8, |val: u8| val > 0),

        ClickhouseType::Native(klickhouse::Type::Uuid) => {
            let vals: Vec<_> = extract!(values, Uuid, |val: klickhouse::Uuid| val.to_string());
            Series::new("", vals)
        }

        ClickhouseType::Native(klickhouse::Type::UInt8) => extract!(values, UInt8),
        ClickhouseType::Native(klickhouse::Type::UInt16) => extract!(values, UInt16),
        ClickhouseType::Native(klickhouse::Type::UInt32) => extract!(values, UInt32),
        ClickhouseType::Native(klickhouse::Type::UInt64) => extract!(values, UInt64),

        ClickhouseType::Native(klickhouse::Type::Int8) => extract!(values, Int8),
        ClickhouseType::Native(klickhouse::Type::Int16) => extract!(values, Int16),
        ClickhouseType::Native(klickhouse::Type::Int32) => extract!(values, Int32),
        ClickhouseType::Native(klickhouse::Type::Int64) => extract!(values, Int64),

        ClickhouseType::Native(klickhouse::Type::Float32) => extract!(values, Float32),
        ClickhouseType::Native(klickhouse::Type::Float64) => extract!(values, Float64),

        ClickhouseType::Native(klickhouse::Type::LowCardinality(s))
            if *s == klickhouse::Type::String =>
        {
            extract_string(values).cast(&DataType::Categorical(None, Default::default()))?
        }

        // Nulls
        ClickhouseType::Nullable(type_) => values_to_series(values, *type_)?,
        ClickhouseType::Native(klickhouse::Type::Nullable(inner)) => {
            values_to_series(values, ClickhouseType::from(*inner))?
        }

        ClickhouseType::Native(klickhouse::Type::Array(inner)) => {
            let inner = ClickhouseType::from(*inner);
            let series: Vec<Series> = values
                .into_iter()
                .map(move |val| match val {
                    klickhouse::Value::Array(val) => values_to_series(val, inner.clone()),
                    klickhouse::Value::Null => Err(Error::UnexpectedNull("In array")),
                    _ => Err(Error::UnsupportedClickhouseType(ClickhouseType::Native(
                        val.guess_type(),
                    ))),
                })
                .try_collect()?;
            Series::new("", series)
        }
        _ => {
            return Err(Error::UnsupportedClickhouseType(type_));
        }
    };
    Ok(series)
}
