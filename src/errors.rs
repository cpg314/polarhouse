use std::collections::HashSet;

use polars::prelude::*;

use super::ClickhouseType;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    #[error("Klickhouse error: {0}")]
    Klickhouse(#[from] klickhouse::KlickhouseError),
    #[error("A Clickhouse client is required in ClickhouseTable")]
    MissingClient,
    #[error("Unsupported Polars data type {0}")]
    UnsupportedPolarsType(DataType),
    #[error("Unsupported Clickhouse data type {0}")]
    UnsupportedClickhouseType(ClickhouseType),
    #[error("Primary key {0} not present in columns")]
    InvalidPrimaryKey(String),
    #[error("Columns mismatch between dataframe and table: {0}")]
    MismatchingColumns(String),
    #[error("Unexpected value type: {0}, expected {1}")]
    MismatchingValueType(klickhouse::Type, klickhouse::Type),
    #[error("Unexpected series type: {0}")]
    MismatchingSeriesType(DataType),
    #[error("Unexpected null value: {0}")]
    UnexpectedNull(&'static str),
    #[error("Column {0} returned by Clickhouse is not present locally")]
    MissingColumnLocal(String),
    #[error("Dataframe should be rechunked")]
    ShouldRechunk,
    #[error("The constructed series do not have the same lengths: {0:?}")]
    MismatchingLengths(HashSet<usize>),
    #[error("HTTP error: {0}")]
    Http(#[from] HttpError),
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("Request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("I/O error: {0}")]
    IO(std::io::Error),
    #[error("Polars to Clickhouse unsupported with HTTP client")]
    Insertion,
    #[error("Server error: {0}")]
    Server(String),
}
