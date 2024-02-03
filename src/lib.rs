#![doc = include_str!("../README.md")]

mod c2p;
pub use clickhouse::table_types as table_types_from_clickhouse;
pub use p2c::ClickhouseTable;
mod errors;
mod structs;
pub use errors::*;
mod clickhouse;
mod p2c;
pub use c2p::get_df_query;

use std::str::FromStr;

/// Wrapper around [klickhouse::Type] with representation for booleans.
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
