#![doc = include_str!("../README.md")]

mod c2p;
pub use p2c::{ClickhouseTable, TableCreationOptions};
mod errors;
mod structs;
pub use errors::*;
mod p2c;
pub use c2p::get_df_query;

use std::str::FromStr;

/// Wrapper around [klickhouse::Type] with representation for booleans.
#[derive(Clone, Debug, PartialEq)]
pub enum ClickhouseType {
    Native(klickhouse::Type),
    Bool,
    Json,
    Nullable(Box<ClickhouseType>),
}
impl ClickhouseType {
    pub fn nullable(self) -> ClickhouseType {
        Self::Nullable(Box::new(self))
    }
}
impl FromStr for ClickhouseType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "Bool" {
            return Ok(Self::Bool);
        } else if s == "Nullable(Boolean)" {
            return Ok(ClickhouseType::Nullable(Box::new(ClickhouseType::Bool)));
        }
        Ok(Self::Native(klickhouse::Type::from_str(s)?))
    }
}

impl From<klickhouse::Type> for ClickhouseType {
    fn from(source: klickhouse::Type) -> Self {
        Self::Native(source)
    }
}

impl From<ClickhouseType> for klickhouse::Type {
    fn from(source: ClickhouseType) -> Self {
        match source {
            ClickhouseType::Native(n) => n,
            ClickhouseType::Bool => klickhouse::Type::UInt8,
            ClickhouseType::Json => klickhouse::Type::String,
            ClickhouseType::Nullable(n) => {
                klickhouse::Type::Nullable(Box::new(n.as_ref().clone().into()))
            }
        }
    }
}

impl std::fmt::Display for ClickhouseType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ClickhouseType::Native(n) => write!(f, "{}", n),
            ClickhouseType::Bool => write!(f, "Bool"),
            ClickhouseType::Json => write!(f, "String"),
            ClickhouseType::Nullable(n) => write!(f, "Nullable({})", n),
        }
    }
}
