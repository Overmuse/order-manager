use super::Owner;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use tokio_postgres::Row;
use tracing::trace;

#[derive(Debug, Serialize, Deserialize)]
pub struct Position {
    pub owner: Owner,
    pub ticker: String,
    pub shares: Decimal,
    pub basis: Decimal,
}

impl Position {
    #[tracing::instrument(skip(owner, ticker, shares, basis))]
    pub fn new(owner: Owner, ticker: String, shares: Decimal, basis: Decimal) -> Self {
        trace!(%owner, %ticker, %shares, %basis, "New Position");
        Self {
            owner,
            ticker,
            shares,
            basis,
        }
    }

    pub fn is_long(&self) -> bool {
        self.shares > Decimal::ZERO
    }

    pub fn is_short(&self) -> bool {
        self.shares < Decimal::ZERO
    }
}

impl TryFrom<Row> for Position {
    type Error = tokio_postgres::Error;
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let owner = row.try_get("owner")?;
        let owner = if owner == "House" {
            Owner::House
        } else {
            Owner::Strategy(owner, row.try_get("sub_owner")?)
        };
        Ok(Self {
            owner,
            ticker: row.try_get("ticker")?,
            shares: row.try_get("shares")?,
            basis: row.try_get("basis")?,
        })
    }
}
