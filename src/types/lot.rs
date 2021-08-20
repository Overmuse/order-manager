use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use tokio_postgres::Row;
use tracing::trace;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Lot {
    pub id: Uuid,
    pub order_id: Uuid,
    pub ticker: String,
    pub fill_time: DateTime<Utc>,
    pub price: Decimal,
    pub shares: Decimal,
}

impl Lot {
    #[tracing::instrument(skip(order_id, ticker, fill_time, price, shares))]
    pub fn new(order_id: Uuid, ticker: String, fill_time: DateTime<Utc>, price: Decimal, shares: Decimal) -> Self {
        trace!(%order_id, %ticker, %fill_time, %price, %shares, "New Lot");
        Self {
            id: Uuid::new_v4(),
            order_id,
            ticker,
            fill_time,
            price,
            shares,
        }
    }
}

impl TryFrom<Row> for Lot {
    type Error = tokio_postgres::Error;
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            order_id: row.try_get("order_id")?,
            ticker: row.try_get("ticker")?,
            fill_time: row.try_get("fill_time")?,
            price: row.try_get("price")?,
            shares: row.try_get("shares")?,
        })
    }
}
