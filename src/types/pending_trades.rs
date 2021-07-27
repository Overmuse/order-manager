use serde::Serialize;
use std::convert::TryFrom;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize)]
pub struct PendingTrade {
    pub id: Uuid,
    pub ticker: String,
    pub qty: i32,
    pub pending_qty: i32,
}

impl PendingTrade {
    #[tracing::instrument(skip(id, ticker, qty))]
    pub fn new(id: Uuid, ticker: String, qty: i32) -> Self {
        tracing::trace!(%id, %ticker, %qty, "New PendingTrade");
        Self {
            id,
            ticker,
            qty,
            pending_qty: qty,
        }
    }
}

impl TryFrom<Row> for PendingTrade {
    type Error = tokio_postgres::Error;
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            ticker: row.try_get("ticker")?,
            qty: row.try_get("qty")?,
            pending_qty: row.try_get("pending_qty")?,
        })
    }
}
