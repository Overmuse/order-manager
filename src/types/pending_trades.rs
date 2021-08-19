use chrono::{DateTime, Utc};
use serde::Serialize;
use std::convert::TryFrom;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Copy, Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Unreported,
    Accepted,
    PartiallyFilled,
    Filled,
    Dead,
}

#[derive(Clone, Debug, Serialize)]
pub struct PendingTrade {
    pub id: Uuid,
    pub ticker: String,
    pub qty: i32,
    pub pending_qty: i32,
    pub datetime: DateTime<Utc>,
    pub status: Status,
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
            datetime: Utc::now(),
            status: Status::Unreported,
        }
    }
}

impl TryFrom<Row> for PendingTrade {
    type Error = tokio_postgres::Error;
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let status = match row.try_get("status")? {
            "unreported" => Status::Unreported,
            "accepted" => Status::Accepted,
            "partially_filled" => Status::PartiallyFilled,
            "filled" => Status::Filled,
            "dead" => Status::Dead,
            _ => unreachable!(),
        };

        Ok(Self {
            id: row.try_get("id")?,
            ticker: row.try_get("ticker")?,
            qty: row.try_get("quantity")?,
            pending_qty: row.try_get("pending_quantity")?,
            datetime: row.try_get("datetime")?,
            status,
        })
    }
}
