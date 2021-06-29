use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Lot {
    pub id: Uuid,
    pub order_id: String,
    pub ticker: String,
    pub fill_time: DateTime<Utc>,
    pub price: Decimal,
    pub shares: Decimal,
}

impl Lot {
    #[tracing::instrument(skip(order_id, ticker, fill_time, price, shares))]
    pub fn new(
        order_id: String,
        ticker: String,
        fill_time: DateTime<Utc>,
        price: Decimal,
        shares: Decimal,
    ) -> Self {
        tracing::trace!(%order_id, %ticker, %fill_time, %price, %shares, "New Lot");
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
