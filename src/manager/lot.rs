use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Lot {
    pub id: Uuid,
    pub ticker: String,
    pub fill_time: DateTime<Utc>,
    pub price: Decimal,
    pub shares: Decimal,
}

impl Lot {
    pub fn new(ticker: String, fill_time: DateTime<Utc>, price: Decimal, shares: Decimal) -> Self {
        Self {
            id: Uuid::new_v4(),
            ticker,
            fill_time,
            price,
            shares,
        }
    }
}
