use position_intents::AmountSpec;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use tokio_postgres::Row;
use tracing::trace;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claim {
    pub id: Uuid,
    pub strategy: String,
    pub sub_strategy: Option<String>,
    pub ticker: String,
    pub amount: AmountSpec,
}

impl Claim {
    #[tracing::instrument(skip(strategy, sub_strategy, ticker, amount))]
    pub fn new(
        strategy: String,
        sub_strategy: Option<String>,
        ticker: String,
        amount: AmountSpec,
    ) -> Self {
        trace!(%strategy, ?sub_strategy, %ticker, ?amount, "New Claim");
        Self {
            id: Uuid::new_v4(),
            strategy,
            sub_strategy,
            ticker,
            amount,
        }
    }
}

impl TryFrom<Row> for Claim {
    type Error = tokio_postgres::Error;
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            strategy: row.try_get("strategy")?,
            sub_strategy: row.try_get("sub_strategy")?,
            ticker: row.try_get("ticker")?,
            amount: unite_amount_spec(row.try_get("amount")?, row.try_get("unit")?),
        })
    }
}

fn unite_amount_spec(amount: Decimal, unit: &str) -> AmountSpec {
    match unit {
        "dollars" => AmountSpec::Dollars(amount),
        "shares" => AmountSpec::Shares(amount),
        "percent" => AmountSpec::Percent(amount),
        "zero" => AmountSpec::Zero,
        _ => unreachable!(),
    }
}
