use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use tokio_postgres::Row;
use tracing::{trace, warn};
use trading_base::Amount;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claim {
    pub id: Uuid,
    pub strategy: String,
    pub sub_strategy: Option<String>,
    pub ticker: String,
    pub amount: Amount,
    pub limit_price: Option<Decimal>,
}

impl Claim {
    #[tracing::instrument(skip(strategy, sub_strategy, ticker, amount, limit_price))]
    pub fn new(
        strategy: String,
        sub_strategy: Option<String>,
        ticker: String,
        amount: Amount,
        limit_price: Option<Decimal>,
    ) -> Self {
        trace!(%strategy, ?sub_strategy, %ticker, ?amount, ?limit_price, "New Claim");
        Self {
            id: Uuid::new_v4(),
            strategy,
            sub_strategy,
            ticker,
            amount,
            limit_price,
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
            limit_price: row.try_get("limit_price")?,
        })
    }
}

fn unite_amount_spec(amount: Decimal, unit: &str) -> Amount {
    match unit {
        "dollars" => Amount::Dollars(amount),
        "shares" => Amount::Shares(amount),
        "zero" => Amount::Zero,
        _ => unreachable!(),
    }
}
#[tracing::instrument(skip(intent_amount, strategy_shares, maybe_price))]

pub fn calculate_claim_amount(
    intent_amount: &Amount,
    strategy_shares: Decimal,
    maybe_price: Option<Decimal>,
) -> Option<Amount> {
    match intent_amount {
        Amount::Dollars(dollars) => match maybe_price {
            Some(price) => Some(Amount::Dollars(dollars - price * strategy_shares)),
            None => {
                warn!("Missing price");
                None
            }
        },
        Amount::Shares(shares) => Some(Amount::Shares(shares - strategy_shares)),
        Amount::Zero => Some(Amount::Shares(-strategy_shares)),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_calculate_claim_amount() {
        let shares = calculate_claim_amount(&Amount::Shares(Decimal::TWO), Decimal::ONE, None);
        let dollars = calculate_claim_amount(&Amount::Dollars(Decimal::TWO), Decimal::ONE, Some(Decimal::ONE));
        let dollars_no_price = calculate_claim_amount(&Amount::Dollars(Decimal::TWO), Decimal::ONE, None);
        let zero = calculate_claim_amount(&Amount::Zero, Decimal::ONE, None);

        assert_eq!(shares, Some(Amount::Shares(Decimal::ONE)));
        assert_eq!(dollars, Some(Amount::Dollars(Decimal::ONE)));
        assert_eq!(dollars_no_price, None);
        assert_eq!(zero, Some(Amount::Shares(-Decimal::ONE)));
    }
}
