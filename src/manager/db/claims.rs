use crate::manager::{Claim, OrderManager};
use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use position_intents::AmountSpec;
use rust_decimal::Decimal;
use sqlx::FromRow;
use tracing::trace;
use uuid::Uuid;

#[derive(FromRow, Debug)]
struct ClaimRow {
    id: Uuid,
    strategy: String,
    sub_strategy: Option<String>,
    ticker: String,
    amount: Decimal,
    unit: String,
}

impl Into<Claim> for ClaimRow {
    fn into(self) -> Claim {
        let amount_spec = unite_amount_spec(self.amount, &self.unit);
        Claim {
            id: self.id,
            strategy: self.strategy,
            ticker: self.ticker,
            sub_strategy: self.sub_strategy,
            amount: amount_spec,
        }
    }
}

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub(crate) async fn get_claims_by_ticker(&self, ticker: &str) -> Result<Vec<Claim>> {
        trace!("Getting claims");
        let res = sqlx::query_as!(ClaimRow, "SELECT * FROM claims WHERE ticker = $1", ticker)
            .fetch(&self.pool)
            .map_ok(Into::into)
            .filter_map(|x| {
                trace!("{:?}", x);
                async { x.ok() }
            })
            .collect()
            .await;
        Ok(res)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn delete_claim_by_id(&self, id: Uuid) -> Result<()> {
        trace!("Deleting claim");
        sqlx::query("DELETE FROM claims WHERE id = $1;")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self, claim), fields(%claim.id))]
    pub(crate) async fn save_claim(&self, claim: Claim) -> Result<()> {
        trace!("Saving claim");
        let (amount, unit) = split_amount_spec(claim.amount);
        sqlx::query("INSERT INTO claims (id, strategy, sub_strategy, ticker, amount, unit) VALUES ($1, $2, $3, $4, $5, $6);")
            .bind(claim.id)
            .bind(claim.strategy)
            .bind(claim.sub_strategy)
            .bind(claim.ticker)
            .bind(amount)
            .bind(unit)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

fn split_amount_spec(amount_spec: AmountSpec) -> (Decimal, &'static str) {
    match amount_spec {
        AmountSpec::Dollars(dollars) => (dollars, "dollars"),
        AmountSpec::Shares(shares) => (shares, "shares"),
        AmountSpec::Percent(percent) => (percent, "percent"),
        AmountSpec::Zero => (Decimal::ZERO, "zero"),
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
