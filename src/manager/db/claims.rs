use crate::manager::{Claim, OrderManager};
use anyhow::Result;
use position_intents::AmountSpec;
use rust_decimal::prelude::*;
use tracing::trace;
use uuid::Uuid;

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub(crate) async fn get_claims_by_ticker(&self, ticker: &str) -> Result<Vec<Claim>> {
        trace!("Getting claims");
        let rows = self
            .db_client
            .query("SELECT * FROM claims WHERE ticker = $1::TEXT", &[&ticker])
            .await?
            .into_iter()
            .map(|row| Claim {
                id: row.get(0),
                strategy: row.get(1),
                sub_strategy: row.get(2),
                ticker: row.get(3),
                amount: unite_amount_spec(row.get(4), row.get(5)),
            })
            .collect();
        Ok(rows)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn delete_claim_by_id(&self, id: Uuid) -> Result<()> {
        trace!("Deleting claim");
        self.db_client
            .execute("DELETE FROM claims WHERE id = $1;", &[&id])
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self, claim), fields(%claim.id))]
    pub(crate) async fn save_claim(&self, claim: Claim) -> Result<()> {
        trace!("Saving claim");
        let (amount, unit) = split_amount_spec(claim.amount);
        self.db_client.execute("INSERT INTO claims (id, strategy, sub_strategy, ticker, amount, unit) VALUES ($1, $2, $3, $4, $5, $6);", &[
            &claim.id,
            &claim.strategy,
            &claim.sub_strategy,
            &claim.ticker,
            &amount,
            &unit])
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
