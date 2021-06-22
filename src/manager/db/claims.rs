use crate::manager::{Claim, OrderManager};
use anyhow::Result;
use position_intents::AmountSpec;
use rust_decimal::Decimal;
use sqlx::Row;
use tracing::trace;
use uuid::Uuid;

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub(crate) async fn get_claims_by_ticker(&self, ticker: &str) -> Result<Vec<Claim>> {
        trace!("Getting claims");
        let res = sqlx::query(
            r#"SELECT id, strategy, sub_strategy as "sub_strategy?", ticker, amount, unit FROM claims WHERE ticker = $1"#,
        )
        .bind(ticker)
        .fetch_all(&self.pool)
        .await?;
        let res = res
            .into_iter()
            .map(|row| -> Result<Claim> {
                let amount_spec = unite_amount_spec(row.try_get("amount")?, row.try_get("unit")?);
                Ok(Claim::new(
                    row.try_get_unchecked("id")?,
                    row.try_get("strategy")?,
                    row.try_get("sub_strategy")?,
                    amount_spec,
                ))
            })
            .inspect(|x| trace!("{:?}", x))
            .filter_map(Result::ok)
            .collect();
        Ok(res)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn delete_claim_by_id(&self, id: String) -> Result<()> {
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
