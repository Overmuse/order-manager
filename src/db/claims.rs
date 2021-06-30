use crate::manager::Claim;
use anyhow::Result;
use position_intents::AmountSpec;
use rust_decimal::prelude::*;
use tokio_postgres::Client;
use tracing::trace;
use uuid::Uuid;

#[tracing::instrument(skip(client, ticker))]
pub(crate) async fn get_claims_by_ticker(client: &Client, ticker: &str) -> Result<Vec<Claim>> {
    trace!(ticker, "Getting claims");
    client
        .query("SELECT * FROM claims WHERE ticker = $1", &[&ticker])
        .await?
        .into_iter()
        .map(|row| -> Result<Claim> {
            Ok(Claim {
                id: row.try_get(0)?,
                strategy: row.try_get(1)?,
                sub_strategy: row.try_get(2)?,
                ticker: row.try_get(3)?,
                amount: unite_amount_spec(row.try_get(4)?, row.try_get(5)?),
            })
        })
        .collect()
}

#[tracing::instrument(skip(client, id))]
pub(crate) async fn get_claim_by_id(client: &Client, id: Uuid) -> Result<Claim> {
    trace!(%id, "Getting claim");
    let row = client
        .query_one("SELECT * FROM claims WHERE id = $1", &[&id])
        .await?;
    Ok(Claim {
        id: row.try_get(0)?,
        strategy: row.try_get(1)?,
        sub_strategy: row.try_get(2)?,
        ticker: row.try_get(3)?,
        amount: unite_amount_spec(row.try_get(4)?, row.try_get(5)?),
    })
}

#[tracing::instrument(skip(client, id, amount))]
pub(crate) async fn update_claim_amount(
    client: &Client,
    id: Uuid,
    amount: AmountSpec,
) -> Result<()> {
    trace!(%id, ?amount, "Updating claim amount");
    let (amount, unit) = split_amount_spec(amount);
    client
        .execute(
            "UPDATE claims SET amount = $1, unit = $2 WHERE id = $3",
            &[&amount, &unit, &id],
        )
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, id))]
pub(crate) async fn delete_claim_by_id(client: &Client, id: Uuid) -> Result<()> {
    trace!(%id, "Deleting claim");
    client
        .execute("DELETE FROM claims WHERE id = $1;", &[&id])
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, claim))]
pub(crate) async fn save_claim(client: &Client, claim: Claim) -> Result<()> {
    trace!(id = %claim.id, "Saving claim");
    let (amount, unit) = split_amount_spec(claim.amount);
    client.execute("INSERT INTO claims (id, strategy, sub_strategy, ticker, amount, unit) VALUES ($1, $2, $3, $4, $5, $6);", &[
            &claim.id,
            &claim.strategy,
            &claim.sub_strategy,
            &claim.ticker,
            &amount,
            &unit])
            .await?;
    Ok(())
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
