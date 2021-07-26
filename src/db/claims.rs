use super::utils::split_amount_spec;
use crate::types::Claim;
use std::convert::TryInto;
use tokio_postgres::{Error, GenericClient};
use tracing::trace;
use trading_base::Amount;
use uuid::Uuid;

pub async fn get_claims<T: GenericClient>(client: &T) -> Result<Vec<Claim>, Error> {
    trace!("Getting claims");
    client
        .query("SELECT * FROM claims", &[])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[tracing::instrument(skip(client, ticker))]
pub async fn get_claims_by_ticker<T: GenericClient>(
    client: &T,
    ticker: &str,
) -> Result<Vec<Claim>, Error> {
    trace!(ticker, "Getting claims");
    client
        .query("SELECT * FROM claims WHERE ticker = $1", &[&ticker])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[tracing::instrument(skip(client, id))]
pub async fn get_claim_by_id<T: GenericClient>(client: &T, id: Uuid) -> Result<Claim, Error> {
    trace!(%id, "Getting claim");
    client
        .query_one("SELECT * FROM claims WHERE id = $1", &[&id])
        .await?
        .try_into()
}

#[tracing::instrument(skip(client, id, amount))]
pub async fn update_claim_amount<T: GenericClient>(
    client: &T,
    id: Uuid,
    amount: Amount,
) -> Result<(), Error> {
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
pub async fn delete_claim_by_id<T: GenericClient>(client: &T, id: Uuid) -> Result<(), Error> {
    trace!(%id, "Deleting claim");
    client
        .execute("DELETE FROM claims WHERE id = $1;", &[&id])
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, claim))]
pub async fn save_claim<T: GenericClient>(client: &T, claim: Claim) -> Result<(), Error> {
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
