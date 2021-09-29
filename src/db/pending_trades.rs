use crate::types::{PendingTrade, Status};
use anyhow::Result;
use std::convert::TryInto;
use tokio_postgres::{Error, GenericClient};
use tracing::trace;
use uuid::Uuid;

#[tracing::instrument(skip(client, ticker))]
pub async fn get_pending_trade_amount_by_ticker<T: GenericClient>(client: &T, ticker: &str) -> Result<i32, Error> {
    trace!(ticker, "Fetching pending trade amount for ticker");
    Ok(client
        .query(
            "SELECT pending_quantity FROM pending_trades WHERE ticker = $1",
            &[&ticker],
        )
        .await?
        .into_iter()
        .fold(0, |acc, x| acc + x.get::<usize, i32>(0)))
}

#[tracing::instrument(skip(client))]
pub async fn get_pending_trades<T: GenericClient>(client: &T) -> Result<Vec<PendingTrade>, Error> {
    trace!("Fetching all pending trades");
    client
        .query("SELECT * FROM pending_trades", &[])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[tracing::instrument(skip(client))]
pub async fn get_pending_trades_by_ticker<T: GenericClient>(
    client: &T,
    ticker: &str,
) -> Result<Vec<PendingTrade>, Error> {
    trace!(ticker, "Fetching pending trades for ticker");
    client
        .query("SELECT * FROM pending_trades WHERE ticker = $1", &[&ticker])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[tracing::instrument(skip(client, id))]
pub async fn get_pending_trade_by_id<T: GenericClient>(client: &T, id: Uuid) -> Result<Option<PendingTrade>, Error> {
    trace!(%id, "Fetching pending trade for id");
    client
        .query_opt("SELECT * FROM pending_trades where id = $1", &[&id])
        .await?
        .map(TryInto::try_into)
        .transpose()
}

#[tracing::instrument(skip(client, id, quantity))]
pub async fn update_pending_trade_quantity<T: GenericClient>(client: &T, id: Uuid, quantity: i32) -> Result<()> {
    trace!(%id, quantity, "Updating pending trade quantity");
    client
        .execute(
            "UPDATE pending_trades SET pending_quantity = $1 WHERE id = $2",
            &[&quantity, &id],
        )
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, id, status))]
pub async fn update_pending_trade_status<T: GenericClient>(client: &T, id: Uuid, status: Status) -> Result<()> {
    trace!(%id, ?status, "Updating pending trade status");
    client
        .execute(
            "UPDATE pending_trades SET status = $1 WHERE id = $2",
            &[&serde_plain::to_string(&status)?, &id],
        )
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, pending_trade))]
pub async fn save_pending_trade<T: GenericClient>(client: &T, pending_trade: PendingTrade) -> Result<()> {
    trace!(id = %pending_trade.id, "Saving pending trade");
    client.execute(
        "INSERT INTO pending_trades (id, ticker, quantity, pending_quantity, datetime, status) VALUES ($1, $2, $3, $4, $5, $6)", 
        &[
            &pending_trade.id,
            &pending_trade.ticker,
            &pending_trade.quantity,
            &pending_trade.pending_quantity,
            &pending_trade.datetime,
            &serde_plain::to_string(&pending_trade.status)?
        ]
    ).await?;
    Ok(())
}

#[tracing::instrument(skip(client, id))]
pub async fn delete_pending_trade_by_id<T: GenericClient>(client: &T, id: Uuid) -> Result<()> {
    trace!(%id, "Deleting pending trade");
    client
        .execute("DELETE FROM pending_trades WHERE id = $1", &[&id])
        .await?;
    Ok(())
}
