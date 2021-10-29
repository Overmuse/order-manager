use crate::types::{Status, Trade};
use anyhow::Result;
use std::convert::TryInto;
use tokio_postgres::{Error, GenericClient};
use tracing::trace;
use uuid::Uuid;

#[tracing::instrument(skip(client, ticker))]
pub async fn get_active_trade_amount_by_ticker<T: GenericClient>(client: &T, ticker: &str) -> Result<i32, Error> {
    trace!(ticker, "Fetching pending trade amount for ticker");
    Ok(client
        .query(
            r#"
SELECT pending_quantity
FROM trades
WHERE ticker = $1 AND status IN ('unreported', 'accepted', 'partially_filled')
        "#,
            &[&ticker],
        )
        .await?
        .into_iter()
        .fold(0, |acc, x| acc + x.get::<usize, i32>(0)))
}

#[tracing::instrument(skip(client))]
pub async fn get_trades<T: GenericClient>(client: &T) -> Result<Vec<Trade>, Error> {
    trace!("Fetching all trades");
    client
        .query("SELECT * FROM trades", &[])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[tracing::instrument(skip(client))]
pub async fn get_trades_by_ticker<T: GenericClient>(client: &T, ticker: &str) -> Result<Vec<Trade>, Error> {
    trace!(ticker, "Fetching trades for ticker");
    client
        .query("SELECT * FROM trades WHERE ticker = $1", &[&ticker])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[tracing::instrument(skip(client, id))]
pub async fn get_trade_by_id<T: GenericClient>(client: &T, id: Uuid) -> Result<Option<Trade>, Error> {
    trace!(%id, "Fetching trade for id");
    client
        .query_opt("SELECT * FROM trades where id = $1", &[&id])
        .await?
        .map(TryInto::try_into)
        .transpose()
}

#[tracing::instrument(skip(client, id, quantity))]
pub async fn update_trade_quantity<T: GenericClient>(client: &T, id: Uuid, quantity: i32) -> Result<()> {
    trace!(%id, quantity, "Updating trade quantity");
    client
        .execute(
            "UPDATE trades SET pending_quantity = $1 WHERE id = $2",
            &[&quantity, &id],
        )
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, id, status))]
pub async fn update_status<T: GenericClient>(client: &T, id: Uuid, status: Status) -> Result<()> {
    trace!(%id, ?status, "Updating trade status");
    client
        .execute("UPDATE trades SET status = $1 WHERE id = $2", &[&status, &id])
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, trade))]
pub async fn save_trade<T: GenericClient>(client: &T, trade: Trade) -> Result<()> {
    trace!(id = %trade.id, "Saving trade");
    client.execute(
        "INSERT INTO trades (id, broker_id, ticker, quantity, pending_quantity, datetime, status) VALUES ($1, $2, $3, $4, $5, $6, $7)", 
        &[
            &trade.id,
            &trade.broker_id,
            &trade.ticker,
            &trade.quantity,
            &trade.pending_quantity,
            &trade.datetime,
            &trade.status
        ]
    ).await?;
    Ok(())
}

#[tracing::instrument(skip(client, id))]
pub async fn delete_trade_by_id<T: GenericClient>(client: &T, id: Uuid) -> Result<()> {
    trace!(%id, "Deleting trade");
    client.execute("DELETE FROM trades WHERE id = $1", &[&id]).await?;
    Ok(())
}
