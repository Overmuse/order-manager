use crate::types::PendingTrade;
use std::convert::TryInto;
use tokio_postgres::{Error, GenericClient};
use tracing::trace;

#[tracing::instrument(skip(client, ticker))]
pub async fn get_pending_trade_amount_by_ticker<T: GenericClient>(
    client: &T,
    ticker: &str,
) -> Result<Option<i32>, Error> {
    trace!(ticker, "Getting pending trade amount");
    client
        .query_opt(
            "SELECT pending_quantity FROM pending_trades WHERE ticker = $1",
            &[&ticker],
        )
        .await?
        .map(|row| row.try_get(0))
        .transpose()
}

#[tracing::instrument(skip(client))]
pub async fn get_pending_trades<T: GenericClient>(client: &T) -> Result<Vec<PendingTrade>, Error> {
    trace!("Getting pending trades");
    client
        .query("SELECT * FROM pending_trades", &[])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[tracing::instrument(skip(client, id))]
pub async fn get_pending_trade_by_id<T: GenericClient>(
    client: &T,
    id: &str,
) -> Result<Option<PendingTrade>, Error> {
    trace!(id, "Getting pending trade");
    client
        .query_opt("SELECT * FROM pending_trades where id = $1", &[&id])
        .await?
        .map(TryInto::try_into)
        .transpose()
}

#[tracing::instrument(skip(client, id, qty))]
pub async fn update_pending_trade_qty<T: GenericClient>(
    client: &T,
    id: &str,
    qty: i32,
) -> Result<(), Error> {
    trace!(id, qty, "Updating pending trade");
    client
        .execute(
            "UPDATE pending_trades SET pending_qty = $1 WHERE id = $2",
            &[&qty, &id],
        )
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, pending_trade))]
pub async fn save_pending_trade<T: GenericClient>(
    client: &T,
    pending_trade: PendingTrade,
) -> Result<(), Error> {
    trace!(id = %pending_trade.id, "Saving pending trade");
    client.execute("INSERT INTO pending_trades (id, ticker, quantity, pending_quantity) VALUES ($1, $2, $3, $4)", &[&pending_trade.id, &pending_trade.ticker, &pending_trade.qty, &pending_trade.pending_qty]).await?;
    Ok(())
}

#[tracing::instrument(skip(client, id))]
pub async fn delete_pending_trade_by_id<T: GenericClient>(
    client: &T,
    id: &str,
) -> Result<(), Error> {
    trace!(id, "Deleting pending trade");
    client
        .execute("DELETE FROM pending_trades WHERE id = $1", &[&id])
        .await?;
    Ok(())
}
