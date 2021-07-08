use crate::types::Lot;
use std::convert::TryInto;
use std::sync::Arc;
use tokio_postgres::{Client, Error};
use tracing::trace;

#[tracing::instrument(skip(client))]
pub async fn get_lots(client: Arc<Client>) -> Result<Vec<Lot>, Error> {
    trace!("Getting lots");
    client
        .query("SELECT * FROM lots", &[])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[tracing::instrument(skip(client, order_id))]
pub async fn get_lots_by_order_id(client: Arc<Client>, order_id: &str) -> Result<Vec<Lot>, Error> {
    trace!(order_id, "Getting lots");
    client
        .query("SELECT * FROM lots WHERE order_id = $1", &[&order_id])
        .await?
        .into_iter()
        .map(|row| -> Result<Lot, Error> {
            Ok(Lot {
                id: row.try_get(0)?,
                order_id: row.try_get(1)?,
                ticker: row.try_get(2)?,
                fill_time: row.try_get(3)?,
                price: row.try_get(4)?,
                shares: row.try_get(5)?,
            })
        })
        .collect()
}

#[tracing::instrument(skip(client, lot))]
pub async fn save_lot(client: Arc<Client>, lot: Lot) -> Result<(), Error> {
    trace!(id = %lot.id, "Saving lot");
    client.execute("INSERT INTO lots (id, order_id, ticker, fill_time, price, shares) VALUES ($1, $2, $3, $4, $5, $6);", &[
            &lot.id,
            &lot.order_id,
            &lot.ticker,
            &lot.fill_time,
            &lot.price,
            &lot.shares,
        ])
            .await?;
    Ok(())
}