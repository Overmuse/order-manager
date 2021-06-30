use crate::manager::Lot;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tracing::trace;

#[tracing::instrument(skip(client))]
pub(crate) async fn get_lots(client: Arc<Mutex<Client>>) -> Result<Vec<Lot>> {
    trace!("Getting lots");
    client
        .lock()
        .await
        .query("SELECT * FROM lots", &[])
        .await?
        .into_iter()
        .map(|row| -> Result<Lot> {
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

#[tracing::instrument(skip(client, order_id))]
pub(crate) async fn get_lots_by_order_id(
    client: Arc<Mutex<Client>>,
    order_id: &str,
) -> Result<Vec<Lot>> {
    trace!(order_id, "Getting lots");
    client
        .lock()
        .await
        .query("SELECT * FROM lots WHERE order_id = $1", &[&order_id])
        .await?
        .into_iter()
        .map(|row| -> Result<Lot> {
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
pub(crate) async fn save_lot(client: Arc<Mutex<Client>>, lot: Lot) -> Result<()> {
    trace!(id = %lot.id, "Saving lot");
    client.lock().await.execute("INSERT INTO lots (id, order_id, ticker, fill_time, price, shares) VALUES ($1, $2, $3, $4, $5, $6);", &[
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
