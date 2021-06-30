use crate::manager::PendingOrder;
use anyhow::Result;
use tokio_postgres::Client;
use tracing::trace;

#[tracing::instrument(skip(client, ticker))]
pub(crate) async fn get_pending_order_amount_by_ticker(
    client: &Client,
    ticker: &str,
) -> Result<Option<i32>> {
    trace!(ticker, "Getting pending order amount");
    client
        .query_opt(
            "SELECT pending_quantity FROM pending_orders WHERE ticker = $1",
            &[&ticker],
        )
        .await?
        .map(|row| -> Result<i32> { Ok(row.try_get(0)?) })
        .transpose()
}

#[tracing::instrument(skip(client, id))]
pub(crate) async fn get_pending_order_by_id(
    client: &Client,
    id: &str,
) -> Result<Option<PendingOrder>> {
    trace!(id, "Getting pending order");
    client
        .query_opt("SELECT * FROM pending_orders where id = $1", &[&id])
        .await?
        .map(|row| -> Result<PendingOrder> {
            Ok(PendingOrder {
                id: row.try_get(0)?,
                ticker: row.try_get(1)?,
                qty: row.try_get(2)?,
                pending_qty: row.try_get(3)?,
            })
        })
        .transpose()
}

#[tracing::instrument(skip(client, id, qty))]
pub(crate) async fn update_pending_order_qty(client: &Client, id: &str, qty: i32) -> Result<()> {
    trace!(id, qty, "Updating pending order");
    client
        .execute(
            "UPDATE pending_orders SET pending_qty = $1 WHERE id = $2",
            &[&qty, &id],
        )
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, pending_order))]
pub(crate) async fn save_pending_order(client: &Client, pending_order: PendingOrder) -> Result<()> {
    trace!(id = %pending_order.id, "Saving pending order");
    client.execute("INSERT INTO pending_orders (id, ticker, quantity, pending_quantity) VALUES ($1, $2, $3, $4)", &[&pending_order.id, &pending_order.ticker, &pending_order.qty, &pending_order.pending_qty]).await?;
    Ok(())
}

#[tracing::instrument(skip(client, id))]
pub(crate) async fn delete_pending_order_by_id(client: &Client, id: &str) -> Result<()> {
    trace!(id, "Deleting pending order");
    client
        .execute("DELETE FROM pending_orders WHERE id = $1", &[&id])
        .await?;
    Ok(())
}
