use crate::types::PendingOrder;
use std::convert::TryInto;
use tokio_postgres::{Error, GenericClient};
use tracing::trace;

#[tracing::instrument(skip(client, ticker))]
pub async fn get_pending_order_amount_by_ticker<T: GenericClient>(
    client: &T,
    ticker: &str,
) -> Result<Option<i32>, Error> {
    trace!(ticker, "Getting pending order amount");
    client
        .query_opt(
            "SELECT pending_quantity FROM pending_orders WHERE ticker = $1",
            &[&ticker],
        )
        .await?
        .map(|row| row.try_get(0))
        .transpose()
}

#[tracing::instrument(skip(client))]
pub async fn get_pending_orders<T: GenericClient>(client: &T) -> Result<Vec<PendingOrder>, Error> {
    trace!("Getting pending orders");
    client
        .query("SELECT * FROM pending_orders", &[])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[tracing::instrument(skip(client, id))]
pub async fn get_pending_order_by_id<T: GenericClient>(
    client: &T,
    id: &str,
) -> Result<Option<PendingOrder>, Error> {
    trace!(id, "Getting pending order");
    client
        .query_opt("SELECT * FROM pending_orders where id = $1", &[&id])
        .await?
        .map(TryInto::try_into)
        .transpose()
}

#[tracing::instrument(skip(client, id, qty))]
pub async fn update_pending_order_qty<T: GenericClient>(
    client: &T,
    id: &str,
    qty: i32,
) -> Result<(), Error> {
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
pub async fn save_pending_order<T: GenericClient>(
    client: &T,
    pending_order: &PendingOrder,
) -> Result<(), Error> {
    trace!(id = %pending_order.id, "Saving pending order");
    client.execute("INSERT INTO pending_orders (id, ticker, quantity, pending_quantity) VALUES ($1, $2, $3, $4)", &[&pending_order.id, &pending_order.ticker, &pending_order.qty, &pending_order.pending_qty]).await?;
    Ok(())
}

#[tracing::instrument(skip(client, id))]
pub async fn delete_pending_order_by_id<T: GenericClient>(
    client: &T,
    id: &str,
) -> Result<(), Error> {
    trace!(id, "Deleting pending order");
    client
        .execute("DELETE FROM pending_orders WHERE id = $1", &[&id])
        .await?;
    Ok(())
}
