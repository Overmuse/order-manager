use alpaca::common::Order;
use anyhow::{Context, Result};
use bson::doc;
use mongodb::{Collection, Cursor, Database};
use tracing::trace;

#[tracing::instrument(skip(db))]
pub(crate) async fn pending_orders(db: &Database) -> Result<Cursor<Order>> {
    trace!("Fetching pending orders");
    let order_collection: Collection<Order> = db.collection("orders");
    let pending_orders = order_collection
        .find(
            doc! {"status": { "$in": ["new", "partially_filled", "done_for_day", "accepted", "pending_new", "accepted_for_bidding", "suspended", "calculated"] }},
            None,
        )
        .await
        .context("Failed to lookup pending orders")?;
    Ok(pending_orders)
}

#[tracing::instrument(skip(db))]
pub(crate) async fn pending_orders_by_ticker(db: &Database, ticker: &str) -> Result<Cursor<Order>> {
    trace!("Fetching pending orders for {}", ticker);
    let order_collection: Collection<Order> = db.collection("orders");
    let pending_orders = order_collection
        .find(
            doc! {
                "symbol": ticker,
                "status": { "$in": ["new", "partially_filled", "done_for_day", "accepted", "pending_new", "accepted_for_bidding", "suspended", "calculated"] }
            },
            None,
        )
        .await
        .context(format!("Failed to lookup pending orders for {}", ticker))?;
    Ok(pending_orders)
}

#[tracing::instrument(skip(db))]
pub(crate) async fn completed_orders(db: &Database) -> Result<Cursor<Order>> {
    trace!("Fetching completed orders");
    let order_collection: Collection<Order> = db.collection("orders");
    let completed_orders = order_collection
            .find(
                doc! {"status": { "$nin": ["new", "partially_filled", "done_for_day", "accepted", "pending_new", "accepted_for_bidding", "suspended", "calculated"] }},
                None,
            )
            .await
            .context("Failed to lookup completed orders")?;
    Ok(completed_orders)
}

#[tracing::instrument(skip(db))]
pub(crate) async fn completed_orders_by_ticker(
    db: &Database,
    ticker: &str,
) -> Result<Cursor<Order>> {
    trace!("Fetching completed orders for {}", ticker);
    let order_collection: Collection<Order> = db.collection("orders");
    let completed_orders = order_collection
            .find(
                doc! {
                    "symbol": ticker,
                    "status": { "$nin": ["new", "partially_filled", "done_for_day", "accepted", "pending_new", "accepted_for_bidding", "suspended", "calculated"] }
                },
                None,
            )
            .await
            .context(format!("Failed to lookup completed orders for {}", ticker))?;
    Ok(completed_orders)
}

#[tracing::instrument(skip(db, order))]
pub(crate) async fn upsert_order(db: &Database, order: Order) -> Result<()> {
    trace!("Upserting order {:?}", order);
    let order_collection: Collection<Order> = db.collection("orders");
    order_collection
        .find_one_and_replace(
            doc! {"client_order_id": &order.client_order_id},
            order,
            mongodb::options::FindOneAndReplaceOptions::builder()
                .upsert(true)
                .build(),
        )
        .await
        .context("Failed to update order")?;
    trace!("Upsert complete");
    Ok(())
}
