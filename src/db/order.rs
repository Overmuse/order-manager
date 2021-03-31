use alpaca::common::Order;
use anyhow::{Context, Result};
use bson::doc;
use mongodb::{Collection, Cursor, Database};
use tracing::trace;

#[tracing::instrument(skip(db))]
pub(crate) async fn pending_orders(db: &Database) -> Result<Cursor<Order>> {
    trace!("Fetching pending orders");
    let order_collection: Collection<Order> = db.collection_with_type("orders");
    let pending_orders = order_collection
        .find(
            doc! {
                "status": { "$in": ["new", "partially_filled", "done_for_day", "accepted", "pending_new", "accepted_for_bidding", "suspended", "calculated"] }
            },
            None,
        )
        .await
        .context("Failed to lookup pending orders")?;
    Ok(pending_orders)
}

#[tracing::instrument(skip(db))]
pub(crate) async fn pending_orders_by_ticker(db: &Database, ticker: &str) -> Result<Cursor<Order>> {
    trace!("Fetching pending orders for {}", ticker);
    let order_collection: Collection<Order> = db.collection_with_type("orders");
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
    let order_collection: Collection<Order> = db.collection_with_type("orders");
    let completed_orders = order_collection
            .find(
                doc! {
                    "status": { "$nin": ["new", "partially_filled", "done_for_day", "accepted", "pending_new", "accepted_for_bidding", "suspended", "calculated"] }
                },
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
    let order_collection: Collection<Order> = db.collection_with_type("orders");
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
    let order_collection: Collection<Order> = db.collection_with_type("orders");
    order_collection
            .find_one_and_replace(
                doc! {
                    "client_order_id": order.client_order_id.as_ref().expect("All orders should have client_order_id")
                },
                order,
                //doc! {
                //    "$set": {
                //        "updated_at": bson::to_bson(&order.updated_at).context("Failed to serialize updated_at")?,
                //        "submitted_at": bson::to_bson(&order.submitted_at).context("Failed to serialize submitted_at")?,
                //        "filled_at": bson::to_bson(&order.filled_at).context("Failed to serialize filled_at")?,
                //        "expired_at": bson::to_bson(&order.expired_at).context("Failed to serialize expired_at")?,
                //        "canceled_at": bson::to_bson(&order.canceled_at).context("Failed to serialize canceled_at")?,
                //        "failed_at": bson::to_bson(&order.failed_at).context("Failed to serialize failed_at")?,
                //        "replaced_at": bson::to_bson(&order.replaced_at).context("Failed to serialize replaced_at")?,
                //        "replaced_by": bson::to_bson(&order.replaced_by).context("Failed to serialize replaced_by")?,
                //        "replaces": bson::to_bson(&order.replaces).context("Failed to serialize replaces")?,
                //        "filled_qty": bson::to_bson(&(order.filled_qty as i32)).context("Failed to serialize filled_qty")?,
                //        "filled_avg_price": bson::to_bson(&order.filled_avg_price).context("Failed to serialize filled_avg_price")?,
                //        "status": bson::to_bson(&order.status).context("Failed to serialize status")?,
                //    }
                //},
                mongodb::options::FindOneAndReplaceOptions::builder()
                    .upsert(true)
                    .build(),
            )
            .await
            .context("Failed to update order")?;
    trace!("Upsert complete");
    Ok(())
}
