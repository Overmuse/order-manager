use crate::DependentOrder;
use anyhow::{Context, Result};
use bson::doc;
use mongodb::{Collection, Cursor, Database};
use tracing::trace;

#[tracing::instrument(skip(db))]
pub(crate) async fn get_dependent_orders(db: &Database) -> Result<Cursor<DependentOrder>> {
    trace!("Fetching dependent orders");
    let dependent_order_collection: Collection<DependentOrder> = db.collection("dependent-orders");
    let dependent_orders = dependent_order_collection
        .find(None, None)
        .await
        .context("Failed to lookup dependent orders")?;
    Ok(dependent_orders)
}

#[tracing::instrument(skip(db))]
pub(crate) async fn get_dependent_order_by_client_order_id(
    db: &Database,
    client_order_id: &str,
) -> Result<Option<DependentOrder>> {
    trace!("Fetching pending orders for {}", client_order_id);
    let dependent_order_collection: Collection<DependentOrder> = db.collection("dependent-orders");
    let dependent_orders = dependent_order_collection
        .find_one(doc! {"client_order_id": client_order_id}, None)
        .await
        .context(format!(
            "Failed to lookup dependent orders for {}",
            client_order_id
        ))?;
    Ok(dependent_orders)
}

#[tracing::instrument(skip(db))]
pub(crate) async fn save_dependent_order(
    db: &Database,
    dependent_order: DependentOrder,
) -> Result<()> {
    trace!("Saving dependent order: {:?}", dependent_order);
    let dependent_order_collection: Collection<DependentOrder> = db.collection("dependent-orders");
    dependent_order_collection
        .insert_one(dependent_order, None)
        .await
        .context("Failed to insert dependent order into database")?;
    Ok(())
}

#[tracing::instrument(skip(db))]
pub(crate) async fn delete_dependent_order_by_client_order_id(
    db: &Database,
    client_order_id: &str,
) -> Result<()> {
    trace!("Deleting dependent order with id {}", client_order_id);
    let dependent_order_collection: Collection<DependentOrder> = db.collection("dependent-orders");
    dependent_order_collection
        .find_one_and_delete(doc! {"client_order_id": client_order_id}, None)
        .await
        .context("Failed to find and delete dependent order")?;
    Ok(())
}
