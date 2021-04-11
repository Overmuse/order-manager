use alpaca::orders::OrderIntent;
use anyhow::{Context, Result};
use bson::doc;
use mongodb::{Collection, Cursor, Database};
use tracing::trace;

#[tracing::instrument(skip(db))]
pub(crate) async fn pending_order_intents(db: &Database) -> Result<Cursor<OrderIntent>> {
    trace!("Fetching pending order intents");
    let order_intent_collection: Collection<OrderIntent> = db.collection("order-intents");
    let pending_order_intents = order_intent_collection
        .find(None, None)
        .await
        .context("Failed to lookup pending order-intents")?;
    Ok(pending_order_intents)
}

#[tracing::instrument(skip(db))]
pub(crate) async fn pending_order_intents_by_ticker(
    db: &Database,
    ticker: &str,
) -> Result<Cursor<OrderIntent>> {
    trace!("Fetching pending order intents for {}", ticker);
    let order_intent_collection: Collection<OrderIntent> = db.collection("order-intents");
    let pending_order_intents = order_intent_collection
        .find(doc! {"symbol": ticker}, None)
        .await
        .context(format!(
            "Failed to lookup pending order-intents for {}",
            ticker
        ))?;
    Ok(pending_order_intents)
}

#[tracing::instrument(skip(db))]
pub(crate) async fn save_order_intent(db: &Database, order_intent: OrderIntent) -> Result<()> {
    trace!("Saving order intent: {:?}", order_intent);
    let order_intent_collection: Collection<OrderIntent> = db.collection("order-intents");
    order_intent_collection
        .insert_one(order_intent, None)
        .await
        .context("Failed to insert order-intent into database")?;
    Ok(())
}

#[tracing::instrument(skip(db))]
pub(crate) async fn delete_order_intent_by_id(db: &Database, client_order_id: &str) -> Result<()> {
    trace!("Deleting order intent with id {}", client_order_id);
    let order_intent_collection: Collection<OrderIntent> = db.collection("order-intents");
    order_intent_collection
        .find_one_and_delete(doc! {"client_order_id": client_order_id}, None)
        .await
        .context("Failed to find and delete order-intent")?;
    Ok(())
}
