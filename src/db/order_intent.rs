use alpaca::orders::OrderIntent;
use anyhow::{Context, Result};
use bson::doc;
use mongodb::{Collection, Cursor, Database};
use tracing::trace;

#[tracing::instrument(skip(db))]
pub(crate) async fn pending_order_intents(db: &Database) -> Result<Cursor<OrderIntent>> {
    trace!("Fetching pending order intents");
    let order_intent_collection: Collection<OrderIntent> = db.collection_with_type("order-intents");
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
    let order_intent_collection: Collection<OrderIntent> = db.collection_with_type("order-intents");
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
pub(crate) async fn save_order_intents(
    db: &Database,
    order_intents: Vec<OrderIntent>,
) -> Result<()> {
    trace!("Saving order intents: {:?}", order_intents);
    let order_intent_collection: Collection<OrderIntent> = db.collection_with_type("order-intents");
    order_intent_collection
        .insert_many(order_intents, None)
        .await
        .context("Failed to insert order-intents into database")?;
    Ok(())
}

#[tracing::instrument(skip(db))]
pub(crate) async fn delete_order_intent_by_id(db: &Database, client_order_id: &str) -> Result<()> {
    trace!("Deleteing order intent with id {}", client_order_id);
    let order_intent_collection: Collection<OrderIntent> = db.collection_with_type("order-intents");
    order_intent_collection
        .find_one_and_delete(doc! {"client_order_id": client_order_id}, None)
        .await
        .context("Failed to find and delete order-intent")?;
    Ok(())
}
