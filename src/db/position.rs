use crate::Position;
use anyhow::{Context, Result};
use bson::doc;
use mongodb::{Collection, Cursor, Database};
use tracing::trace;

#[tracing::instrument(skip(db))]
pub(crate) async fn delete_position_by_ticker(db: &Database, ticker: &str) -> Result<()> {
    trace!("Deleting position for ticker {}", ticker);
    let position_collection: Collection<Position> = db.collection("positions");
    position_collection
        .find_one_and_delete(doc! {"ticker": ticker}, None)
        .await
        .context(format!("Failed to find and delete position for {}", ticker))?;
    Ok(())
}

#[tracing::instrument(skip(db))]
pub(crate) async fn get_positions(db: &Database) -> Result<Cursor<Position>> {
    trace!("Fetching positions");
    let position_collection: Collection<Position> = db.collection("positions");
    position_collection
        .find(None, None)
        .await
        .context("Failed to find get positions")
}

#[tracing::instrument(skip(db))]
pub(crate) async fn get_position_by_ticker(
    db: &Database,
    ticker: &str,
) -> Result<Option<Position>> {
    trace!("Fetching position for ticker {}", ticker);
    let position_collection: Collection<Position> = db.collection("positions");
    position_collection
        .find_one(doc! {"ticker": ticker}, None)
        .await
        .context("Failed to find get positions")
}

#[tracing::instrument(skip(db))]
pub(crate) async fn upsert_position(db: &Database, position: Position) -> Result<()> {
    if position.qty == 0 {
        delete_position_by_ticker(db, &position.ticker).await?
    } else {
        trace!("Upserting position: {:?}", position);
        let position_collection: Collection<Position> = db.collection("positions");
        position_collection
            .find_one_and_replace(
                doc! {"ticker": &position.ticker},
                position,
                mongodb::options::FindOneAndReplaceOptions::builder()
                    .upsert(true)
                    .build(),
            )
            .await
            .context("Failed to update position")?;
    }
    Ok(())
}
