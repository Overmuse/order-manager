use super::OrderManager;
use crate::db;
use anyhow::{Context, Result};
use tracing::debug;
use uuid::Uuid;

impl OrderManager {
    #[tracing::instrument(skip(self, id))]
    pub async fn trigger_dependent_trades(&self, id: Uuid) -> Result<()> {
        let trades = db::take_dependent_trades(&*self.db_client.read().await, id)
            .await
            .context("Failed to take and delete dependent trader")?;
        if !trades.is_empty() {
            debug!(%id, "Triggering dependent trades");
            for trade in trades {
                self.send_trade(trade).await?
            }
        }
        Ok(())
    }
}
