use super::OrderManager;
use crate::db;
use anyhow::{Context, Result};
use tracing::debug;

impl OrderManager {
    #[tracing::instrument(skip(self, id))]
    pub(super) async fn trigger_dependent_orders(&mut self, id: &str) -> Result<()> {
        let orders = db::take_dependent_orders(self.db_client.clone(), id)
            .await
            .context("Failed to take and delete dependent order")?;
        if !orders.is_empty() {
            debug!(id, "Triggering dependent orders");
            for order in orders {
                self.order_sender
                    .send(order)
                    .context("Failed to send dependent-order to OrderSender")?
            }
        }
        Ok(())
    }
}
