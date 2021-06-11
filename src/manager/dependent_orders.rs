use super::OrderManager;
use anyhow::{Context, Result};
use tracing::debug;

impl OrderManager {
    #[tracing::instrument(skip(self, id))]
    pub(super) fn trigger_dependent_orders(&mut self, id: &str) -> Result<()> {
        let orders = self.dependent_orders.remove(id);
        if let Some(orders) = orders {
            debug!("Triggering dependent orders");
            for order in orders {
                self.order_sender
                    .send(order)
                    .context("Failed to send dependent-order to OrderSender")?
            }
        }
        Ok(())
    }
}
