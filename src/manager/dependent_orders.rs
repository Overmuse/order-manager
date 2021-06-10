use super::OrderManager;
use anyhow::{Context, Result};
use tracing::trace;

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub(super) fn trigger_dependent_orders(&mut self, id: &str) -> Result<()> {
        trace!("Triggering orders");
        let orders = self.dependent_orders.remove(id);
        if let Some(orders) = orders {
            for order in orders {
                self.order_sender
                    .send(order)
                    .context("Failed to send dependent-order to OrderSender")?
            }
        }
        Ok(())
    }
}
