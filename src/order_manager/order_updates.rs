use super::OrderManager;
use crate::db;
use crate::types::{split_lot, Allocation, Lot};
use alpaca::{Event, OrderEvent, Side};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use tracing::debug;
use trading_base::Amount;

impl OrderManager {
    #[tracing::instrument(skip(self, event), fields(id = %event.order.client_order_id))]
    pub async fn handle_order_update(&self, event: OrderEvent) -> Result<()> {
        debug!("Handling order update");
        let id = &event.order.client_order_id;
        let ticker = &event.order.symbol;
        let qty = match event.order.side {
            Side::Buy => Decimal::from_usize(event.order.qty).unwrap(),
            Side::Sell => -Decimal::from_usize(event.order.qty).unwrap(),
        };
        debug!(status = ?event.event, "Order status update");
        match event.event {
            Event::Canceled { .. } | Event::Expired { .. } | Event::Rejected { .. } => {
                db::delete_pending_order_by_id(self.db_client.as_ref(), id)
                    .await
                    .context("Failed to delete pending order")?;
            }
            Event::Fill {
                price, timestamp, ..
            } => {
                debug!("Order filled");
                let new_lot = self
                    .make_lot(&id, ticker, timestamp, price, qty)
                    .await
                    .context("Failed to make lot")?;
                debug!("Deleting pending order");
                db::delete_pending_order_by_id(
                    self.db_client.as_ref(),
                    &event.order.client_order_id,
                )
                .await
                .context("Failed to delete pending order")?;
                debug!("Saving lot");
                db::save_lot(self.db_client.as_ref(), &new_lot)
                    .await
                    .context("Failed to save lot")?;
                debug!("Assigning lot");
                self.assign_lot(new_lot)
                    .await
                    .context("Failed to assign lot")?;
                debug!("Triggering dependent orders");
                self.trigger_dependent_orders(&id)
                    .await
                    .context("Failed to trigger dependent-orders")?
            }
            Event::PartialFill {
                price, timestamp, ..
            } => {
                let pending_order = db::get_pending_order_by_id(self.db_client.as_ref(), id)
                    .await
                    .context("Failed to get pending order")?
                    .ok_or_else(|| anyhow!("Partial fill received without seeing `new` event"))?;
                let filled_qty = match event.order.side {
                    Side::Buy => event.order.filled_qty.to_isize().unwrap(),
                    Side::Sell => -(event.order.filled_qty.to_isize().unwrap()),
                };
                let pending_qty = pending_order.qty - filled_qty as i32;
                db::update_pending_order_qty(self.db_client.as_ref(), &id, pending_qty)
                    .await
                    .context("Failed to update pending order quantity")?;
                let new_lot = self
                    .make_lot(&id, ticker, timestamp, price, qty)
                    .await
                    .context("Failed to make lot")?;
                db::save_lot(self.db_client.as_ref(), &new_lot)
                    .await
                    .context("Failed to make lot")?;
                self.assign_lot(new_lot)
                    .await
                    .context("Failed to assign lot")?;
            }
            _ => (),
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, ticker, timestamp, price, position_quantity))]
    async fn make_lot(
        &self,
        id: &str,
        ticker: &str,
        timestamp: DateTime<Utc>,
        price: Decimal,
        position_quantity: Decimal,
    ) -> Result<Lot> {
        let (previous_quantity, previous_price) = self.previous_fill_data(id).await?;
        let new_quantity = position_quantity - previous_quantity;
        let new_price =
            (price * position_quantity - previous_quantity * previous_price) / new_quantity;
        Ok(Lot::new(
            id.to_string(),
            ticker.to_string(),
            timestamp,
            new_price,
            new_quantity,
        ))
    }

    #[tracing::instrument(skip(self, order_id))]
    async fn previous_fill_data(&self, order_id: &str) -> Result<(Decimal, Decimal)> {
        let previous_lots = db::get_lots_by_order_id(self.db_client.as_ref(), order_id)
            .await
            .context("Failed to get lots")?;
        let (prev_qty, prev_price) = previous_lots.iter().fold(
            (Decimal::new(0, 0), Decimal::new(1, 0)),
            |(shares, price), lot| {
                (
                    shares + lot.shares,
                    (price * shares + lot.price) / (shares + lot.shares),
                )
            },
        );
        Ok((prev_qty, prev_price))
    }

    #[tracing::instrument(skip(self, lot))]
    async fn assign_lot(&self, lot: Lot) -> Result<()> {
        let claims = db::get_claims_by_ticker(self.db_client.as_ref(), &lot.ticker)
            .await
            .context("Failed to get claim")?;
        let allocations = split_lot(&claims, &lot);
        for allocation in allocations {
            self.adjust_claim(&allocation)
                .await
                .context("Failed to adjust claim")?;
            db::save_allocation(self.db_client.as_ref(), &allocation)
                .await
                .context("Failed to save allocation")?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, allocation))]
    async fn adjust_claim(&self, allocation: &Allocation) -> Result<()> {
        if let Some(claim_id) = allocation.claim_id {
            let claim = db::get_claim_by_id(self.db_client.as_ref(), claim_id)
                .await
                .context("Failed to get claim")?;
            let amount = match claim.amount {
                Amount::Dollars(dollars) => {
                    let new_dollars = dollars - allocation.basis;
                    Amount::Dollars(new_dollars)
                }
                Amount::Shares(shares) => {
                    let new_shares = shares - allocation.shares;
                    Amount::Shares(new_shares)
                }
                _ => unimplemented!(),
            };
            db::update_claim_amount(self.db_client.as_ref(), claim_id, &amount)
                .await
                .context("Failed to update claim amount")?;
        };
        Ok(())
    }
}
