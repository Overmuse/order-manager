use super::{split_lot, Allocation, Lot, OrderManager};
use alpaca::{Event, OrderEvent, Side};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use position_intents::AmountSpec;
use rust_decimal::prelude::*;
use tracing::debug;

impl OrderManager {
    #[tracing::instrument(skip(self, event), fields(id = %event.order.client_order_id))]
    pub(super) async fn handle_order_update(&mut self, event: OrderEvent) -> Result<()> {
        debug!("Handling order update");
        let id = event.order.client_order_id.clone();
        let ticker = event.order.symbol.clone();
        let qty = match event.order.side {
            Side::Buy => Decimal::from_usize(event.order.qty).unwrap(),
            Side::Sell => -Decimal::from_usize(event.order.qty).unwrap(),
        };
        //self.upsert_order(&event.order).await?;
        //self.delete_pending_order_by_id(id.clone()).await?;
        match event.event {
            Event::Canceled { .. } => {
                debug!("Order cancelled");
                self.delete_pending_order_by_id(&event.order.client_order_id)
                    .await?;
            }
            Event::Expired { .. } => {
                debug!("Order expired");
                self.delete_pending_order_by_id(&event.order.client_order_id)
                    .await?;
            }
            Event::Fill {
                price, timestamp, ..
            } => {
                debug!("Fill");
                let new_lot = self.make_lot(&id, ticker, timestamp, price, qty).await?;
                debug!("Deleting pending order");
                self.delete_pending_order_by_id(&event.order.client_order_id)
                    .await?;
                debug!("Saving lot");
                self.save_lot(new_lot.clone()).await?;
                debug!("Assigning lot");
                self.assign_lot(new_lot).await?;
                debug!("Triggering dependent orders");
                self.trigger_dependent_orders(&id)
                    .await
                    .context("Failed to trigger dependent-orders")?
            }
            Event::PartialFill {
                price, timestamp, ..
            } => {
                debug!("Partial fill");
                let pending_order = self
                    .get_pending_order_by_id(&event.order.client_order_id)
                    .await?
                    .ok_or_else(|| anyhow!("Partial fill received without seeing `new` event"))?;
                let filled_qty = match event.order.side {
                    Side::Buy => event.order.filled_qty.to_isize().unwrap(),
                    Side::Sell => -(event.order.filled_qty.to_isize().unwrap()),
                };
                let pending_qty = pending_order.qty - filled_qty as i32;
                self.update_pending_order_qty(&id, pending_qty).await?;
                let new_lot = self.make_lot(&id, ticker, timestamp, price, qty).await?;
                self.save_lot(new_lot.clone()).await?;
                self.assign_lot(new_lot).await?;
            }
            _ => (),
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn make_lot(
        &self,
        id: &str,
        ticker: String,
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
            ticker,
            timestamp,
            new_price,
            new_quantity,
        ))
    }

    #[tracing::instrument(skip(self))]
    async fn previous_fill_data(&self, order_id: &str) -> Result<(Decimal, Decimal)> {
        let previous_lots = self.get_lots_by_order_id(order_id).await?;
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

    #[tracing::instrument(skip(self))]
    async fn assign_lot(&mut self, lot: Lot) -> Result<()> {
        let claims = self.get_claims_by_ticker(&lot.ticker).await?;
        let allocations = split_lot(&claims, &lot);
        for allocation in allocations {
            self.adjust_claim(&allocation).await?;
            self.save_allocation(allocation).await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, allocation))]
    async fn adjust_claim(&self, allocation: &Allocation) -> Result<()> {
        if let Some(claim_id) = allocation.claim_id {
            let claim = self.get_claim_by_id(claim_id).await?;
            let amount = match claim.amount {
                AmountSpec::Dollars(dollars) => {
                    let new_dollars = dollars - allocation.basis;
                    AmountSpec::Dollars(new_dollars)
                }
                AmountSpec::Shares(shares) => {
                    let new_shares = shares - allocation.shares;
                    AmountSpec::Shares(new_shares)
                }
                _ => unimplemented!(),
            };
            self.update_claim_amount(claim_id, amount).await?;
        };
        Ok(())
    }
}
