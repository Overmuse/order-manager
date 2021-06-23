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
                self.pending_orders.remove(&event.order.client_order_id);
            }
            Event::Expired { .. } => {
                debug!("Order expired");
                self.pending_orders.remove(&event.order.client_order_id);
            }
            Event::Fill {
                price, timestamp, ..
            } => {
                debug!("Fill");
                let new_lot = self.make_lot(&id, ticker, timestamp, price, qty).await?;
                self.pending_orders.remove(&event.order.client_order_id);
                self.save_lot(new_lot.clone()).await?;
                self.assign_lot(new_lot).await?;
                self.trigger_dependent_orders(&id)
                    .context("Failed to trigger dependent-orders")?
            }
            Event::PartialFill {
                price, timestamp, ..
            } => {
                debug!("Partial fill");
                let pending_order = self
                    .pending_orders
                    .get_mut(&event.order.client_order_id)
                    .ok_or_else(|| anyhow!("Partial fill received without seeing `new` event"))?;
                let filled_qty = match event.order.side {
                    Side::Buy => event.order.filled_qty.to_isize().unwrap(),
                    Side::Sell => -(event.order.filled_qty.to_isize().unwrap()),
                };
                pending_order.pending_qty = pending_order.qty - filled_qty;
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
        if !claims.is_empty() {
            let allocations = split_lot(&claims, &lot);
            self.delete_claims_from_allocations(&lot.ticker, &allocations)
                .await?;
            for allocation in allocations {
                self.save_allocation(allocation).await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, allocations))]
    async fn delete_claims_from_allocations(
        &mut self,
        ticker: &str,
        allocations: &[Allocation],
    ) -> Result<()> {
        let mut claims = self.get_claims_by_ticker(ticker).await?;
        for allocation in allocations {
            for mut claim in claims.iter_mut() {
                if allocation.claim_id == Some(claim.id) {
                    match claim.amount {
                        AmountSpec::Dollars(dollars) => {
                            let new_dollars = dollars - allocation.basis;
                            claim.amount = AmountSpec::Dollars(new_dollars);
                        }
                        AmountSpec::Shares(shares) => {
                            let new_shares = shares - allocation.shares;
                            claim.amount = AmountSpec::Shares(new_shares);
                        }
                        _ => unimplemented!(),
                    }
                }
            }
        }
        for claim in claims {
            let should_delete = match claim.amount {
                AmountSpec::Dollars(dollars) => dollars.is_zero(),
                AmountSpec::Shares(shares) => shares.is_zero(),
                _ => unimplemented!(),
            };
            if should_delete {
                self.delete_claim_by_id(claim.id).await?
            }
        }

        Ok(())
    }
}
