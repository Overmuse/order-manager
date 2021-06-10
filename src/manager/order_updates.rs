use super::{split_lot, Allocation, Claim, Lot, OrderManager};
use alpaca::{Event, OrderEvent, Side};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use position_intents::AmountSpec;
use rust_decimal::prelude::*;
use tracing::trace;

impl OrderManager {
    pub(super) async fn handle_order_update(&mut self, event: OrderEvent) -> Result<()> {
        trace!("Handling order update: {:?}", event);
        let id = event.order.client_order_id.clone();
        let ticker = event.order.symbol.clone();
        let qty = match event.order.side {
            Side::Buy => Decimal::from_usize(event.order.qty).unwrap(),
            Side::Sell => -Decimal::from_usize(event.order.qty).unwrap(),
        };
        //self.upsert_order(&event.order).await?;
        //self.delete_pending_order_by_id(id.clone()).await?;
        match event.event {
            Event::Fill {
                price, timestamp, ..
            } => {
                let new_lot = self.make_lot(&id, ticker, timestamp, price, qty);
                self.save_partially_filled_order_lot(id.clone(), new_lot.clone());
                self.assign_lot(new_lot);
                self.trigger_dependent_orders(&id)
                    .context("Failed to trigger dependent-orders")?
            }
            Event::PartialFill {
                price, timestamp, ..
            } => {
                let new_lot = self.make_lot(&id, ticker, timestamp, price, qty);
                self.save_partially_filled_order_lot(id, new_lot.clone());
                self.assign_lot(new_lot);
            }
            _ => (),
        }
        Ok(())
    }

    fn make_lot(
        &self,
        id: &str,
        ticker: String,
        timestamp: DateTime<Utc>,
        price: Decimal,
        position_quantity: Decimal,
    ) -> Lot {
        let (previous_quantity, previous_price) = self.previous_fill_data(id);
        let new_quantity = position_quantity - previous_quantity;
        let new_price =
            (price * position_quantity - previous_quantity * previous_price) / new_quantity;
        Lot::new(ticker, timestamp, new_price, new_quantity)
    }

    fn previous_fill_data(&self, order_id: &str) -> (Decimal, Decimal) {
        let previous_lots = self.partially_filled_orders.get_vec(order_id);
        previous_lots
            .map(|lots| {
                lots.iter().fold(
                    (Decimal::new(0, 0), Decimal::new(1, 0)),
                    |(shares, price), lot| {
                        (
                            shares + lot.shares,
                            (price * shares + lot.price) / (shares + lot.shares),
                        )
                    },
                )
            })
            .unwrap_or_else(|| (Decimal::new(0, 0), Decimal::new(1, 0)))
    }

    fn save_partially_filled_order_lot(&mut self, order_id: String, lot: Lot) {
        self.partially_filled_orders.insert(order_id, lot);
    }

    fn assign_lot(&mut self, lot: Lot) {
        trace!("Assigning lot to claims: {:?}", lot);
        let claims = self.get_claims(&lot.ticker);
        trace!("Current claims: {:?}", claims);
        if let Some(claims) = claims {
            let allocations = split_lot(&claims, &lot);
            trace!("Allocations: {:?}", allocations);
            self.delete_claims_from_allocations(&allocations);
            self.save_allocations(&allocations);
        }
    }

    fn get_claims(&self, ticker: &str) -> Option<&Vec<Claim>> {
        self.unfilled_claims.get_vec(ticker)
    }

    fn delete_claims_from_allocations(&mut self, allocations: &[Allocation]) {
        for allocation in allocations {
            let claims = self.unfilled_claims.get_vec_mut(&allocation.ticker);
            if let Some(claims) = claims {
                for claim in claims {
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
        }
        self.unfilled_claims.retain(|_, claim| match claim.amount {
            AmountSpec::Dollars(dollars) => !dollars.is_zero(),
            AmountSpec::Shares(shares) => !shares.is_zero(),
            _ => unimplemented!(),
        });
        trace!("Remaining claims: {:?}", self.unfilled_claims);
    }

    fn save_allocations(&mut self, allocations: &[Allocation]) {
        self.allocations.extend_from_slice(allocations)
    }
}
