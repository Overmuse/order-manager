use super::OrderManager;
use crate::db;
use crate::event_sender::Event;
use crate::types::{split_lot, Allocation, Lot, Status};
use alpaca::{Event as AlpacaEvent, OrderEvent, Side};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use tracing::debug;
use trading_base::Amount;
use uuid::Uuid;

impl OrderManager {
    #[tracing::instrument(skip(self, event), fields(id = %event.order.client_order_id))]
    pub async fn handle_order_update(&self, event: OrderEvent) -> Result<()> {
        debug!("Handling order update");
        let id = Uuid::parse_str(&event.order.client_order_id)?;
        let ticker = &event.order.symbol;
        let price = event.order.filled_avg_price.unwrap_or(Decimal::ONE);
        let qty = match event.order.side {
            Side::Buy => {
                Decimal::from_usize(event.order.filled_qty).ok_or_else(|| anyhow!("Failed to convert Decimal"))?
            }
            Side::Sell => {
                -Decimal::from_usize(event.order.filled_qty).ok_or_else(|| anyhow!("Failed to convert Decimal"))?
            }
        };
        debug!(status = ?event.event, "Order status update");
        match event.event {
            AlpacaEvent::New => {
                db::update_pending_trade_status(self.db_client.as_ref(), id, Status::Accepted).await?;
            }
            AlpacaEvent::Canceled { .. } | AlpacaEvent::Expired { .. } | AlpacaEvent::Rejected { .. } => {
                db::update_pending_trade_status(self.db_client.as_ref(), id, Status::Dead).await?;
                db::delete_pending_trade_by_id(self.db_client.as_ref(), id)
                    .await
                    .context("Failed to delete pending trade")?;
            }
            AlpacaEvent::Fill { timestamp, .. } => {
                debug!("Order filled");
                db::update_pending_trade_status(self.db_client.as_ref(), id, Status::Filled).await?;
                let new_lot = self
                    .make_lot(id, ticker, timestamp, price, qty)
                    .await
                    .context("Failed to make lot")?;
                debug!("Deleting pending trade");
                db::delete_pending_trade_by_id(self.db_client.as_ref(), id)
                    .await
                    .context("Failed to delete pending trade")?;
                debug!("Saving lot");
                db::save_lot(self.db_client.as_ref(), &new_lot)
                    .await
                    .context("Failed to save lot")?;
                self.event_sender.send(Event::Lot(new_lot.clone())).await?;
                debug!("Assigning lot");
                self.assign_lot(new_lot).await.context("Failed to assign lot")?;
                debug!("Triggering dependent trades");
                self.trigger_dependent_trades(id)
                    .await
                    .context("Failed to trigger dependent-trades")?
            }
            AlpacaEvent::PartialFill { timestamp, .. } => {
                db::update_pending_trade_status(self.db_client.as_ref(), id, Status::PartiallyFilled).await?;
                let pending_trade = db::get_pending_trade_by_id(self.db_client.as_ref(), id)
                    .await
                    .context("Failed to get pending trade")?
                    .ok_or_else(|| anyhow!("Partial fill received without seeing `new` event"))?;
                let filled_qty = match event.order.side {
                    Side::Buy => event
                        .order
                        .filled_qty
                        .to_isize()
                        .ok_or_else(|| anyhow!("Failed to convert Decimal"))?,
                    Side::Sell => {
                        -(event
                            .order
                            .filled_qty
                            .to_isize()
                            .ok_or_else(|| anyhow!("Failed to convert Decimal"))?)
                    }
                };
                let pending_qty = pending_trade.quantity - filled_qty as i32;
                db::update_pending_trade_quantity(self.db_client.as_ref(), id, pending_qty)
                    .await
                    .context("Failed to update pending trade quantity")?;
                let new_lot = self
                    .make_lot(id, ticker, timestamp, price, qty)
                    .await
                    .context("Failed to make lot")?;
                db::save_lot(self.db_client.as_ref(), &new_lot)
                    .await
                    .context("Failed to make lot")?;
                self.event_sender.send(Event::Lot(new_lot.clone())).await?;
                self.assign_lot(new_lot).await.context("Failed to assign lot")?;
            }
            _ => (),
        }
        self.reconcile().await
    }

    #[tracing::instrument(skip(self, ticker, timestamp, price, position_quantity))]
    async fn make_lot(
        &self,
        id: Uuid,
        ticker: &str,
        timestamp: DateTime<Utc>,
        price: Decimal,
        position_quantity: Decimal,
    ) -> Result<Lot> {
        let previous_lots = db::get_lots_by_order_id(self.db_client.as_ref(), id)
            .await
            .context("Failed to get lots")?;
        let (previous_quantity, previous_price) = aggregate_previous_lots(&previous_lots);
        let (quantity, price) =
            calculate_lot_quantity_and_price(previous_quantity, previous_price, position_quantity, price);
        Ok(Lot::new(id, ticker.to_string(), timestamp, price, quantity))
    }

    #[tracing::instrument(skip(self, lot))]
    async fn assign_lot(&self, lot: Lot) -> Result<()> {
        let claims = db::get_claims_by_ticker(self.db_client.as_ref(), &lot.ticker)
            .await
            .context("Failed to get claim")?;
        let allocations = split_lot(&claims, &lot);
        for allocation in allocations {
            self.adjust_claim(&allocation).await.context("Failed to adjust claim")?;
            db::save_allocation(self.db_client.as_ref(), &allocation)
                .await
                .context("Failed to save allocation")?;
            self.event_sender.send(Event::Allocation(allocation)).await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, allocation))]
    async fn adjust_claim(&self, allocation: &Allocation) -> Result<()> {
        if let Some(claim_id) = allocation.claim_id {
            let claim = db::get_claim_by_id(self.db_client.as_ref(), claim_id)
                .await
                .context("Failed to get claim")?;
            let amount = calculate_claim_adjustment_amount(&claim.amount, allocation);
            db::update_claim_amount(self.db_client.as_ref(), claim_id, &amount)
                .await
                .context("Failed to update claim amount")?;
        };
        Ok(())
    }
}

fn calculate_claim_adjustment_amount(claim_amount: &Amount, allocation: &Allocation) -> Amount {
    match claim_amount {
        Amount::Dollars(dollars) => {
            let new_dollars = dollars - allocation.basis;
            Amount::Dollars(new_dollars)
        }
        Amount::Shares(shares) => {
            let new_shares = shares - allocation.shares;
            Amount::Shares(new_shares)
        }
        _ => unimplemented!(),
    }
}

fn aggregate_previous_lots(lots: &[Lot]) -> (Decimal, Decimal) {
    lots.iter().fold((Decimal::ZERO, Decimal::ONE), |(shares, price), lot| {
        (
            shares + lot.shares,
            ((price * shares + lot.price * lot.shares) / (shares + lot.shares)).round_dp(8),
        )
    })
}

/// Calculates the incremental lot quantity and price given an old cumulative share count and fill
/// price and new ones.
fn calculate_lot_quantity_and_price(
    old_quantity: Decimal,
    old_price: Decimal,
    new_quantity: Decimal,
    new_price: Decimal,
) -> (Decimal, Decimal) {
    let quantity = new_quantity - old_quantity;
    let price = ((new_price * new_quantity - old_price * old_quantity) / quantity).round_dp(8);
    (quantity, price)
}

#[cfg(test)]
mod test {
    use super::*;

    const ORDER_ID: Uuid = Uuid::nil();

    fn lot(shares: Decimal, price: Decimal) -> Lot {
        Lot::new(ORDER_ID, "TICK".to_string(), Utc::now(), price, shares)
    }

    #[test]
    fn test_lot_aggregation() {
        let previous_lots = [
            lot(Decimal::ONE, Decimal::new(111, 0)),
            lot(Decimal::TWO, Decimal::new(101, 0)),
            lot(Decimal::TEN, Decimal::new(100, 0)),
        ];
        let (qty, price) = aggregate_previous_lots(&previous_lots);
        assert_eq!(qty, Decimal::new(13, 0));
        assert_eq!(price, Decimal::new(101, 0));
    }

    #[test]
    fn test_lot_calculation() {
        let (q, p) = calculate_lot_quantity_and_price(Decimal::ZERO, Decimal::ONE, Decimal::TWO, Decimal::ONE_HUNDRED);
        assert_eq!(q, Decimal::TWO);
        assert_eq!(p, Decimal::ONE_HUNDRED);

        let (q2, p2) = calculate_lot_quantity_and_price(q, p, Decimal::TEN, Decimal::ONE_THOUSAND);
        assert_eq!(q2, Decimal::new(8, 0));
        assert_eq!(p2, Decimal::new(1225, 0));
    }
}
