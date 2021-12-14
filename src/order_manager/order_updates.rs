use super::OrderManager;
use crate::db;
use crate::event_sender::Event;
use crate::types::{split_lot, Allocation, Lot};
use alpaca::{Event as AlpacaEvent, OrderEvent, Side};
use anyhow::{Context, Result};
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
        let ticker = event.order.symbol.clone();
        debug!(status = ?event.event, "Order status update");
        match event.event {
            AlpacaEvent::New => {
                db::save_trade(self.db_client.as_ref(), From::from(event.order)).await?;
            }
            AlpacaEvent::Canceled { .. } => {
                db::save_trade(self.db_client.as_ref(), From::from(event.order)).await?;
            }
            AlpacaEvent::Expired { .. } | AlpacaEvent::Rejected { .. } => {
                db::save_trade(self.db_client.as_ref(), From::from(event.order)).await?;
            }
            AlpacaEvent::Fill {
                timestamp, qty, price, ..
            } => {
                debug!("Order filled");
                let qty = match event.order.side {
                    Side::Buy => Decimal::from_isize(qty).unwrap(),
                    Side::Sell => -Decimal::from_isize(qty).unwrap(),
                };
                db::save_trade(self.db_client.as_ref(), From::from(event.order)).await?;
                let new_lot = self
                    .make_lot(id, &ticker, timestamp, price, qty)
                    .await
                    .context("Failed to make lot")?;
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
            AlpacaEvent::PartialFill {
                timestamp, qty, price, ..
            } => {
                let qty = match event.order.side {
                    Side::Buy => Decimal::from_isize(qty).unwrap(),
                    Side::Sell => -Decimal::from_isize(qty).unwrap(),
                };
                db::save_trade(self.db_client.as_ref(), From::from(event.order)).await?;
                let new_lot = self
                    .make_lot(id, &ticker, timestamp, price, qty)
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
        Ok(())
    }

    #[tracing::instrument(skip(self, ticker, timestamp, price, quantity))]
    async fn make_lot(
        &self,
        id: Uuid,
        ticker: &str,
        timestamp: DateTime<Utc>,
        price: Decimal,
        quantity: Decimal,
    ) -> Result<Lot> {
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
