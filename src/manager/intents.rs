use super::{Claim, OrderManager, Owner, Position};
use alpaca::{orders::OrderIntent, OrderType, Side};
use anyhow::{Context, Result};
use chrono::Utc;
use num_traits::Signed;
use position_intents::{AmountSpec, PositionIntent};
use rust_decimal::prelude::*;
use tracing::{debug, trace};
use uuid::Uuid;

impl OrderManager {
    #[tracing::instrument(skip(self, intent), fields(id = %intent.id))]
    pub(super) fn handle_position_intent(&mut self, intent: PositionIntent) -> Result<()> {
        trace!("Handling position intent: {:?}", intent);
        if let Some(dt) = intent.before {
            if dt >= Utc::now() {
                // Intent has already expired, so don't do anything
                debug!("Expired intent: {:?}", intent);
                return Ok(());
            }
        }
        if let Some(dt) = intent.after {
            if dt >= Utc::now() {
                // Not ready to transmit intent yet
                return self.schedule_position_intent(intent);
            }
        }
        self.transmit_position_intent(intent)
    }

    #[tracing::instrument(skip(self, intent), fields(id = %intent.id))]
    fn transmit_position_intent(&mut self, intent: PositionIntent) -> Result<()> {
        let positions = self.get_positions(&intent.ticker);
        trace!("Current position: {:?}", positions);
        match self.make_orders(&intent, &positions) {
            (None, None, None) => Ok(()),
            (Some(claim), Some(sent), None) => {
                self.unfilled_claims.insert(intent.ticker, claim);
                Ok(self.order_sender.send(sent)?)
            }
            (Some(claim), Some(sent), Some(saved)) => {
                self.dependent_orders
                    .insert(sent.client_order_id.clone().unwrap(), saved);
                self.unfilled_claims.insert(intent.ticker, claim);
                Ok(self.order_sender.send(sent)?)
            }
            _ => unreachable!(),
        }
    }

    #[tracing::instrument(skip(self, intent), fields(id = %intent.id))]
    fn schedule_position_intent(&self, intent: PositionIntent) -> Result<()> {
        let res = self
            .scheduler_sender
            .send(intent)
            .context("Failed to send intent to scheduler")?;
        Ok(res)
    }

    #[tracing::instrument(skip(self))]
    fn get_positions(&self, ticker: &str) -> Vec<Position> {
        let mut allocations = self.allocations.clone();
        trace!("Current allocations: {:?}", allocations);
        allocations.retain(|x| &x.ticker == ticker);
        allocations
            .group_by(|a1, a2| a1.owner == a2.owner)
            .map(|allocs| Position::from_allocations(allocs))
            .collect()
    }

    fn make_orders(
        &self,
        intent: &PositionIntent,
        positions: &[Position],
    ) -> (Option<Claim>, Option<OrderIntent>, Option<OrderIntent>) {
        let (strategy_shares, total_shares) = positions.iter().fold(
            (Decimal::ZERO, Decimal::ZERO),
            |(strat_shares, all_shares), pos| {
                if pos.owner == Owner::Strategy(intent.strategy.clone()) {
                    (strat_shares + pos.shares, all_shares + pos.shares)
                } else {
                    (strat_shares, all_shares + pos.shares)
                }
            },
        );

        let diff_shares = match intent.amount {
            AmountSpec::Dollars(dollars) => {
                // TODO: fix the below so we can always have a price
                let price = intent
                    .decision_price
                    .or(intent.limit_price)
                    .expect("Need either limit price or decision price");
                dollars / price - strategy_shares
            }
            AmountSpec::Shares(shares) => shares - strategy_shares,
            AmountSpec::Zero => -strategy_shares,
            _ => unimplemented!(),
        };
        if diff_shares.is_zero() {
            (None, None, None)
        } else {
            let claim = Claim::new(intent.strategy.clone(), AmountSpec::Shares(diff_shares));
            let signum_product = total_shares.signum() * (diff_shares + total_shares).signum();
            if !signum_product.is_sign_negative() {
                let trade =
                    make_order_intent(&intent.id, &intent.ticker, diff_shares, intent.limit_price);
                (Some(claim), Some(trade), None)
            } else {
                let sent = make_order_intent(
                    &intent.id,
                    &intent.ticker,
                    -total_shares,
                    intent.limit_price,
                );
                let saved = make_order_intent(
                    &intent.id,
                    &intent.ticker,
                    diff_shares + total_shares,
                    intent.limit_price,
                );
                (Some(claim), Some(sent), Some(saved))
            }
        }
    }
}

fn make_order_intent(
    prefix: &str,
    ticker: &str,
    qty: Decimal,
    limit_price: Option<Decimal>,
) -> OrderIntent {
    let side = if qty > Decimal::ZERO {
        Side::Buy
    } else {
        Side::Sell
    };
    let order_type = match limit_price {
        Some(limit) => OrderType::Limit { limit_price: limit },
        None => OrderType::Market,
    };
    OrderIntent::new(ticker)
        .client_order_id(format!("{}_{}", prefix, Uuid::new_v4().to_string()))
        .qty(qty.abs().ceil().to_usize().unwrap())
        .order_type(order_type)
        .side(side)
}
