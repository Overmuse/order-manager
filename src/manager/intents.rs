use super::{Allocation, Claim, OrderManager, Owner, PendingOrder, Position};
use alpaca::{orders::OrderIntent, OrderType, Side};
use anyhow::{Context, Result};
use chrono::Utc;
use num_traits::Signed;
use position_intents::{AmountSpec, PositionIntent, TickerSpec, UpdatePolicy};
use rust_decimal::prelude::*;
use std::collections::HashSet;
use tracing::debug;
use uuid::Uuid;

impl OrderManager {
    #[tracing::instrument(skip(self, intent), fields(id = %intent.id))]
    pub(super) fn handle_position_intent(&mut self, intent: PositionIntent) -> Result<()> {
        debug!("Handling position intent");
        if let Some(dt) = intent.before {
            if dt <= Utc::now() {
                // Intent has already expired, so don't do anything
                debug!("Expired intent");
                return Ok(());
            }
        }
        if let Some(dt) = intent.after {
            if dt >= Utc::now() {
                // Not ready to transmit intent yet
                debug!("Sending intent to scheduler");
                return self.schedule_position_intent(intent);
            }
        }
        debug!("Transmitting intent");
        self.transmit_position_intent(intent)
    }

    #[tracing::instrument(skip(self, intent))]
    fn transmit_position_intent(&mut self, intent: PositionIntent) -> Result<()> {
        match &intent.ticker {
            TickerSpec::Ticker(ticker) => {
                let positions = self.get_positions(&ticker);
                match self.make_orders(&intent, &ticker, &positions) {
                    (None, None, None) => {
                        debug!("No trades generated");
                        Ok(())
                    }
                    (Some(claim), Some(sent), None) => {
                        self.unfilled_claims.insert(ticker.clone(), claim);
                        Ok(self.order_sender.send(sent)?)
                    }
                    (Some(claim), Some(sent), Some(saved)) => {
                        self.dependent_orders
                            .insert(sent.client_order_id.clone().unwrap(), saved);
                        self.unfilled_claims.insert(ticker.clone(), claim);
                        Ok(self.order_sender.send(sent)?)
                    }
                    _ => unreachable!(),
                }
            }
            TickerSpec::All => {
                // TODO: Clean up this branch
                let owned_tickers: HashSet<String> = self
                    .allocations
                    .iter()
                    .filter_map(|alloc| {
                        if alloc.owner
                            == Owner::Strategy(intent.strategy.clone(), intent.sub_strategy.clone())
                        {
                            Some(alloc.ticker.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                for ticker in owned_tickers {
                    let positions = self.get_positions(&ticker);
                    match self.make_orders(&intent, &ticker, &positions) {
                        (None, None, None) => {
                            debug!("No trades generated")
                        }
                        (Some(claim), Some(sent), None) => {
                            self.unfilled_claims.insert(ticker.clone(), claim);
                            let qty = match sent.side {
                                Side::Buy => sent.qty.to_isize().unwrap(),
                                Side::Sell => -(sent.qty.to_isize().unwrap()),
                            };
                            self.pending_orders.insert(
                                sent.client_order_id.clone().unwrap(),
                                PendingOrder::new(
                                    sent.client_order_id.clone().unwrap(),
                                    ticker,
                                    qty,
                                ),
                            );
                            self.order_sender.send(sent)?
                        }
                        (Some(claim), Some(sent), Some(saved)) => {
                            let qty = match sent.side {
                                Side::Buy => sent.qty.to_isize().unwrap(),
                                Side::Sell => -(sent.qty.to_isize().unwrap()),
                            };
                            self.dependent_orders
                                .insert(sent.client_order_id.clone().unwrap(), saved);
                            self.unfilled_claims.insert(ticker.clone(), claim);
                            self.pending_orders.insert(
                                sent.client_order_id.clone().unwrap(),
                                PendingOrder::new(
                                    sent.client_order_id.clone().unwrap(),
                                    ticker,
                                    qty,
                                ),
                            );
                            self.order_sender.send(sent)?
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(())
            }
        }
    }

    #[tracing::instrument(skip(self, intent))]
    fn schedule_position_intent(&self, intent: PositionIntent) -> Result<()> {
        self.scheduler_sender
            .send(intent)
            .context("Failed to send intent to scheduler")
    }

    #[tracing::instrument(skip(self))]
    fn get_positions(&self, ticker: &str) -> Vec<Position> {
        let mut allocations = self.allocations.clone();
        allocations.retain(|x| x.ticker == ticker);
        let mut by_owner: multimap::MultiMap<Owner, Allocation> = multimap::MultiMap::new();
        for allocation in allocations {
            by_owner.insert(allocation.owner.clone(), allocation)
        }
        by_owner
            .iter_all()
            .map(|(_, allocs)| Position::from_allocations(allocs))
            .collect()
    }

    #[tracing::instrument(skip(self, intent, positions))]
    fn make_orders(
        &self,
        intent: &PositionIntent,
        ticker: &str,
        positions: &[Position],
    ) -> (Option<Claim>, Option<OrderIntent>, Option<OrderIntent>) {
        let (strategy_shares, total_shares) = positions.iter().fold(
            (Decimal::ZERO, Decimal::ZERO),
            |(strat_shares, all_shares), pos| {
                if pos.owner
                    == Owner::Strategy(intent.strategy.clone(), intent.sub_strategy.clone())
                {
                    (strat_shares + pos.shares, all_shares + pos.shares)
                } else {
                    (strat_shares, all_shares + pos.shares)
                }
            },
        );
        let pending_shares: Decimal = self
            .pending_orders
            .values()
            .filter_map(|pending_order| {
                if pending_order.ticker == ticker {
                    Some(pending_order.pending_qty)
                } else {
                    None
                }
            })
            .sum::<isize>()
            .into();

        match intent.update_policy {
            UpdatePolicy::Retain => {
                debug!("No trading needed");
                return (None, None, None);
            }
            UpdatePolicy::RetainLong => {
                if strategy_shares.is_sign_positive() {
                    debug!("No trading needed");
                    return (None, None, None);
                }
            }
            UpdatePolicy::RetainShort => {
                if strategy_shares.is_sign_negative() {
                    debug!("No trading needed");
                    return (None, None, None);
                }
            }
            _ => (),
        };

        let diff_shares = match intent.amount {
            AmountSpec::Dollars(dollars) => {
                // TODO: fix the below so we can always have a price
                let price = intent
                    .decision_price
                    .or(intent.limit_price)
                    .or(intent.stop_price)
                    .expect("Need either limit price, stop price or decision price");
                dollars / price - strategy_shares
            }
            AmountSpec::Shares(shares) => shares - strategy_shares,
            AmountSpec::Zero => -strategy_shares,
            _ => unimplemented!(),
        };
        if diff_shares.is_zero() {
            debug!("No trading needed");
            (None, None, None)
        } else {
            let claim = Claim::new(
                intent.strategy.clone(),
                intent.sub_strategy.clone(),
                AmountSpec::Shares(diff_shares),
            );
            let signum_product = (total_shares + pending_shares).signum()
                * (diff_shares + total_shares + pending_shares).signum();
            if !signum_product.is_sign_negative() {
                let trade = make_order_intent(
                    &intent.id.to_string(),
                    &ticker,
                    diff_shares,
                    intent.limit_price,
                    intent.stop_price,
                );
                (Some(claim), Some(trade), None)
            } else {
                let sent = make_order_intent(
                    &intent.id.to_string(),
                    &ticker,
                    -(total_shares + pending_shares),
                    intent.limit_price,
                    intent.stop_price,
                );
                let saved = make_order_intent(
                    &intent.id.to_string(),
                    &ticker,
                    diff_shares + total_shares + pending_shares,
                    intent.limit_price,
                    intent.stop_price,
                );
                (Some(claim), Some(sent), Some(saved))
            }
        }
    }
}

#[tracing::instrument]
fn make_order_intent(
    prefix: &str,
    ticker: &str,
    qty: Decimal,
    limit_price: Option<Decimal>,
    stop_price: Option<Decimal>,
) -> OrderIntent {
    let side = if qty > Decimal::ZERO {
        Side::Buy
    } else {
        Side::Sell
    };
    let order_type = match (limit_price, stop_price) {
        (Some(limit_price), Some(stop_price)) => OrderType::StopLimit {
            limit_price,
            stop_price,
        },
        (Some(limit_price), None) => OrderType::Limit { limit_price },
        (None, Some(stop_price)) => OrderType::Stop { stop_price },
        (None, None) => OrderType::Market,
    };
    OrderIntent::new(ticker)
        .client_order_id(format!("{}_{}", prefix, Uuid::new_v4().to_string()))
        .qty(qty.abs().ceil().to_usize().unwrap())
        .order_type(order_type)
        .side(side)
}
