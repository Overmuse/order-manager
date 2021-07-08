use super::OrderManager;
use crate::db;
use crate::types::{Claim, Owner, PendingOrder, Position};
use alpaca::{orders::OrderIntent, OrderType, Side};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use num_traits::Signed;
use position_intents::{AmountSpec, PositionIntent, TickerSpec, UpdatePolicy};
use rust_decimal::prelude::*;
use tracing::{debug, trace};
use uuid::Uuid;

trait PositionIntentExt {
    fn is_expired(&self) -> bool;
    fn is_active(&self) -> bool;
}

impl PositionIntentExt for PositionIntent {
    fn is_expired(&self) -> bool {
        if let Some(dt) = self.before {
            dt <= Utc::now()
        } else {
            false
        }
    }

    fn is_active(&self) -> bool {
        if let Some(dt) = self.after {
            dt >= Utc::now()
        } else {
            true
        }
    }
}

impl OrderManager {
    #[tracing::instrument(skip(self, intent), fields(id = %intent.id))]
    pub async fn triage_intent(&mut self, intent: PositionIntent) -> Result<()> {
        debug!("Handling position intent");
        if intent.is_expired() {
            // Intent has already expired, so don't do anything
            debug!("Expired intent");
            Ok(())
        } else if !intent.is_active() {
            // Not ready to transmit intent yet
            debug!("Sending intent to scheduler");
            self.schedule_position_intent(intent)
        } else {
            debug!("Evaluating intent");
            self.evaluate_intent(intent).await
        }
    }

    #[tracing::instrument(skip(self, intent))]
    fn schedule_position_intent(&self, intent: PositionIntent) -> Result<()> {
        self.scheduler_sender
            .send(intent)
            .context("Failed to send intent to scheduler")
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_intent(&mut self, intent: PositionIntent) -> Result<()> {
        trace!("Evaluating intent");
        match &intent.ticker.clone() {
            TickerSpec::Ticker(ticker) => self.evaluate_single_ticker_intent(intent, &ticker).await,
            TickerSpec::All => self.evaluate_multi_ticker_intent(intent).await,
        }
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_single_ticker_intent(
        &self,
        intent: PositionIntent,
        ticker: &str,
    ) -> Result<()> {
        let pending_shares = db::get_pending_order_amount_by_ticker(self.db_client.clone(), ticker)
            .await
            .context("Failed to get pending order amount")?
            .unwrap_or(0)
            .into();
        let positions = db::get_positions_by_ticker(self.db_client.clone(), &ticker)
            .await
            .context("Failed to get positions")?;
        let strategy_shares = positions
            .iter()
            .filter_map(|pos| {
                if let Owner::Strategy(strategy, sub_strategy) = &pos.owner {
                    if strategy == &intent.strategy && sub_strategy == &intent.sub_strategy {
                        return Some(pos.shares);
                    }
                }
                None
            })
            .sum();
        let total_shares = positions.iter().map(|pos| pos.shares).sum();
        match self.make_orders(
            &intent,
            &ticker,
            strategy_shares,
            total_shares,
            pending_shares,
        )? {
            (None, None, None) => {
                debug!("No trades generated");
                Ok(())
            }
            (Some(claim), Some(sent), None) => {
                db::save_claim(self.db_client.clone(), claim)
                    .await
                    .context("Failed to save claim")?;
                let qty = match sent.side {
                    Side::Buy => sent.qty.to_isize().unwrap(),
                    Side::Sell => -(sent.qty.to_isize().unwrap()),
                };
                db::save_pending_order(
                    self.db_client.clone(),
                    PendingOrder::new(
                        sent.client_order_id.clone().unwrap(),
                        ticker.to_string(),
                        qty as i32,
                    ),
                )
                .await
                .context("Failed to save pending order")?;
                Ok(self
                    .order_sender
                    .send(sent)
                    .context("Failed to send order")?)
            }
            (Some(claim), Some(sent), Some(saved)) => {
                debug!("Saving dependent order");
                db::save_dependent_order(
                    self.db_client.clone(),
                    &sent.client_order_id.clone().unwrap(),
                    saved,
                )
                .await
                .context("Failed to save dependent order")?;
                db::save_claim(self.db_client.clone(), claim)
                    .await
                    .context("Failed to save claim")?;
                let qty = match sent.side {
                    Side::Buy => sent.qty.to_isize().unwrap(),
                    Side::Sell => -(sent.qty.to_isize().unwrap()),
                };
                db::save_pending_order(
                    self.db_client.clone(),
                    PendingOrder::new(
                        sent.client_order_id.clone().unwrap(),
                        ticker.to_string(),
                        qty as i32,
                    ),
                )
                .await
                .context("Failed to save pending order")?;
                Ok(self
                    .order_sender
                    .send(sent)
                    .context("Failed to send order")?)
            }
            _ => unreachable!(),
        }
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_multi_ticker_intent(&self, intent: PositionIntent) -> Result<()> {
        trace!("Evaluating multi-ticker intent");
        if let AmountSpec::Zero = intent.amount {
            let owner = Owner::Strategy(intent.strategy, intent.sub_strategy);
            let positions = match intent.update_policy {
                UpdatePolicy::Retain => {
                    debug!("UpdatePolicy::Retain: No trading needed");
                    return Ok(());
                }
                UpdatePolicy::RetainLong => {
                    db::get_positions_by_owner(self.db_client.clone(), owner)
                        .await
                        .context("Failed to get positions")?
                        .into_iter()
                        .filter(|pos| pos.is_short())
                        .collect()
                }
                UpdatePolicy::RetainShort => {
                    db::get_positions_by_owner(self.db_client.clone(), owner)
                        .await
                        .context("Failed to get position")?
                        .into_iter()
                        .filter(|pos| pos.is_long())
                        .collect()
                }
                UpdatePolicy::Update => db::get_positions_by_owner(self.db_client.clone(), owner)
                    .await
                    .context("Failed to get positions")?,
            };
            let id = intent.id.to_string();
            for position in positions {
                self.close_position(&id, position)
                    .await
                    .context("Failed to close position")?
            }
            Ok(())
        } else {
            Err(anyhow!("amount must be Zero if ticker is All"))
        }
    }

    #[tracing::instrument(skip(self, id, position), fields(position.ticker))]
    async fn close_position(&self, id: &str, position: Position) -> Result<()> {
        if let Owner::Strategy(strategy, sub_strategy) = position.owner {
            let order_intent =
                make_order_intent(id, &position.ticker, -position.shares, None, None);
            let claim = Claim::new(
                strategy,
                sub_strategy,
                position.ticker.clone(),
                AmountSpec::Shares(-position.shares),
            );
            db::save_claim(self.db_client.clone(), claim)
                .await
                .context("Failed to save claim")?;
            db::save_pending_order(
                self.db_client.clone(),
                PendingOrder::new(
                    order_intent.client_order_id.clone().unwrap(),
                    position.ticker.to_string(),
                    (-position.shares).to_i32().unwrap(),
                ),
            )
            .await
            .context("Failed to save pending order")?;
            Ok(self
                .order_sender
                .send(order_intent)
                .context("Failed to send order")?)
        } else {
            Err(anyhow!("Can't close position of house account"))
        }
    }

    #[tracing::instrument(skip(
        self,
        intent,
        ticker,
        strategy_shares,
        total_shares,
        pending_shares
    ))]
    fn make_orders(
        &self,
        intent: &PositionIntent,
        ticker: &str,
        strategy_shares: Decimal,
        total_shares: Decimal,
        pending_shares: Decimal,
    ) -> Result<(Option<Claim>, Option<OrderIntent>, Option<OrderIntent>)> {
        match intent.update_policy {
            UpdatePolicy::Retain => {
                debug!("UpdatePolicy::Retain: No trading needed");
                return Ok((None, None, None));
            }
            UpdatePolicy::RetainLong => {
                if strategy_shares > Decimal::ZERO {
                    debug!(
                        "UpdatePolicy::RetainLong and position {}: No trading needed",
                        strategy_shares
                    );
                    return Ok((None, None, None));
                }
            }
            UpdatePolicy::RetainShort => {
                if strategy_shares < Decimal::ZERO {
                    debug!(
                        "UpdatePolicy::RetainShort and position {}: No trading needed",
                        strategy_shares
                    );
                    return Ok((None, None, None));
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
                if price.is_zero() {
                    return Err(anyhow!("Price of intent cannot be zero"));
                }
                dollars / price - strategy_shares
            }
            AmountSpec::Shares(shares) => shares - strategy_shares,
            AmountSpec::Zero => -strategy_shares,
            _ => unimplemented!(),
        };
        if diff_shares.is_zero() {
            debug!("No change in shares: No trading needed");
            return Ok((None, None, None));
        }
        let claim = Claim::new(
            intent.strategy.clone(),
            intent.sub_strategy.clone(),
            ticker.to_string(),
            AmountSpec::Shares(diff_shares),
        );
        let signum_product = (total_shares + pending_shares).signum()
            * (diff_shares + total_shares + pending_shares).signum();
        if !signum_product.is_sign_negative() {
            let trade = make_order_intent(
                &intent.id.to_string(),
                ticker,
                diff_shares,
                intent.limit_price,
                intent.stop_price,
            );
            Ok((Some(claim), Some(trade), None))
        } else {
            let sent = make_order_intent(
                &intent.id.to_string(),
                ticker,
                -(total_shares + pending_shares),
                intent.limit_price,
                intent.stop_price,
            );
            let saved = make_order_intent(
                &intent.id.to_string(),
                ticker,
                diff_shares + total_shares + pending_shares,
                intent.limit_price,
                intent.stop_price,
            );
            Ok((Some(claim), Some(sent), Some(saved)))
        }
    }
}

#[tracing::instrument(skip(prefix, ticker, qty, limit_price, stop_price))]
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
