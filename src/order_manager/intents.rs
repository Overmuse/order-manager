use super::OrderManager;
use crate::db;
use crate::types::{Claim, Owner, Position};
use alpaca::{orders::OrderIntent, OrderType, Side};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use num_traits::Signed;
use rust_decimal::prelude::*;
use tracing::{debug, trace, warn};
use trading_base::{Amount, Identifier, PositionIntent, UpdatePolicy};
use uuid::Uuid;

pub(crate) trait PositionIntentExt {
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
            dt <= Utc::now()
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
            self.schedule_position_intent(intent).await
        } else {
            debug!("Evaluating intent");
            self.evaluate_intent(intent).await
        }
    }

    #[tracing::instrument(skip(self, intent))]
    pub async fn schedule_position_intent(&self, intent: PositionIntent) -> Result<()> {
        db::save_scheduled_intent(self.db_client.as_ref(), intent.clone())
            .await
            .context("Failed to save scheduled intent")?;
        self.scheduler_sender
            .send(intent)
            .context("Failed to send intent to scheduler")
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_intent(&mut self, intent: PositionIntent) -> Result<()> {
        trace!("Evaluating intent");
        match &intent.identifier.clone() {
            Identifier::Ticker(ticker) => self.evaluate_single_ticker_intent(intent, &ticker).await,
            Identifier::All => self.evaluate_multi_ticker_intent(intent).await,
        }
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_single_ticker_intent(
        &self,
        intent: PositionIntent,
        ticker: &str,
    ) -> Result<()> {
        let pending_shares =
            db::get_pending_order_amount_by_ticker(self.db_client.as_ref(), ticker)
                .await
                .context("Failed to get pending order amount")?
                .unwrap_or(0)
                .into();
        let positions = db::get_positions_by_ticker(self.db_client.as_ref(), &ticker)
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
        let maybe_claim = self.make_claim(&intent, &ticker, strategy_shares);
        if let Some(mut claim) = maybe_claim {
            self.net_claim(&mut claim);
            let diff_shares = match claim.amount {
                Amount::Shares(shares) => shares,
                _ => unreachable!(),
            };
            db::save_claim(self.db_client.as_ref(), claim)
                .await
                .context("Failed to save claim")?;

            let (sent, maybe_saved) =
                self.make_orders(&intent, &ticker, diff_shares, total_shares, pending_shares)?;
            if let Some(saved) = maybe_saved {
                debug!("Saving dependent order");
                db::save_dependent_order(
                    self.db_client.as_ref(),
                    &sent.client_order_id.clone().unwrap(),
                    saved,
                )
                .await
                .context("Failed to save dependent order")?;
            }

            self.send_order(sent).await?
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_multi_ticker_intent(&self, intent: PositionIntent) -> Result<()> {
        trace!("Evaluating multi-ticker intent");
        if let Amount::Zero = intent.amount {
            let owner = Owner::Strategy(intent.strategy, intent.sub_strategy);
            let positions = match intent.update_policy {
                UpdatePolicy::Retain => {
                    debug!("UpdatePolicy::Retain: No trading needed");
                    return Ok(());
                }
                UpdatePolicy::RetainLong => {
                    db::get_positions_by_owner(self.db_client.as_ref(), owner)
                        .await
                        .context("Failed to get positions")?
                        .into_iter()
                        .filter(|pos| pos.is_short())
                        .collect()
                }
                UpdatePolicy::RetainShort => {
                    db::get_positions_by_owner(self.db_client.as_ref(), owner)
                        .await
                        .context("Failed to get position")?
                        .into_iter()
                        .filter(|pos| pos.is_long())
                        .collect()
                }
                UpdatePolicy::Update => db::get_positions_by_owner(self.db_client.as_ref(), owner)
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
                Amount::Shares(-position.shares),
            );
            db::save_claim(self.db_client.as_ref(), claim)
                .await
                .context("Failed to save claim")?;
            self.send_order(order_intent).await
        } else {
            Err(anyhow!("Can't close position of house account"))
        }
    }

    #[tracing::instrument(skip(self, intent, ticker, strategy_shares,))]
    fn make_claim(
        &self,
        intent: &PositionIntent,
        ticker: &str,
        strategy_shares: Decimal,
    ) -> Option<Claim> {
        match intent.update_policy {
            UpdatePolicy::Retain => {
                debug!("UpdatePolicy::Retain: No trading needed");
                return None;
            }
            UpdatePolicy::RetainLong => {
                if strategy_shares > Decimal::ZERO {
                    debug!(
                        "UpdatePolicy::RetainLong and position {}: No trading needed",
                        strategy_shares
                    );
                    return None;
                }
            }
            UpdatePolicy::RetainShort => {
                if strategy_shares < Decimal::ZERO {
                    debug!(
                        "UpdatePolicy::RetainShort and position {}: No trading needed",
                        strategy_shares
                    );
                    return None;
                }
            }
            _ => (),
        };
        let diff_shares = match intent.amount {
            Amount::Dollars(dollars) => {
                // TODO: fix the below so we can always have a price
                let price = intent
                    .decision_price
                    .or(intent.limit_price)
                    .or(intent.stop_price)
                    .expect("Need either limit price, stop price or decision price");
                if price.is_zero() {
                    warn!("Price of intent cannot be zero");
                    return None;
                }
                dollars / price - strategy_shares
            }
            Amount::Shares(shares) => shares - strategy_shares,
            Amount::Zero => -strategy_shares,
        };
        if diff_shares.is_zero() {
            debug!("No change in shares: No trading needed");
            return None;
        }
        let claim = Claim::new(
            intent.strategy.clone(),
            intent.sub_strategy.clone(),
            ticker.to_string(),
            Amount::Shares(diff_shares),
        );
        Some(claim)
    }

    #[tracing::instrument(skip(self, intent, ticker, diff_shares, total_shares, pending_shares))]
    fn make_orders(
        &self,
        intent: &PositionIntent,
        ticker: &str,
        diff_shares: Decimal,
        total_shares: Decimal,
        pending_shares: Decimal,
    ) -> Result<(OrderIntent, Option<OrderIntent>)> {
        let signum_product = (total_shares + pending_shares).signum()
            * (diff_shares + total_shares + pending_shares).signum();
        if !signum_product.is_sign_negative() {
            let sent = make_order_intent(
                &intent.id.to_string(),
                ticker,
                diff_shares,
                intent.limit_price,
                intent.stop_price,
            );
            Ok((sent, None))
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
            Ok((sent, Some(saved)))
        }
    }

    fn net_claim(&self, _claim: &mut Claim) {}
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
