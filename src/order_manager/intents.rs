use super::OrderManager;
use crate::db;
use crate::types::{Claim, Owner, Position};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use num_traits::Signed;
use rust_decimal::prelude::*;
use tracing::{debug, trace, warn};
use trading_base::{Amount, Identifier, OrderType, PositionIntent, TradeIntent, UpdatePolicy};

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
    pub async fn triage_intent(&self, intent: PositionIntent) -> Result<()> {
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
        db::save_scheduled_intent(self.db_client.as_ref(), &intent)
            .await
            .context("Failed to save scheduled intent")?;
        self.scheduler_sender
            .send(intent)
            .context("Failed to send intent to scheduler")
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_intent(&self, intent: PositionIntent) -> Result<()> {
        trace!("Evaluating intent");
        match &intent.identifier {
            Identifier::Ticker(ticker) => {
                self.evaluate_single_ticker_intent(&intent, &ticker).await
            }
            Identifier::All => self.evaluate_multi_ticker_intent(intent).await,
        }
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_single_ticker_intent(
        &self,
        intent: &PositionIntent,
        ticker: &str,
    ) -> Result<()> {
        let position = db::get_positions_by_owner_and_ticker(
            self.db_client.as_ref(),
            &Owner::Strategy(intent.strategy.clone(), intent.sub_strategy.clone()),
            &ticker,
        )
        .await
        .context("Failed to get positions")?;
        let maybe_claim = self.make_claim(intent, &ticker, position);
        if let Some(mut claim) = maybe_claim {
            self.net_claim(&mut claim).await?;
            db::save_claim(self.db_client.as_ref(), &claim)
                .await
                .context("Failed to save claim")?;
            self.generate_trades(intent, ticker, claim).await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn generate_trades(
        &self,
        intent: &PositionIntent,
        ticker: &str,
        claim: Claim,
    ) -> Result<()> {
        let positions = db::get_positions_by_ticker(self.db_client.as_ref(), ticker).await?;
        let diff_shares = match claim.amount {
            Amount::Shares(shares) => shares,
            _ => unreachable!(),
        };
        let pending_shares =
            db::get_pending_trade_amount_by_ticker(self.db_client.as_ref(), ticker)
                .await
                .context("Failed to get pending trade amount")?
                .unwrap_or(0)
                .into();
        let total_shares = positions.iter().map(|pos| pos.shares).sum();

        let (sent, maybe_saved) =
            self.make_trades(&intent, &ticker, diff_shares, total_shares, pending_shares)?;
        if let Some(saved) = maybe_saved {
            debug!("Saving dependent trade");
            db::save_dependent_trade(self.db_client.as_ref(), sent.id, &saved)
                .await
                .context("Failed to save dependent trade")?;
        }

        self.send_trade(sent).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_multi_ticker_intent(&self, intent: PositionIntent) -> Result<()> {
        trace!("Evaluating multi-ticker intent");
        if let Amount::Zero = intent.amount {
            let owner = Owner::Strategy(intent.strategy, intent.sub_strategy);
            let positions_to_close = match intent.update_policy {
                UpdatePolicy::Retain => {
                    debug!("UpdatePolicy::Retain: No trading needed");
                    return Ok(());
                }
                UpdatePolicy::RetainLong => {
                    db::get_positions_by_owner(self.db_client.as_ref(), &owner)
                        .await
                        .context("Failed to get positions")?
                        .into_iter()
                        .filter(|pos| pos.is_short())
                        .collect()
                }
                UpdatePolicy::RetainShort => {
                    db::get_positions_by_owner(self.db_client.as_ref(), &owner)
                        .await
                        .context("Failed to get position")?
                        .into_iter()
                        .filter(|pos| pos.is_long())
                        .collect()
                }
                UpdatePolicy::Update => db::get_positions_by_owner(self.db_client.as_ref(), &owner)
                    .await
                    .context("Failed to get positions")?,
            };
            for position in positions_to_close {
                self.close_position(position)
                    .await
                    .context("Failed to close position")?
            }
            Ok(())
        } else {
            Err(anyhow!("amount must be Zero if ticker is All"))
        }
    }

    #[tracing::instrument(skip(self, position), fields(position.ticker))]
    async fn close_position(&self, position: Position) -> Result<()> {
        if let Owner::Strategy(strategy, sub_strategy) = position.owner {
            let trade_intent = make_trade_intent(&position.ticker, -position.shares, None, None);
            let claim = Claim::new(
                strategy,
                sub_strategy,
                position.ticker.clone(),
                Amount::Shares(-position.shares),
            );
            db::save_claim(self.db_client.as_ref(), &claim)
                .await
                .context("Failed to save claim")?;
            self.send_trade(trade_intent).await
        } else {
            Err(anyhow!("Can't close position of house account"))
        }
    }

    #[tracing::instrument(skip(self, intent, ticker, position))]
    fn make_claim(
        &self,
        intent: &PositionIntent,
        ticker: &str,
        position: Option<Position>,
    ) -> Option<Claim> {
        let strategy_shares = position.as_ref().map(|x| x.shares).unwrap_or(Decimal::ZERO);
        match intent.update_policy {
            UpdatePolicy::Retain => {
                debug!("UpdatePolicy::Retain: No trading needed");
                return None;
            }
            UpdatePolicy::RetainLong => {
                if strategy_shares > Decimal::ZERO {
                    debug!(
                        "UpdatePolicy::RetainLong and existing long position: No trading needed",
                    );
                    return None;
                }
            }
            UpdatePolicy::RetainShort => {
                if strategy_shares < Decimal::ZERO {
                    debug!(
                        "UpdatePolicy::RetainShort and existing short position: No trading needed",
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
    fn make_trades(
        &self,
        intent: &PositionIntent,
        ticker: &str,
        diff_shares: Decimal,
        total_shares: Decimal,
        pending_shares: Decimal,
    ) -> Result<(TradeIntent, Option<TradeIntent>)> {
        let signum_product = (total_shares + pending_shares).signum()
            * (diff_shares + total_shares + pending_shares).signum();
        if !signum_product.is_sign_negative() {
            let sent =
                make_trade_intent(ticker, diff_shares, intent.limit_price, intent.stop_price);
            Ok((sent, None))
        } else {
            let sent = make_trade_intent(
                ticker,
                -(total_shares + pending_shares),
                intent.limit_price,
                intent.stop_price,
            );
            let saved = make_trade_intent(
                ticker,
                diff_shares + total_shares + pending_shares,
                intent.limit_price,
                intent.stop_price,
            );
            Ok((sent, Some(saved)))
        }
    }

    async fn net_claim(&self, _claim: &mut Claim) -> Result<()> {
        Ok(())
    }
}

#[tracing::instrument(skip(ticker, qty, limit_price, stop_price))]
fn make_trade_intent(
    ticker: &str,
    qty: Decimal,
    limit_price: Option<Decimal>,
    stop_price: Option<Decimal>,
) -> TradeIntent {
    let order_type = match (limit_price, stop_price) {
        (Some(limit_price), Some(stop_price)) => OrderType::StopLimit {
            limit_price,
            stop_price,
        },
        (Some(limit_price), None) => OrderType::Limit { limit_price },
        (None, Some(stop_price)) => OrderType::Stop { stop_price },
        (None, None) => OrderType::Market,
    };

    TradeIntent::new(
        ticker,
        qty.round_dp_with_strategy(0, RoundingStrategy::AwayFromZero)
            .to_isize()
            .unwrap(),
    )
    .order_type(order_type)
}
