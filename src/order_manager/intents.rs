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
            Identifier::Ticker(ticker) => self.evaluate_single_ticker_intent(&intent, ticker).await,
            Identifier::All => self.evaluate_multi_ticker_intent(intent).await,
        }
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_single_ticker_intent(&self, intent: &PositionIntent, ticker: &str) -> Result<()> {
        let maybe_position = db::get_position_by_owner_and_ticker(
            self.db_client.as_ref(),
            &Owner::Strategy(intent.strategy.clone(), intent.sub_strategy.clone()),
            ticker,
        )
        .await
        .context("Failed to get positions")?;
        let strategy_shares = maybe_position.as_ref().map(|x| x.shares).unwrap_or(Decimal::ZERO);
        let maybe_claim = self.make_claim(intent, ticker, strategy_shares).await?;
        if let Some(mut claim) = maybe_claim {
            self.net_claim(&mut claim).await?;
            db::save_claim(self.db_client.as_ref(), &claim)
                .await
                .context("Failed to save claim")?;
            self.generate_trades(ticker, &claim.amount, intent.limit_price, intent.stop_price)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn generate_trades(
        &self,
        ticker: &str,
        amount: &Amount,
        limit_price: Option<Decimal>,
        stop_price: Option<Decimal>,
    ) -> Result<()> {
        let positions = db::get_positions_by_ticker(self.db_client.as_ref(), ticker).await?;
        let diff_shares = match amount {
            Amount::Shares(shares) => *shares,
            Amount::Dollars(dollars) => {
                let price = self
                    .redis
                    .get::<Option<f64>>(&format!("price/{}", ticker))
                    .await?
                    .map(Decimal::from_f64)
                    .flatten()
                    .or(limit_price);
                match price {
                    Some(price) => dollars / price,
                    None => {
                        warn!("Missing price");
                        return Ok(());
                    }
                }
            }
            _ => unreachable!(),
        };
        let pending_shares = db::get_pending_trade_amount_by_ticker(self.db_client.as_ref(), ticker)
            .await
            .context("Failed to get pending trade amount")?
            .unwrap_or(0)
            .into();
        let total_shares = positions.iter().map(|pos| pos.shares).sum();

        let (sent, maybe_saved) = self.make_trades(
            ticker,
            diff_shares,
            total_shares,
            pending_shares,
            limit_price,
            stop_price,
        )?;
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
                UpdatePolicy::RetainLong => db::get_positions_by_owner(self.db_client.as_ref(), &owner)
                    .await
                    .context("Failed to get positions")?
                    .into_iter()
                    .filter(|pos| pos.is_short())
                    .collect(),
                UpdatePolicy::RetainShort => db::get_positions_by_owner(self.db_client.as_ref(), &owner)
                    .await
                    .context("Failed to get position")?
                    .into_iter()
                    .filter(|pos| pos.is_long())
                    .collect(),
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
    pub async fn close_position(&self, position: Position) -> Result<()> {
        let trade_intent = make_trade_intent(&position.ticker, -position.shares, None, None)?;
        if let Owner::Strategy(strategy, sub_strategy) = position.owner {
            let claim = Claim::new(
                strategy,
                sub_strategy,
                position.ticker.clone(),
                Amount::Shares(-position.shares),
                None,
            );
            db::save_claim(self.db_client.as_ref(), &claim)
                .await
                .context("Failed to save claim")?;
        };
        self.send_trade(trade_intent).await
    }

    #[tracing::instrument(skip(self, intent, ticker, strategy_shares))]
    async fn make_claim(
        &self,
        intent: &PositionIntent,
        ticker: &str,
        strategy_shares: Decimal,
    ) -> Result<Option<Claim>> {
        match intent.update_policy {
            UpdatePolicy::Retain => {
                debug!("UpdatePolicy::Retain: No trading needed");
                return Ok(None);
            }
            UpdatePolicy::RetainLong => {
                if strategy_shares > Decimal::ZERO {
                    debug!("UpdatePolicy::RetainLong and existing long position: No trading needed",);
                    return Ok(None);
                }
            }
            UpdatePolicy::RetainShort => {
                if strategy_shares < Decimal::ZERO {
                    debug!("UpdatePolicy::RetainShort and existing short position: No trading needed",);
                    return Ok(None);
                }
            }
            _ => (),
        };
        let diff_amount = match intent.amount {
            Amount::Dollars(dollars) => {
                let price: Option<f64> = self.redis.get(&format!("price/{}", ticker)).await?;
                let price = price.map(Decimal::from_f64).flatten().or(intent.limit_price);
                match price {
                    Some(price) => Amount::Dollars(dollars - price * strategy_shares),
                    None => {
                        warn!("Missing price");
                        return Ok(None);
                    }
                }
            }
            Amount::Shares(shares) => Amount::Shares(shares - strategy_shares),
            Amount::Zero => Amount::Shares(-strategy_shares),
        };
        if diff_amount.is_zero() {
            debug!("No change in shares: No trading needed");
            return Ok(None);
        }
        let claim = Claim::new(
            intent.strategy.clone(),
            intent.sub_strategy.clone(),
            ticker.to_string(),
            diff_amount,
            intent.limit_price,
        );
        Ok(Some(claim))
    }

    #[tracing::instrument(skip(self, ticker, diff_shares, total_shares, pending_shares, limit_price, stop_price))]
    fn make_trades(
        &self,
        ticker: &str,
        diff_shares: Decimal,
        total_shares: Decimal,
        pending_shares: Decimal,
        limit_price: Option<Decimal>,
        stop_price: Option<Decimal>,
    ) -> Result<(TradeIntent, Option<TradeIntent>)> {
        let signum_product =
            (total_shares + pending_shares).signum() * (diff_shares + total_shares + pending_shares).signum();
        if !signum_product.is_sign_negative() {
            let sent = make_trade_intent(ticker, diff_shares, limit_price, stop_price)?;
            Ok((sent, None))
        } else {
            let sent = make_trade_intent(ticker, -(total_shares + pending_shares), limit_price, stop_price)?;
            let saved = make_trade_intent(
                ticker,
                diff_shares + total_shares + pending_shares,
                limit_price,
                stop_price,
            )?;
            Ok((sent, Some(saved)))
        }
    }

    async fn net_claim(&self, claim: &mut Claim) -> Result<()> {
        let _maybe_house_position =
            db::get_position_by_owner_and_ticker(self.db_client.as_ref(), &Owner::House, &claim.ticker).await?;
        // TODO: Actually net the claim

        Ok(())
    }
}

#[tracing::instrument(skip(ticker, qty, limit_price, stop_price))]
fn make_trade_intent(
    ticker: &str,
    qty: Decimal,
    limit_price: Option<Decimal>,
    stop_price: Option<Decimal>,
) -> Result<TradeIntent> {
    let order_type = match (limit_price, stop_price) {
        (Some(limit_price), Some(stop_price)) => OrderType::StopLimit {
            limit_price,
            stop_price,
        },
        (Some(limit_price), None) => OrderType::Limit { limit_price },
        (None, Some(stop_price)) => OrderType::Stop { stop_price },
        (None, None) => OrderType::Market,
    };

    let intent = TradeIntent::new(
        ticker,
        qty.round_dp_with_strategy(0, RoundingStrategy::AwayFromZero)
            .to_isize()
            .ok_or_else(|| anyhow!("Failed to convert decimal"))?,
    )
    .order_type(order_type);
    Ok(intent)
}
