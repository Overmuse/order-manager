use super::OrderManager;
use crate::db;
use crate::event_sender::Event;
use crate::types::{calculate_claim_amount, Claim, Owner, Position, Trade};
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
            db::save_scheduled_intent(&*self.db_client.read().await, &intent)
                .await
                .context("Failed to save scheduled intent")?;
            self.schedule_position_intent(intent)
        } else {
            debug!("Evaluating intent");
            let maybe_trade_intent = self.evaluate_intent(intent).await?;
            if let Some(trade_intent) = maybe_trade_intent {
                self.event_sender.send(Event::RiskCheckRequest(trade_intent)).await?
            }
            Ok(())
        }
    }

    #[tracing::instrument(skip(self, intent))]
    pub fn schedule_position_intent(&self, intent: PositionIntent) -> Result<()> {
        self.scheduler_sender
            .send(intent)
            .context("Failed to send intent to scheduler")
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_intent(&self, intent: PositionIntent) -> Result<Option<TradeIntent>> {
        trace!("Evaluating intent");
        match &intent.identifier {
            Identifier::Ticker(ticker) => self.evaluate_single_ticker_intent(&intent, ticker).await,
            Identifier::All => self.evaluate_multi_ticker_intent(intent).await.map(|_| None),
        }
    }

    async fn get_strategy_shares(&self, ticker: &str, strategy: &str, sub_strategy: Option<&str>) -> Result<Decimal> {
        let maybe_position =
            db::get_position_by_owner_and_ticker(&*self.db_client.read().await, strategy, sub_strategy, ticker)
                .await
                .context("Failed to get positions")?;
        Ok(maybe_position.as_ref().map(|x| x.shares).unwrap_or(Decimal::ZERO))
    }

    #[tracing::instrument(skip(self, intent))]
    async fn evaluate_single_ticker_intent(
        &self,
        intent: &PositionIntent,
        ticker: &str,
    ) -> Result<Option<TradeIntent>> {
        let strategy_shares = self
            .get_strategy_shares(ticker, &intent.strategy, intent.sub_strategy.as_deref())
            .await?;
        if !should_position_be_updated(intent, strategy_shares) {
            return Ok(None);
        }
        if let Amount::Zero = intent.amount {
            // If there's no active trades for ticker, cancel any claim for this strategy and ticker
            let mut write_lock = self.db_client.write().await;
            let transaction = write_lock.transaction().await?;
            let active_amount = db::get_active_trade_amount_by_ticker(&transaction, ticker).await?;
            if active_amount.is_zero() {
                debug!("Cancelling claim that is no longer active");
                db::delete_claims_by_strategy_and_ticker(
                    &transaction,
                    &intent.strategy,
                    intent.sub_strategy.as_deref(),
                    ticker,
                )
                .await?;
            }
            transaction.commit().await?;
        }
        let maybe_price = get_last_price(&self.datastore_url, ticker)
            .await
            .ok()
            .or(intent.limit_price);
        let diff_amount = calculate_claim_amount(&intent.amount, strategy_shares, maybe_price);
        match diff_amount {
            Some(amount) if !amount.is_zero() => {
                let active_trades: Vec<_> = db::get_trades_by_ticker(&*self.db_client.read().await, ticker)
                    .await?
                    .into_iter()
                    .filter(Trade::is_active)
                    .collect();
                if !active_trades.is_empty() {
                    let maybe_trade = self
                        .generate_trades(ticker, &amount, intent.limit_price, intent.stop_price)
                        .await?;
                    if let Some(trade) = maybe_trade {
                        let id = active_trades.first().expect("Guaranteed to be non-empty").id;
                        db::save_dependent_trade(&*self.db_client.read().await, id, &trade).await?
                    }
                    return Ok(None);
                }
                let claim = Claim::new(
                    intent.strategy.clone(),
                    intent.sub_strategy.clone(),
                    ticker.to_string(),
                    amount,
                    intent.limit_price,
                    intent.before,
                );
                db::upsert_claim(&*self.db_client.read().await, &claim)
                    .await
                    .context("Failed to upsert claim")?;
                self.event_sender.send(Event::Claim(claim.clone())).await?;
                self.generate_trades(ticker, &claim.amount, intent.limit_price, intent.stop_price)
                    .await
            }
            _ => {
                trace!("No trade generated");
                Ok(None)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn generate_trades(
        &self,
        ticker: &str,
        amount: &Amount,
        limit_price: Option<Decimal>,
        stop_price: Option<Decimal>,
    ) -> Result<Option<TradeIntent>> {
        let positions = db::get_positions_by_ticker(&*self.db_client.read().await, ticker).await?;
        let diff_shares = match amount {
            Amount::Shares(shares) => *shares,
            Amount::Dollars(dollars) => {
                let price = get_last_price(&self.datastore_url, ticker)
                    .await
                    .map(Some)
                    .unwrap_or(limit_price);
                debug!(?price);
                match price {
                    Some(price) => (dollars / price).round_dp(8),
                    None => {
                        warn!("Missing price");
                        return Ok(None);
                    }
                }
            }
            _ => unreachable!(),
        };
        let active_shares: Decimal = db::get_active_trade_amount_by_ticker(&*self.db_client.read().await, ticker)
            .await
            .context("Failed to get active trade amount")?
            .into();
        let owned_shares: Decimal = positions.iter().map(|pos| pos.shares).sum();

        let (sent, maybe_saved) = make_trades(
            ticker,
            diff_shares,
            owned_shares + active_shares,
            limit_price,
            stop_price,
        )?;
        if let Some(saved) = maybe_saved {
            debug!("Saving dependent trade");
            db::save_dependent_trade(&*self.db_client.read().await, sent.id, &saved)
                .await
                .context("Failed to save dependent trade")?;
        }

        Ok(Some(sent))
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
                UpdatePolicy::RetainLong => db::get_positions_by_owner(&*self.db_client.read().await, &owner)
                    .await
                    .context("Failed to get positions")?
                    .into_iter()
                    .filter(|pos| pos.is_short())
                    .collect(),
                UpdatePolicy::RetainShort => db::get_positions_by_owner(&*self.db_client.read().await, &owner)
                    .await
                    .context("Failed to get position")?
                    .into_iter()
                    .filter(|pos| pos.is_long())
                    .collect(),
                UpdatePolicy::Update => db::get_positions_by_owner(&*self.db_client.read().await, &owner)
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
        let ticker = &position.ticker;
        let positions = db::get_positions_by_ticker(&*self.db_client.read().await, ticker).await?;
        let active_shares: Decimal = db::get_active_trade_amount_by_ticker(&*self.db_client.read().await, ticker)
            .await
            .context("Failed to get active trade amount")?
            .into();
        let owned_shares: Decimal = positions.iter().map(|pos| pos.shares).sum();
        let (sent, maybe_saved) = make_trades(
            &position.ticker,
            -position.shares,
            owned_shares + active_shares,
            None,
            None,
        )?;
        if let Some(saved) = maybe_saved {
            debug!("Saving dependent trade");
            db::save_dependent_trade(&*self.db_client.read().await, sent.id, &saved)
                .await
                .context("Failed to save dependent trade")?;
        }

        if let Owner::Strategy(strategy, sub_strategy) = position.owner {
            let claim = Claim::new(
                strategy,
                sub_strategy,
                position.ticker.clone(),
                Amount::Shares(-position.shares),
                None,
                None,
            );
            db::upsert_claim(&*self.db_client.read().await, &claim)
                .await
                .context("Failed to save claim")?;
            self.event_sender.send(Event::Claim(claim)).await?;
        };
        self.send_trade(sent).await
    }
}

#[tracing::instrument(skip(intent, strategy_shares))]
fn should_position_be_updated(intent: &PositionIntent, strategy_shares: Decimal) -> bool {
    match intent.update_policy {
        UpdatePolicy::Retain => {
            debug!("UpdatePolicy::Retain: No trading needed");
            false
        }
        UpdatePolicy::RetainLong => {
            if strategy_shares > Decimal::ZERO {
                debug!("UpdatePolicy::RetainLong and existing long position: No trading needed",);
                false
            } else {
                true
            }
        }
        UpdatePolicy::RetainShort => {
            if strategy_shares < Decimal::ZERO {
                debug!("UpdatePolicy::RetainShort and existing short position: No trading needed",);
                false
            } else {
                true
            }
        }
        _ => true,
    }
}

#[tracing::instrument(skip(ticker, diff_shares, total_shares, limit_price, stop_price))]
fn make_trades(
    ticker: &str,
    diff_shares: Decimal,
    total_shares: Decimal,
    limit_price: Option<Decimal>,
    stop_price: Option<Decimal>,
) -> Result<(TradeIntent, Option<TradeIntent>)> {
    let signum_product = total_shares.signum() * (diff_shares + total_shares).signum();
    if !signum_product.is_sign_negative() {
        let sent = make_trade_intent(ticker, diff_shares, limit_price, stop_price)?;
        Ok((sent, None))
    } else {
        let sent = make_trade_intent(ticker, -total_shares, limit_price, stop_price)?;
        let saved = make_trade_intent(ticker, diff_shares + total_shares, limit_price, stop_price)?;
        Ok((sent, Some(saved)))
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

async fn get_last_price(base_url: &str, ticker: &str) -> Result<Decimal> {
    let url = format!("{}/last/{}", base_url, ticker);
    let price: Decimal = reqwest::get(url).await?.json().await?;
    Ok(price)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_should_position_be_updated() {
        let retain = PositionIntent::builder("Strat", "AAPL", Amount::Shares(Decimal::ONE_HUNDRED))
            .update_policy(UpdatePolicy::Retain)
            .build()
            .unwrap();
        let retain_long = PositionIntent::builder("Strat", "AAPL", Amount::Shares(Decimal::ONE_HUNDRED))
            .update_policy(UpdatePolicy::RetainLong)
            .build()
            .unwrap();
        let retain_short = PositionIntent::builder("Strat", "AAPL", Amount::Shares(Decimal::ONE_HUNDRED))
            .update_policy(UpdatePolicy::RetainShort)
            .build()
            .unwrap();
        let update = PositionIntent::builder("Strat", "AAPL", Amount::Shares(Decimal::ONE_HUNDRED))
            .update_policy(UpdatePolicy::Update)
            .build()
            .unwrap();
        assert!(!should_position_be_updated(&retain, Decimal::ZERO));
        assert!(!should_position_be_updated(&retain, Decimal::ONE));
        assert!(!should_position_be_updated(&retain, -Decimal::ONE));
        assert!(should_position_be_updated(&retain_long, Decimal::ZERO));
        assert!(!should_position_be_updated(&retain_long, Decimal::ONE));
        assert!(should_position_be_updated(&retain_long, -Decimal::ONE));
        assert!(should_position_be_updated(&retain_short, Decimal::ZERO));
        assert!(should_position_be_updated(&retain_short, Decimal::ONE));
        assert!(!should_position_be_updated(&retain_short, -Decimal::ONE));
        assert!(should_position_be_updated(&update, Decimal::ZERO));
        assert!(should_position_be_updated(&update, Decimal::ONE));
        assert!(should_position_be_updated(&update, -Decimal::ONE));
    }

    #[test]
    fn test_make_trade_intent() {
        let market = make_trade_intent("AAPL", Decimal::ONE, None, None).unwrap();
        let limit = make_trade_intent("AAPL", Decimal::ONE, Some(Decimal::ONE), None).unwrap();
        let stop = make_trade_intent("AAPL", Decimal::ONE, None, Some(Decimal::ONE)).unwrap();
        let stop_limit = make_trade_intent("AAPL", Decimal::ONE, Some(Decimal::ONE), Some(Decimal::TWO)).unwrap();
        assert_eq!(market.order_type, OrderType::Market);
        assert_eq!(
            limit.order_type,
            OrderType::Limit {
                limit_price: Decimal::ONE
            }
        );
        assert_eq!(
            stop.order_type,
            OrderType::Stop {
                stop_price: Decimal::ONE
            }
        );
        assert_eq!(
            stop_limit.order_type,
            OrderType::StopLimit {
                stop_price: Decimal::TWO,
                limit_price: Decimal::ONE
            }
        );
    }

    #[test]
    fn test_trade_intent_rounding() {
        let small_round_up = make_trade_intent("AAPL", Decimal::new(999, 3), None, None).unwrap();
        let large_round_up = make_trade_intent("AAPL", Decimal::new(1, 3), None, None).unwrap();
        let small_round_down = make_trade_intent("AAPL", -Decimal::new(999, 3), None, None).unwrap();
        let large_round_down = make_trade_intent("AAPL", -Decimal::new(1, 3), None, None).unwrap();
        assert_eq!(small_round_up.qty, 1);
        assert_eq!(large_round_up.qty, 1);
        assert_eq!(small_round_down.qty, -1);
        assert_eq!(large_round_down.qty, -1);
    }

    #[test]
    fn test_make_trades() {
        // No change in sign leads to one trade
        let (sent, maybe_saved) = make_trades("AAPL", Decimal::TWO, Decimal::ONE, None, None).unwrap();
        assert_eq!(sent.qty, 2);
        assert_eq!(maybe_saved, None);

        // Change in sign leads to two trades
        let (sent, maybe_saved) = make_trades("AAPL", -Decimal::TWO, Decimal::ONE, None, None).unwrap();
        assert_eq!(sent.qty, -1);
        let saved = maybe_saved.unwrap();
        assert_eq!(saved.qty, -1);
    }
}
