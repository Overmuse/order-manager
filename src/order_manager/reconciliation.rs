use crate::db;
use crate::types::{Owner, Status};
use crate::Event;
use crate::OrderManager;
use anyhow::Result;
use chrono::{Duration, Utc};
use rust_decimal::prelude::*;
use tracing::{debug, warn};
use trading_base::Amount;

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub async fn reconcile(&self) -> Result<()> {
        self.cancel_old_pending_trades().await?;
        self.reconcile_claims().await?;
        self.reconcile_house_positions().await
    }

    #[tracing::instrument(skip(self))]
    async fn cancel_old_pending_trades(&self) -> Result<()> {
        let pending_trades = db::get_pending_trades(self.db_client.as_ref()).await?;
        for trade in pending_trades {
            if (Utc::now() - trade.datetime) > Duration::seconds(self.settings.unreported_trade_expiry_seconds as i64)
                && trade.status == Status::Unreported
            {
                warn!(id = %trade.id, "Deleting unreported trade");
                db::delete_pending_trade_by_id(self.db_client.as_ref(), trade.id).await?;
                if let Some(claim_id) = trade.claim_id {
                    db::delete_claim_by_id(self.db_client.as_ref(), claim_id).await?;
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn reconcile_claims(&self) -> Result<()> {
        let claims = db::get_claims(self.db_client.as_ref()).await?;
        let claims = claims.iter().filter(|claim| !claim.amount.is_zero());
        for claim in claims {
            let pending_trade_amount =
                db::get_pending_trade_amount_by_ticker(self.db_client.as_ref(), &claim.ticker).await?;

            if pending_trade_amount.is_zero() {
                debug!("Unfilled claim, sending new trade");
                let maybe_trade = self
                    .generate_trades(&claim.ticker, &claim.amount, claim.limit_price, None, Some(claim.id))
                    .await?;
                if let Some(trade_intent) = maybe_trade {
                    self.event_sender.send(Event::RiskCheckRequest(trade_intent)).await?
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn reconcile_house_positions(&self) -> Result<()> {
        let house_positions = db::get_positions_by_owner(self.db_client.as_ref(), &Owner::House).await?;
        let house_positions = house_positions.iter().filter(|pos| pos.shares != Decimal::ZERO);
        for position in house_positions {
            if position.shares.abs() > Decimal::from_f64(0.99).unwrap() {
                debug!(ticker = %position.ticker, shares = %position.shares, "Reducing size of house position");
                let mut shares_to_liquidate = (position.shares.abs() - Decimal::from_f64(0.99).unwrap())
                    .round_dp_with_strategy(0, RoundingStrategy::AwayFromZero);
                shares_to_liquidate.set_sign_positive(position.shares.is_sign_positive());
                let maybe_trade = self
                    .generate_trades(
                        &position.ticker,
                        &Amount::Shares(-shares_to_liquidate),
                        None,
                        None,
                        None,
                    )
                    .await?;
                if let Some(intent) = maybe_trade {
                    self.event_sender.send(Event::RiskCheckRequest(intent)).await?
                }
            }
        }
        Ok(())
    }
}
