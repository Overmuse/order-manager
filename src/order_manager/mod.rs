use crate::db;
use crate::order_manager::input::State;
use crate::redis::Redis;
use crate::settings::AppSettings;
use crate::types::{Owner, PendingTrade, Status};
use crate::TradeSenderHandle;
use alpaca::AlpacaMessage;
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use rdkafka::consumer::StreamConsumer;
use rust_decimal::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_postgres::Client;
use tracing::{debug, error, info, warn};
use trading_base::{Amount, PositionIntent, TradeIntent};

mod dependent_trades;
mod input;
mod intents;
mod order_updates;
use input::Input;
use intents::PositionIntentExt;

pub struct OrderManager {
    kafka_consumer: StreamConsumer,
    scheduler_sender: UnboundedSender<PositionIntent>,
    scheduler_receiver: UnboundedReceiver<PositionIntent>,
    trade_sender: TradeSenderHandle,
    db_client: Arc<Client>,
    redis: Redis,
    settings: AppSettings,
}

impl OrderManager {
    pub fn new(
        kafka_consumer: StreamConsumer,
        scheduler_sender: UnboundedSender<PositionIntent>,
        scheduler_receiver: UnboundedReceiver<PositionIntent>,
        trade_sender: TradeSenderHandle,
        db_client: Arc<Client>,
        redis: Redis,
        settings: AppSettings,
    ) -> Self {
        Self {
            kafka_consumer,
            scheduler_sender,
            scheduler_receiver,
            trade_sender,
            db_client,
            redis,
            settings,
        }
    }

    pub async fn run(mut self) {
        info!("Starting OrderManager");
        if let Err(e) = self
            .initalize()
            .await
            .context("Failed to initialize order manager")
        {
            error!("{:?}", e)
        };

        loop {
            let message = self.receive_message().await;
            if let Err(e) = self.handle_input(message).await {
                error!("{:?}", e)
            }
        }
    }

    async fn initalize(&self) -> Result<()> {
        debug!("Populating scheduled intents");
        let scheduled_intents = db::get_scheduled_indents(self.db_client.as_ref())
            .await
            .context("Failed to get scheduled intents")?;
        for intent in scheduled_intents {
            if !intent.is_active() {
                self.schedule_position_intent(intent)
                    .await
                    .context("Failed to schedule position intent")?
            }
        }
        Ok(())
    }

    async fn handle_input(&self, input: Result<Input>) -> Result<()> {
        match input {
            Ok(Input::PositionIntent(intent)) => self
                .triage_intent(intent)
                .await
                .context("Failed to triage PositionIntent")?,
            Ok(Input::AlpacaMessage(AlpacaMessage::TradeUpdates(oe))) => self
                .handle_order_update(oe)
                .await
                .context("Failed to handle OrderEvent")?,
            Ok(Input::AlpacaMessage(_)) => unreachable!(),
            Ok(Input::Time(State::Open { .. })) => {
                debug!("Handling time update");
                self.reconcile().await.context("Failed to reconcile")?;
            }
            Ok(Input::Time(State::Closed { .. })) => {}
            Err(e) => return Err(e),
        };
        Ok(())
    }

    async fn reconcile(&self) -> Result<()> {
        self.cancel_old_pending_trades().await?;
        self.reconcile_claims().await?;
        self.reconcile_house_positions().await
    }

    async fn cancel_old_pending_trades(&self) -> Result<()> {
        let pending_trades = db::get_pending_trades(self.db_client.as_ref()).await?;
        for trade in pending_trades {
            if (Utc::now() - trade.datetime)
                > Duration::seconds(self.settings.unreported_trade_expiry_seconds as i64)
                && trade.status == Status::Unreported
            {
                warn!(id = %trade.id, "Deleting unreported trade");
                db::delete_pending_trade_by_id(self.db_client.as_ref(), trade.id).await?
            }
        }
        Ok(())
    }

    async fn reconcile_claims(&self) -> Result<()> {
        let claims = db::get_claims(self.db_client.as_ref()).await?;
        let claims = claims.iter().filter(|claim| !claim.amount.is_zero());
        for claim in claims {
            let pending_trade_amount =
                db::get_pending_trade_amount_by_ticker(self.db_client.as_ref(), &claim.ticker)
                    .await?
                    .unwrap_or(0);

            if pending_trade_amount == 0 {
                self.generate_trades(&claim.ticker, &claim.amount, None, None)
                    .await?;
            }
        }
        Ok(())
    }

    async fn reconcile_house_positions(&self) -> Result<()> {
        let house_positions =
            db::get_positions_by_owner(self.db_client.as_ref(), &Owner::House).await?;
        let house_positions = house_positions
            .iter()
            .filter(|pos| pos.shares != Decimal::ZERO);
        for position in house_positions {
            if position.shares.abs() > Decimal::from_f64(0.99).unwrap() {
                let mut shares_to_liquidate = position.shares.abs() - Decimal::ONE;
                shares_to_liquidate.set_sign_positive(position.shares.is_sign_positive());
                self.generate_trades(
                    &position.ticker,
                    &Amount::Shares(-shares_to_liquidate),
                    None,
                    None,
                )
                .await?
            }
        }
        Ok(())
    }

    async fn send_trade(&self, trade: TradeIntent) -> Result<()> {
        db::save_pending_trade(
            self.db_client.as_ref(),
            PendingTrade::new(trade.id, trade.ticker.clone(), trade.qty as i32),
        )
        .await
        .context("Failed to save pending trade")?;

        self.trade_sender
            .send(trade)
            .await
            .context("Failed to send trade")?;
        Ok(())
    }
}
