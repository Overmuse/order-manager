use crate::db;
use crate::order_manager::input::State;
use crate::redis::Redis;
use crate::settings::AppSettings;
use crate::types::PendingTrade;
use crate::TradeSenderHandle;
use alpaca::AlpacaMessage;
use anyhow::{Context, Result};
use rdkafka::consumer::StreamConsumer;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_postgres::Client;
use tracing::{debug, error, info};
use trading_base::{PositionIntent, TradeIntent};

mod dependent_trades;
mod input;
mod intents;
mod order_updates;
mod reconciliation;
use input::Input;

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
        if let Err(e) = self.initalize().await.context("Failed to initialize order manager") {
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
            self.schedule_position_intent(intent)
                .await
                .context("Failed to schedule position intent")?
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
        debug!("Finished handling input");
        Ok(())
    }

    async fn send_trade(&self, trade: TradeIntent) -> Result<()> {
        db::save_pending_trade(
            self.db_client.as_ref(),
            PendingTrade::new(trade.id, trade.ticker.clone(), trade.qty as i32),
        )
        .await
        .context("Failed to save pending trade")?;

        self.trade_sender.send(trade).await.context("Failed to send trade")?;
        Ok(())
    }
}
