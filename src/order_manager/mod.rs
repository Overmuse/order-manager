use crate::db;
use crate::event_sender::Event;
use crate::settings::AppSettings;
use crate::types::PendingTrade;
use crate::EventSenderHandle;
use anyhow::{Context, Result};
use rdkafka::consumer::StreamConsumer;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_postgres::Client;
use tracing::{debug, error, info};
use trading_base::{PositionIntent, TradeIntent, TradeMessage};

mod dependent_trades;
mod input;
mod intents;
mod order_updates;
mod reconciliation;
mod risk_check;

pub struct OrderManager {
    kafka_consumer: StreamConsumer,
    scheduler_sender: UnboundedSender<PositionIntent>,
    scheduler_receiver: UnboundedReceiver<PositionIntent>,
    event_sender: EventSenderHandle,
    db_client: Arc<Client>,
    datastore_url: String,
    settings: AppSettings,
}

impl OrderManager {
    pub fn new(
        kafka_consumer: StreamConsumer,
        scheduler_sender: UnboundedSender<PositionIntent>,
        scheduler_receiver: UnboundedReceiver<PositionIntent>,
        event_sender: EventSenderHandle,
        db_client: Arc<Client>,
        datastore_url: String,
        settings: AppSettings,
    ) -> Self {
        Self {
            kafka_consumer,
            scheduler_sender,
            scheduler_receiver,
            event_sender,
            db_client,
            datastore_url,
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
                .context("Failed to schedule position intent")?
        }
        Ok(())
    }

    async fn send_trade(&self, intent: TradeIntent) -> Result<()> {
        db::save_pending_trade(
            self.db_client.as_ref(),
            PendingTrade::new(intent.id, intent.ticker.clone(), intent.qty as i32),
        )
        .await
        .context("Failed to save pending trade")?;

        self.event_sender
            .send(Event::TradeMessage(TradeMessage::New { intent }))
            .await
            .context("Failed to send trade")?;
        Ok(())
    }
}
