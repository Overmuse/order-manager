use crate::db;
use alpaca::orders::OrderIntent;
use alpaca::AlpacaMessage;
use anyhow::{Context, Result};
use position_intents::PositionIntent;
use rdkafka::consumer::StreamConsumer;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_postgres::Client;
use tracing::{debug, error, info};

mod dependent_orders;
mod input;
mod intents;
mod order_updates;
use input::Input;
use intents::PositionIntentExt;

pub struct OrderManager {
    kafka_consumer: StreamConsumer,
    scheduler_sender: UnboundedSender<PositionIntent>,
    scheduler_receiver: UnboundedReceiver<PositionIntent>,
    order_sender: UnboundedSender<OrderIntent>,
    db_client: Arc<Client>,
}

impl OrderManager {
    pub fn new(
        kafka_consumer: StreamConsumer,
        scheduler_sender: UnboundedSender<PositionIntent>,
        scheduler_receiver: UnboundedReceiver<PositionIntent>,
        order_sender: UnboundedSender<OrderIntent>,
        db_client: Arc<Client>,
    ) -> Self {
        Self {
            kafka_consumer,
            scheduler_sender,
            scheduler_receiver,
            order_sender,
            db_client,
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

    async fn initalize(&mut self) -> Result<()> {
        debug!("Populating scheduled intents");
        let scheduled_intents = db::get_scheduled_indents(self.db_client.clone())
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

    async fn handle_input(&mut self, input: Result<Input>) -> Result<()> {
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
            Err(e) => return Err(e),
        };
        Ok(())
    }
}
