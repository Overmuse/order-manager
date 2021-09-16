use super::OrderManager;
use crate::db;
use alpaca::AlpacaMessage;
use anyhow::{anyhow, Result};
use rdkafka::Message;
use risk_manager::RiskCheckResponse;
use serde::{Deserialize, Serialize};
use tracing::debug;
use trading_base::PositionIntent;

#[derive(Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum State {
    Open { next_close: usize },
    Closed { next_open: usize },
}

#[derive(Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum Input {
    PositionIntent(PositionIntent),
    AlpacaMessage(AlpacaMessage),
    RiskCheckResponse(RiskCheckResponse),
    Time(State),
}

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub async fn receive_message(&mut self) -> Result<Input> {
        tokio::select! {
            kafka_message = self.kafka_consumer.recv() => {
                debug!("Message received from kafka");
                let message = kafka_message?;
                let payload = message.payload().ok_or_else(|| anyhow!("Empty payload"))?;
                Ok(serde_json::from_slice(payload)?)
            },
            scheduled_intent = self.scheduler_receiver.recv() => {
                debug!("Message received from scheduler");
                let intent = scheduled_intent.ok_or_else(|| anyhow!("Channel closed"))?;
                db::delete_scheduled_intent(self.db_client.as_ref(), intent.id).await?;
                Ok(Input::PositionIntent(intent))
            }
        }
    }
}
