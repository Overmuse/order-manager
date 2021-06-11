use super::OrderManager;
use alpaca::AlpacaMessage;
use anyhow::{anyhow, Result};
use position_intents::PositionIntent;
use rdkafka::Message;
use serde::Deserialize;
use tracing::debug;

#[derive(Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub(super) enum Input {
    PositionIntent(PositionIntent),
    AlpacaMessage(AlpacaMessage),
}

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub(super) async fn receive_message(&mut self) -> Result<Input> {
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
                Ok(Input::PositionIntent(intent))
            }
        }
    }
}
