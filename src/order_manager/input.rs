use super::OrderManager;
use crate::db;
use alpaca::AlpacaMessage;
use anyhow::{anyhow, Context, Result};
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
                db::delete_scheduled_intent(&*self.db_client.read().await, intent.id).await?;
                Ok(Input::PositionIntent(intent))
            }
        }
    }

    pub async fn handle_input(&self, input: Result<Input>) -> Result<()> {
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
            Ok(Input::RiskCheckResponse(response)) => {
                self.handle_risk_check_response(response)
                    .await
                    .context("Failed to handle RiskCheckResponse")?;
            }
            Err(e) => return Err(e),
        };
        debug!("Finished handling input");
        Ok(())
    }
}
