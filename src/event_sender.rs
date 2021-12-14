use crate::types::{Allocation, Claim, Lot};
use anyhow::Result;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};
use trading_base::{TradeIntent, TradeMessage};

struct EventSender {
    producer: FutureProducer,
    receiver: mpsc::Receiver<Event>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Event {
    TradeMessage(TradeMessage),
    Allocation(Allocation),
    Claim(Claim),
    Lot(Lot),
    RiskCheckRequest(TradeIntent),
}

impl EventSender {
    fn new(producer: FutureProducer, receiver: mpsc::Receiver<Event>) -> Self {
        Self { producer, receiver }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(&mut self) {
        info!("Starting EventSender");
        while let Some(event) = self.receiver.recv().await {
            info!("Sending event {:?}", event);
            let payload = serde_json::to_string(&event);
            if let Err(e) = payload {
                return error!("{:?}", e);
            }
            let payload = payload.unwrap();
            let (topic, key) = match event {
                Event::TradeMessage(ref tm) => match tm {
                    TradeMessage::New { intent } => ("trade-intents", intent.ticker.as_str()),
                    TradeMessage::Cancel { .. } => ("trade-intents", ""),
                },
                Event::Allocation(ref alloc) => ("allocations", alloc.ticker.as_str()),
                Event::Claim(ref claim) => ("claims", claim.ticker.as_str()),
                Event::Lot(ref lot) => ("lots", lot.ticker.as_str()),
                Event::RiskCheckRequest(ref intent) => ("risk-check-request", intent.ticker.as_str()),
            };
            let record = FutureRecord::to(topic).key(key).payload(&payload);
            let send = self.producer.send(record, Duration::ZERO).await;
            if let Err((e, m)) = send {
                error!("Error: {:?}\nMessage: {:?}", e, m)
            }
        }
        info!("Ending EventSender");
    }
}

pub struct EventSenderHandle {
    sender: mpsc::Sender<Event>,
}

impl EventSenderHandle {
    pub fn new(producer: FutureProducer) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = EventSender::new(producer, receiver);
        tokio::spawn(async move { actor.run().await });
        Self { sender }
    }

    pub async fn send(&self, msg: Event) -> Result<()> {
        self.sender.send(msg).await?;
        Ok(())
    }
}
