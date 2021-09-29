use crate::types::{Allocation, Claim, Lot};
use anyhow::Result;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};
use trading_base::TradeIntent;

struct EventSender {
    producer: FutureProducer,
    receiver: mpsc::Receiver<Event>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Event {
    TradeIntent(TradeIntent),
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
                Event::TradeIntent(ti) => ("trade-intents", ti.ticker),
                Event::Allocation(alloc) => ("allocations", alloc.ticker),
                Event::Claim(claim) => ("claims", claim.ticker),
                Event::Lot(lot) => ("lots", lot.ticker),
                Event::RiskCheckRequest(intent) => ("risk-check-request", intent.ticker),
            };
            let record = FutureRecord::to(topic).key(&key).payload(&payload);
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
