use crate::settings::AppSettings;
use crate::types::{Allocation, Claim, Lot};
use anyhow::Result;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};
use trading_base::TradeIntent;

struct EventSender {
    settings: AppSettings,
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
    fn new(settings: AppSettings, producer: FutureProducer, receiver: mpsc::Receiver<Event>) -> Self {
        Self {
            settings,
            producer,
            receiver,
        }
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
                Event::TradeIntent(ti) => (self.settings.topics.trade.as_str(), ti.ticker),
                Event::Allocation(alloc) => (self.settings.topics.allocation.as_str(), alloc.ticker),
                Event::Claim(claim) => (self.settings.topics.claim.as_str(), claim.ticker),
                Event::Lot(lot) => (self.settings.topics.lot.as_str(), lot.ticker),
                Event::RiskCheckRequest(ti) => (self.settings.topics.risk.as_str(), ti.ticker),
            };
            let record = FutureRecord::to(topic).key(&key).payload(&payload);
            let send = self.producer.send(record, Duration::from_secs(0)).await;
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
    pub fn new(settings: AppSettings, producer: FutureProducer) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = EventSender::new(settings, producer, receiver);
        tokio::spawn(async move { actor.run().await });
        Self { sender }
    }

    pub async fn send(&self, msg: Event) -> Result<()> {
        self.sender.send(msg).await?;
        Ok(())
    }
}
