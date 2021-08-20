use anyhow::Result;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};
use trading_base::TradeIntent;

struct TradeSender {
    producer: FutureProducer,
    receiver: mpsc::Receiver<TradeIntent>,
}

impl TradeSender {
    fn new(producer: FutureProducer, receiver: mpsc::Receiver<TradeIntent>) -> Self {
        Self { producer, receiver }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(&mut self) {
        info!("Starting TradeSender");
        while let Some(oi) = self.receiver.recv().await {
            info!("Sending trade_intent {:?}", oi);
            let payload = serde_json::to_string(&oi);
            match payload {
                Ok(payload) => {
                    let record = FutureRecord::to("trade-intents").key(&oi.ticker).payload(&payload);
                    let send = self.producer.send(record, Duration::from_secs(0)).await;
                    if let Err((e, m)) = send {
                        error!("Error: {:?}\nMessage: {:?}", e, m)
                    }
                }
                Err(e) => {
                    error!("{:?}", e)
                }
            }
        }
        info!("Ending TradeSender");
    }
}

pub struct TradeSenderHandle {
    sender: mpsc::Sender<TradeIntent>,
}

impl TradeSenderHandle {
    pub fn new(producer: FutureProducer) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = TradeSender::new(producer, receiver);
        tokio::spawn(async move { actor.run().await });
        Self { sender }
    }

    pub async fn send(&self, msg: TradeIntent) -> Result<()> {
        self.sender.send(msg).await?;
        Ok(())
    }
}
