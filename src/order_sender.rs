use alpaca::orders::OrderIntent;
use anyhow::Result;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

struct OrderSender {
    producer: FutureProducer,
    receiver: mpsc::Receiver<OrderIntent>,
}

impl OrderSender {
    fn new(producer: FutureProducer, receiver: mpsc::Receiver<OrderIntent>) -> Self {
        Self { producer, receiver }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(&mut self) {
        info!("Starting OrderSender");
        while let Some(oi) = self.receiver.recv().await {
            info!("Sending order_intent {:?}", oi);
            let payload = serde_json::to_string(&oi);
            match payload {
                Ok(payload) => {
                    let record = FutureRecord::to("order-intents")
                        .key(&oi.symbol)
                        .payload(&payload);
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
        info!("Ending OrderSender");
    }
}

pub struct OrderSenderHandle {
    sender: mpsc::Sender<OrderIntent>,
}

impl OrderSenderHandle {
    pub fn new(producer: FutureProducer) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = OrderSender::new(producer, receiver);
        tokio::spawn(async move { actor.run().await });
        Self { sender }
    }

    pub async fn send(&self, msg: OrderIntent) -> Result<()> {
        self.sender.send(msg).await?;
        Ok(())
    }
}
