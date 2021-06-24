use alpaca::orders::OrderIntent;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info};

pub struct PriceUpdater {
    producer: FutureProducer,
    order_queue: UnboundedReceiver<OrderIntent>,
}

impl OrderSender {
    pub fn new(producer: FutureProducer, order_queue: UnboundedReceiver<OrderIntent>) -> Self {
        Self {
            producer,
            order_queue,
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) {
        info!("Starting OrderSender");
        let producer = self.producer;
        while let Some(oi) = self.order_queue.recv().await {
            info!("Sending order_intent {:?}", oi);
            let payload = serde_json::to_string(&oi);
            match payload {
                Ok(payload) => {
                    let record = FutureRecord::to("order-intents")
                        .key(&oi.symbol)
                        .payload(&payload);
                    let send = producer.send(record, Duration::from_secs(0)).await;
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
