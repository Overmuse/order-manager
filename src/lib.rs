#![feature(type_alias_impl_trait)]
use crate::manager::OrderManager;
use chrono::prelude::*;
use futures::channel::mpsc::unbounded;
use futures::prelude::*;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::FutureRecord;
use serde::{Deserialize, Serialize};
use serde_json;
pub use settings::Settings;
use std::time::Duration;

//pub mod db;
pub mod kafka;
pub mod manager;
pub mod order;
pub mod policy;
pub mod settings;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PositionIntent {
    pub strategy: String,
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub qty: i32,
}

pub fn handle_message(
    msg: Result<BorrowedMessage, rdkafka::error::KafkaError>,
) -> Result<String, Box<dyn std::error::Error>> {
    let owned = msg?.detach();
    let payload = owned.payload().unwrap();
    let msg: String = String::from_utf8(payload.into())?;
    Ok(msg)
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings::new()?;
    let consumer = kafka::consumer(&settings.kafka)?;
    consumer.subscribe(&["position-intents"])?;
    let producer = kafka::producer(&settings.kafka)?;
    let order_manager = OrderManager::new();
    let (mut tx, mut rx) = unbounded::<String>();
    tokio::spawn(async move {
        loop {
            let msg = rx.next().await.expect("No more messages");
            let position: PositionIntent =
                serde_json::from_str(&msg).expect("Could not deserialize position");
            let orders = order_manager
                .make_orders(position, None)
                .expect("Failed to make orders");
            for order in orders {
                let payload = serde_json::to_string(&order).expect("Could not serialize order");
                let record = FutureRecord::to("test").key("test").payload(&payload);
                producer
                    .send(record, Duration::from_secs(0))
                    .await
                    .expect("Error when sending message to kafka");
            }
        }
    });
    let mut msg_stream = consumer.stream().map(handle_message).map(|x| x.unwrap());
    while let Some(msg) = msg_stream.next().await {
        tx.send(msg).await.unwrap();
    }
    Ok(())
}
