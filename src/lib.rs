#![feature(type_alias_impl_trait)]
use crate::manager::OrderManager;
use alpaca::stream::AlpacaMessage;
use chrono::prelude::*;
use futures::channel::mpsc::unbounded;
use futures::prelude::*;
use log::debug;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::FutureRecord;
use serde::{Deserialize, Serialize};
use serde_json;
pub use settings::Settings;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

pub mod db;
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

struct MessageSummary {
    origin_topic: String,
    payload: String,
}

fn handle_message(
    msg: Result<BorrowedMessage, rdkafka::error::KafkaError>,
) -> Result<MessageSummary, Box<dyn std::error::Error>> {
    let owned = msg?.detach();
    let payload = owned.payload().unwrap();
    let msg: String = String::from_utf8(payload.into())?;
    debug!("Message: {:?}", msg);
    Ok(MessageSummary {
        origin_topic: owned.topic().into(),
        payload: msg,
    })
}

pub async fn run(settings: Settings) -> Result<(), Box<dyn std::error::Error>> {
    let consumer = kafka::consumer(&settings.kafka)?;
    consumer.subscribe(&["intended-positions", "overmuse-trades"])?;
    let producer = kafka::producer(&settings.kafka)?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&settings.database.url)
        .await?;
    let order_manager = OrderManager::new(pool);
    let (mut tx, mut rx) = unbounded::<MessageSummary>();
    tokio::spawn(async move {
        loop {
            let msg = rx.next().await.expect("No more messages");
            match msg.origin_topic.as_str() {
                "intended-positions" => {
                    let position: PositionIntent =
                        serde_json::from_str(&msg.payload).expect("Could not deserialize position");
                    let orders = order_manager
                        .make_orders(position.clone(), None)
                        .expect("Failed to make orders");
                    debug!("Orders: {:?}", orders);
                    for order in orders {
                        let payload =
                            serde_json::to_string(&order).expect("Could not serialize order");
                        let record = FutureRecord::to("order-intents")
                            .key(&position.ticker)
                            .payload(&payload);
                        producer
                            .send(record, Duration::from_secs(0))
                            .await
                            .expect("Error when sending message to kafka");
                    }
                }
                "overmuse-trades" => {
                    let alpaca_message: AlpacaMessage = serde_json::from_str(&msg.payload)
                        .expect("Could not deserialize alpaca message");
                    if let AlpacaMessage::TradeUpdates(order_event) = alpaca_message {
                        order_manager
                            .register_order(*order_event)
                            .await
                            .expect("Failed to save order");
                    }
                }
                _ => unreachable!(),
            }
        }
    });
    let mut msg_stream = consumer.stream().map(handle_message).map(|x| x.unwrap());
    while let Some(msg) = msg_stream.next().await {
        tx.send(msg).await.unwrap();
    }
    Ok(())
}
