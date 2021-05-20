use crate::manager::OrderManager;
use alpaca::{rest::orders::OrderIntent, stream::AlpacaMessage};
use anyhow::{anyhow, Context, Result};
use chrono::prelude::*;
use futures::prelude::*;
use mongodb::Client;
use rdkafka::{producer::FutureProducer, Message};
use serde::{Deserialize, Serialize};
pub use settings::Settings;
use tokio::sync::mpsc::unbounded_channel;
use tracing::{error, info};

mod db;
pub mod manager;
mod order_generator;
pub mod settings;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PositionIntentCondition {
    Id(String),
    After(DateTime<Utc>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PositionIntent {
    pub id: String,
    pub strategy: String,
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub qty: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<PositionIntentCondition>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Long,
    Short,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Position {
    pub ticker: String,
    pub avg_entry_price: f64,
    pub qty: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DependentOrder {
    client_order_id: String,
    order: OrderIntent,
}

#[allow(clippy::large_enum_variant)]
#[derive(Deserialize)]
#[serde(untagged)]
pub enum Input {
    PositionIntent(PositionIntent),
    AlpacaMessage(AlpacaMessage),
}

async fn send_order_intent(producer: &FutureProducer, oi: OrderIntent) -> Result<()> {
    let payload = serde_json::to_string(&oi)?;
    let record = rdkafka::producer::FutureRecord::to("order-intents")
        .key(&oi.symbol)
        .payload(&payload);
    producer
        .send(record, std::time::Duration::from_secs(0))
        .await
        .map_err(|(e, m)| anyhow!("Error: {:?}\nMessage: {:?}", e, m))?;
    Ok(())
}

pub async fn run(settings: Settings) -> Result<()> {
    info!("Starting order-manager");
    let client = Client::with_uri_str(&settings.database.url)
        .await
        .context("Failed to create mongo client")?;
    let producer = kafka_settings::producer(&settings.kafka)?;
    let consumer = kafka_settings::consumer(&settings.kafka)?;
    let (tx, mut rx) = unbounded_channel::<OrderIntent>();
    tokio::spawn(async move {
        while let Some(oi) = rx.recv().await {
            if let Err(e) = send_order_intent(&producer, oi).await {
                error!("{:?}", e)
            }
        }
    });
    let database = client.database(&settings.database.name);
    let order_manager = OrderManager::new(database);
    consumer
        .stream()
        .map(|msg| {
            let parsed: Input = serde_json::from_slice(msg.unwrap().payload().unwrap()).unwrap();
            parsed
        })
        .map(|inp| async { order_manager.handle_message(inp).await })
        .for_each(|oi| async { tx.send(oi.await.unwrap().unwrap()).unwrap() })
        .await;

    Ok(())
}
