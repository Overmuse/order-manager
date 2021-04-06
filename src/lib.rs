use crate::manager::OrderManager;
use alpaca::rest::orders::OrderIntent;
use anyhow::{Context, Result};
use chrono::prelude::*;
use mongodb::{
    options::{ClientOptions, Credential, StreamAddress},
    Client,
};
use serde::{Deserialize, Serialize};
pub use settings::Settings;
use stream_processor::StreamRunner;

mod db;
pub mod manager;
mod order_generator;
pub mod settings;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PositionIntent {
    pub strategy: String,
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub qty: i32,
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

pub async fn run(settings: Settings) -> Result<()> {
    let database_options = ClientOptions::builder()
        .hosts(vec![StreamAddress::parse(&settings.database.url)?])
        .credential(
            Credential::builder()
                .username(Some(settings.database.username))
                .password(Some(settings.database.password))
                .build(),
        )
        .build();
    let client = Client::with_options(database_options)?;
    let database = client.database(&settings.database.name);
    let order_manager = OrderManager::new().bind(database);
    let runner = StreamRunner::new(order_manager, settings.kafka);
    runner
        .run()
        .await
        .context("Failed in running stream-processing loop")
}
