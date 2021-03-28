#![feature(type_alias_impl_trait)]
use crate::manager::OrderManager;
use anyhow::{Context, Result};
use chrono::prelude::*;
use mongodb::Client;
use serde::{Deserialize, Serialize};
pub use settings::Settings;
use stream_processor::StreamRunner;

pub mod manager;
pub mod policy;
pub mod settings;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PositionIntent {
    pub strategy: String,
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub qty: i32,
}

pub async fn run(settings: Settings) -> Result<()> {
    let client = Client::with_uri_str(&settings.database.url).await?;
    let database = client.database(&settings.database.name);
    let order_manager = OrderManager::new().bind(database);
    let runner = StreamRunner::new(order_manager, settings.kafka);
    runner
        .run()
        .await
        .context("Failed in running stream-processing loop")
}
