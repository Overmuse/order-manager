#![feature(type_alias_impl_trait)]
use crate::manager::OrderManager;
use anyhow::Result;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
pub use settings::Settings;
use sqlx::postgres::PgPoolOptions;
use stream_processor::{KafkaSettings, SecurityProtocol, StreamRunner};

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

pub async fn run(settings: Settings) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&settings.database.url)
        .await?;

    // TODO: Fix this ugliness
    let security_protocol = match settings.kafka.security_protocol {
        settings::SecurityProtocol::Plaintext => SecurityProtocol::Plaintext,
        settings::SecurityProtocol::SaslSsl {
            sasl_username,
            sasl_password,
        } => SecurityProtocol::SaslSsl {
            sasl_username,
            sasl_password,
        },
    };
    let kafka_settings = KafkaSettings::new(
        settings.kafka.bootstrap_servers,
        settings.kafka.group_id,
        security_protocol,
        vec!["intended-positions".into(), "overmuse-trades".into()],
    );
    let order_manager = OrderManager::new().bind(pool);
    let runner = StreamRunner::new(order_manager, kafka_settings);
    runner.run().await.map_err(From::from)
}
