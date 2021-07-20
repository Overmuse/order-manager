use anyhow::{Context, Result};
use kafka_settings::{consumer, producer};
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio_postgres::{connect, NoTls};
use tracing::error;

mod db;
mod intent_scheduler;
pub mod order_manager;
mod order_sender;
mod settings;
pub mod types;
mod webserver;

use crate::order_manager::OrderManager;
use intent_scheduler::IntentScheduler;
use order_sender::OrderSenderHandle;
pub use settings::Settings;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

pub async fn run(settings: Settings) -> Result<()> {
    let consumer = consumer(&settings.kafka).context("Failed to create kafka consumer")?;
    let producer = producer(&settings.kafka).context("Failed to create kafka producer")?;
    let (scheduled_intents_tx1, scheduled_intents_rx1) = unbounded_channel();
    let (scheduled_intents_tx2, scheduled_intents_rx2) = unbounded_channel();
    let order_sender_handle = OrderSenderHandle::new(producer);
    let intent_scheduler = IntentScheduler::new(scheduled_intents_tx1, scheduled_intents_rx2);
    let (mut client, connection) = connect(
        &format!("{}/{}", settings.database.url, settings.database.name,),
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });
    embedded::migrations::runner()
        .run_async(&mut client)
        .await?;
    let client = Arc::new(client);
    let order_manager = OrderManager::new(
        consumer,
        scheduled_intents_tx2,
        scheduled_intents_rx1,
        order_sender_handle,
        client.clone(),
    );
    tokio::join!(
        webserver::run(settings.webserver.port, client),
        order_manager.run(),
        intent_scheduler.run()
    );
    Ok(())
}
