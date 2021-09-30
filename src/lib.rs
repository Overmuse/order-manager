use anyhow::{Context, Result};
use kafka_settings::{consumer, producer};
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio_postgres::{connect, NoTls};
use tracing::error;

mod db;
mod event_sender;
mod intent_scheduler;
pub mod order_manager;
mod settings;
pub mod types;
mod webserver;

use crate::order_manager::OrderManager;
pub use event_sender::Event;
use event_sender::EventSenderHandle;
use intent_scheduler::IntentScheduler;
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
    let event_sender_handle = EventSenderHandle::new(producer);
    let intent_scheduler = IntentScheduler::new(scheduled_intents_tx1, scheduled_intents_rx2);
    let (mut client, connection) =
        connect(&format!("{}/{}", settings.database.url, settings.database.name,), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });
    embedded::migrations::runner().run_async(&mut client).await?;
    let client = Arc::new(client);
    let order_manager = OrderManager::new(
        consumer,
        scheduled_intents_tx2,
        scheduled_intents_rx1,
        event_sender_handle,
        client.clone(),
        settings.datastore.base_url,
        settings.app,
    );
    tokio::join!(
        webserver::run(settings.webserver.port, client),
        order_manager.run(),
        intent_scheduler.run()
    );
    Ok(())
}
