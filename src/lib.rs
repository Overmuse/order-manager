use anyhow::{Context, Result};
use kafka_settings::{consumer, producer};
use tokio::sync::mpsc::unbounded_channel;
use tokio_postgres::{connect, NoTls};
use tracing::error;

mod db;
mod intent_scheduler;
pub(crate) mod manager;
mod order_sender;
mod settings;
mod webserver;
use intent_scheduler::IntentScheduler;
use manager::OrderManager;
use order_sender::OrderSender;
pub use settings::Settings;
use webserver::WebServer;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

pub async fn run(settings: Settings) -> Result<()> {
    let consumer = consumer(&settings.kafka).context("Failed to create kafka consumer")?;
    let producer = producer(&settings.kafka).context("Failed to create kafka producer")?;
    let (order_tx, order_rx) = unbounded_channel();
    let (scheduled_intents_tx1, scheduled_intents_rx1) = unbounded_channel();
    let (scheduled_intents_tx2, scheduled_intents_rx2) = unbounded_channel();
    let order_sender = OrderSender::new(producer, order_rx);
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
    let webserver = WebServer::new();
    let order_manager = OrderManager::new(
        consumer,
        scheduled_intents_tx2,
        scheduled_intents_rx1,
        order_tx,
        client,
    );
    tokio::join!(
        webserver.run(),
        order_manager.run(),
        order_sender.run(),
        intent_scheduler.run()
    );
    Ok(())
}
