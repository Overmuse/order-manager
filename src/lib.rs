use anyhow::{Context, Result};
use kafka_settings::{consumer, producer};
//use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc::unbounded_channel;

mod intent_scheduler;
mod manager;
mod order_sender;
mod settings;
use intent_scheduler::IntentScheduler;
use manager::OrderManager;
use order_sender::OrderSender;
pub use settings::Settings;

pub async fn run(settings: Settings) -> Result<()> {
    let consumer = consumer(&settings.kafka).context("Failed to create kafka consumer")?;
    let producer = producer(&settings.kafka).context("Failed to create kafka producer")?;
    let (order_tx, order_rx) = unbounded_channel();
    let (scheduled_intents_tx1, scheduled_intents_rx1) = unbounded_channel();
    let (scheduled_intents_tx2, scheduled_intents_rx2) = unbounded_channel();
    let order_sender = OrderSender::new(producer, order_rx);
    let intent_scheduler = IntentScheduler::new(scheduled_intents_tx1, scheduled_intents_rx2);
    //let pool = PgPoolOptions::new()
    //    .max_connections(5)
    //    .connect(&format!(
    //        "{}/{}",
    //        settings.database.url, settings.database.name,
    //    ))
    //    .await?;
    let order_manager = OrderManager::new(
        consumer,
        scheduled_intents_tx2,
        scheduled_intents_rx1,
        order_tx,
        //    pool,
    );
    tokio::join!(
        order_manager.run(),
        order_sender.run(),
        intent_scheduler.run()
    );
    Ok(())
}
