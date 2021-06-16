use anyhow::Result;
use order_manager::run;
use order_manager::Settings;
use tracing::subscriber::set_global_default;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::{layer::SubscriberExt, Registry};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let formatting_layer = BunyanFormattingLayer::new("order-manager".into(), std::io::stdout);
    let subscriber = Registry::default()
        .with(JsonStorageLayer)
        .with(formatting_layer);
    set_global_default(subscriber)?;
    LogTracer::init().expect("Failed to set logger");
    let settings = Settings::new()?;
    run(settings).await
}
