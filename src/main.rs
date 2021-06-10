use anyhow::Result;
use order_manager::run;
use order_manager::Settings;
use tracing::subscriber::set_global_default;
use tracing_log::LogTracer;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    set_global_default(subscriber)?;
    LogTracer::init().expect("Failed to set logger");
    let settings = Settings::new()?;
    run(settings).await
}
