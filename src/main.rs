use anyhow::Result;
use order_manager::run;
use order_manager::Settings;
use tracing::subscriber::set_global_default;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    set_global_default(subscriber)?;
    let settings = Settings::new()?;
    run(settings).await
}
