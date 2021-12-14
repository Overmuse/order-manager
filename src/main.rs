use anyhow::Result;
use order_manager::run;
use order_manager::Settings;
use sentry::{ClientOptions, IntoDsn};
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let settings = Settings::new()?;
    let _guard = sentry::init(ClientOptions {
        dsn: settings.sentry.dsn.clone().into_dsn().unwrap(),
        environment: Some(settings.sentry.environment.clone().into()),
        release: sentry::release_name!(),
        ..ClientOptions::default()
    });

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().json())
        .with(sentry_tracing::layer())
        .init();
    run(settings).await
}
