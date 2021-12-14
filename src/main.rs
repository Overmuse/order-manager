use anyhow::Result;
use order_manager::run;
use order_manager::Settings;
use sentry::{ClientOptions, IntoDsn};
use sqlx::{migrate::Migrator, Connection, PgConnection};
use std::path::Path;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let settings = Settings::new()?;
    let mut conn = PgConnection::connect(&format!("{}/{}", settings.database.url, settings.database.name)).await?;
    let runner = Migrator::new(Path::new("migrations")).await?;
    runner.run(&mut conn).await?;
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
