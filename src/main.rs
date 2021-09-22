use anyhow::Result;
use order_manager::run;
use order_manager::Settings;
use sqlx::{migrate::Migrator, Connection, PgConnection};
use std::path::Path;
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
    let mut conn = PgConnection::connect(&format!("{}/{}", settings.database.url, settings.database.name)).await?;
    let runner = Migrator::new(Path::new("migrations")).await?;
    runner.run(&mut conn).await?;
    run(settings).await
}
