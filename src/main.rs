use order_manager::run;
use order_manager::Settings;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings::new().unwrap();
    println!("{:?}", settings);
    let _consumer = order_manager::kafka::consumer(&settings.kafka).unwrap();
    let _producer = order_manager::kafka::producer(&settings.kafka).unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&settings.database.url)
        .await?;
    run().await
}
