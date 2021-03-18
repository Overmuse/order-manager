use order_manager::Settings;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let settings = Settings::new().unwrap();
    println!("{:?}", settings);
    let _consumer = order_manager::kafka::consumer(&settings.kafka).unwrap();
    let _producer = order_manager::kafka::producer(&settings.kafka).unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&settings.database.url)
        .await?;
    println!(
        "{:?}",
        order_manager::db::get_all_positions(&pool).await.unwrap()
    );

    Ok(())
}
