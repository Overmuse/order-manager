use env_logger;
use order_manager::run;
use order_manager::Settings;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let settings = Settings::new().unwrap();
    run(settings).await
}
