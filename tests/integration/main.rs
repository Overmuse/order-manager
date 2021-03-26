use alpaca::orders::OrderIntent;
use anyhow::Result;
use order_manager::{run, Settings};
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureRecord;
use rdkafka::ClientConfig;
use rdkafka::Message;
use tracing::debug;

use position_intents::position_payload;
use setup::setup;
use teardown::teardown;
mod position_intents;
mod setup;
mod teardown;

#[tokio::main]
#[test]
async fn main() -> Result<()> {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", "localhost:9094");
    let (admin, admin_options, consumer, producer) = setup(&client_config).await;
    consumer.subscribe(&[&"order-intents"]).unwrap();

    tokio::spawn(async {
        let settings = Settings::new()?;
        debug!("Running order manager");
        run(settings).await
    });

    // TODO: Replace this sleep with a liveness check
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // TEST 1: An initial position intent leads to an order intent for the full size of the
    // position intent.
    let payload = position_payload("AAPL".into(), 100);
    let initial_intent = FutureRecord::to("position-intents")
        .key("AAPL")
        .payload(&payload);
    producer
        .send_result(initial_intent)
        .unwrap()
        .await
        .unwrap()
        .unwrap();

    let msg = consumer.recv().await.unwrap();
    let owned = msg.detach();
    let payload = owned.payload().unwrap();
    let order_intent: OrderIntent = serde_json::from_slice(payload).unwrap();
    assert_eq!(order_intent.qty, 100);

    // TEST 2: An additional position intent leads to an order intent with only the _net_ size
    // difference.
    let payload = position_payload("AAPL".into(), 150);
    let initial_intent = FutureRecord::to("position-intents")
        .key("AAPL")
        .payload(&payload);
    producer
        .send_result(initial_intent)
        .unwrap()
        .await
        .unwrap()
        .unwrap();

    let msg = consumer.recv().await.unwrap();
    let owned = msg.detach();
    let payload = owned.payload().unwrap();
    let order_intent: OrderIntent = serde_json::from_slice(payload).unwrap();
    assert_eq!(order_intent.qty, 50);

    teardown(&admin, &admin_options).await;
    Ok(())
}
