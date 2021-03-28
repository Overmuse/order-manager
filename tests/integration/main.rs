use alpaca::orders::OrderIntent;
use anyhow::Result;
use mongodb::Client;
use order_manager::{run, Settings};
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureRecord;
use rdkafka::Message;
use tracing::{debug, info};

use position_intents::position_payload;
use setup::setup;
use teardown::teardown;
mod position_intents;
mod setup;
mod teardown;

#[tokio::test]
async fn main() -> Result<()> {
    let mongo_client = Client::with_uri_str("mongodb://localhost:27017").await?;
    let (admin, admin_options, consumer, producer) = setup(&mongo_client).await;
    debug!("Subscribing to topics");
    consumer.subscribe(&[&"order-intents"]).unwrap();

    tokio::spawn(async {
        let settings = Settings::new().unwrap();
        debug!("Running order manager");
        run(settings).await
    });

    // TODO: Replace this sleep with a liveness check
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // TEST 1: An initial position intent leads to an order intent for the full size of the
    // position intent.
    info!("Test 1");
    let payload = position_payload("AAPL".into(), 100);
    let intent = FutureRecord::to("position-intents")
        .key("AAPL")
        .payload(&payload);
    producer
        .send_result(intent)
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
    info!("Test 2");
    let payload = position_payload("AAPL".into(), 150);
    let intent = FutureRecord::to("position-intents")
        .key("AAPL")
        .payload(&payload);
    producer
        .send_result(intent)
        .unwrap()
        .await
        .unwrap()
        .unwrap();

    let msg = consumer.recv().await.unwrap();
    let owned = msg.detach();
    let payload = owned.payload().unwrap();
    let order_intent: OrderIntent = serde_json::from_slice(payload).unwrap();
    assert_eq!(order_intent.qty, 50);
    let id = order_intent.client_order_id.unwrap();

    let new_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"new","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":null,"expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"500","filled_qty":"0","filled_avg_price":null,"order_class":"","order_type":"market","type":"market","side":"buy","time_in_force":"day","limit_price":null,"stop_price":null,"status":"accepted","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        id
    );

    let order_update = FutureRecord::to("overmuse-trades")
        .key("AAPL")
        .payload(&new_message);

    producer
        .send_result(order_update)
        .unwrap()
        .await
        .unwrap()
        .unwrap();

    teardown(&mongo_client, &admin, &admin_options).await;
    Ok(())
}
