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
        std::env::set_var("DATABASE__NAME", "testdb");
        std::env::set_var("DATABASE__URL", "localhost:27017");
        std::env::set_var("DATABASE__USERNAME", "mongo");
        std::env::set_var("DATABASE__PASSWORD", "password");
        std::env::set_var("KAFKA__BOOTSTRAP_SERVER", "localhost:9094");
        std::env::set_var("KAFKA__GROUP_ID", "order-manager");
        std::env::set_var("KAFKA__INPUT_TOPICS", "overmuse-trades,position-intents");
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "localhost:9094");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");
        let settings = Settings::new();
        run(settings.unwrap()).await
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
    let original_id = order_intent.client_order_id.unwrap();

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

    // TEST 3: After an order update, we still account for pending amounts
    let new_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"new","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":null,"expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"50","filled_qty":"0","filled_avg_price":null,"order_class":"","order_type":"market","type":"market","side":"buy","time_in_force":"day","limit_price":null,"stop_price":null,"status":"new","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
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
    let payload = position_payload("AAPL".into(), 151);
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
    assert_eq!(order_intent.qty, 1);

    // TEST 4: After an order fill, we still account for pending amounts
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"50","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"50","filled_qty":"50","filled_avg_price":"100.0","order_class":"","order_type":"market","type":"market","side":"buy","time_in_force":"day","limit_price":null,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        id
    );
    let order_update = FutureRecord::to("overmuse-trades")
        .key("AAPL")
        .payload(&fill_message);
    producer
        .send_result(order_update)
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    let payload = position_payload("AAPL".into(), 152);
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
    assert_eq!(order_intent.qty, 1);

    // TEST 5: After an order cancellation, we need to send more volume
    let new_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"new","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47e","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","canceled_at":null,"expired_at":null,"filled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"100","filled_qty":"0","filled_avg_price":null,"order_class":"","order_type":"market","type":"market","side":"buy","time_in_force":"day","limit_price":null,"stop_price":null,"status":"new","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        original_id
    );
    let cancel_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"canceled","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47e","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","canceled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"filled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"100","filled_qty":"0","filled_avg_price":null,"order_class":"","order_type":"market","type":"market","side":"buy","time_in_force":"day","limit_price":null,"stop_price":null,"status":"canceled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        original_id
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
    let order_update = FutureRecord::to("overmuse-trades")
        .key("AAPL")
        .payload(&cancel_message);
    producer
        .send_result(order_update)
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    let payload = position_payload("AAPL".into(), 153);
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
    assert_eq!(order_intent.qty, 101);

    teardown(&mongo_client, &admin, &admin_options).await;
    Ok(())
}
