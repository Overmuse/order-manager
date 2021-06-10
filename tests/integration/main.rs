use alpaca::orders::OrderIntent;
use anyhow::{anyhow, Result};
use order_manager::{run, Settings};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use rust_decimal::Decimal;
use tracing::{debug, info};

use intents::position_payload;
use order_events::send_order_event;
use setup::setup;
use teardown::teardown;
mod intents;
mod order_events;
mod setup;
mod teardown;

async fn send_position(producer: &FutureProducer, ticker: &str, qty: Decimal) -> Result<()> {
    let payload = position_payload(ticker.into(), qty);
    let intent = FutureRecord::to("position-intents")
        .key(ticker)
        .payload(&payload);
    producer
        .send_result(intent)
        .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?
        .await?
        .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?;
    Ok(())
}

async fn receive_oi(consumer: &StreamConsumer) -> Result<OrderIntent> {
    let msg = consumer.recv().await?;
    let payload = msg.payload().ok_or(anyhow!("Missing payload"))?;
    let order_intent: OrderIntent = serde_json::from_slice(payload)?;
    Ok(order_intent)
}

#[tokio::test]
async fn main() -> Result<()> {
    let (admin, admin_options, consumer, producer) = setup().await;
    debug!("Subscribing to topics");
    consumer.subscribe(&[&"order-intents"]).unwrap();
    consumer
        .subscription()
        .unwrap()
        .set_all_offsets(rdkafka::topic_partition_list::Offset::End)
        .unwrap();

    tokio::spawn(async {
        std::env::set_var("DATABASE__NAME", "order-manager");
        std::env::set_var(
            "DATABASE__URL",
            "postgres://postgres:password@localhost:5432/order-manager",
        );
        std::env::set_var("KAFKA__BOOTSTRAP_SERVER", "localhost:9094");
        std::env::set_var("KAFKA__GROUP_ID", "order-manager");
        std::env::set_var("KAFKA__INPUT_TOPICS", "overmuse-trades,position-intents");
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "localhost:9094");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");
        std::env::set_var("KAFKA__ACKS", "0");
        std::env::set_var("KAFKA__RETRIES", "0");
        let settings = Settings::new();
        run(settings.unwrap()).await
    });
    //
    // TODO: Replace this sleep with a liveness check
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // TEST 1: An initial position intent leads to an order intent for the full size of the
    // position intent.
    info!("Test 1");
    send_position(&producer, "AAPL", Decimal::new(100, 0))
        .await
        .unwrap();

    let order_intent = receive_oi(&consumer).await.unwrap();
    assert_eq!(order_intent.qty, 100);
    let original_id = order_intent.client_order_id.unwrap();

    // TEST 2: An additional position intent leads to an order intent with only the _net_ size
    // difference.
    info!("Test 2");
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"100","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"100","filled_qty":"100","filled_avg_price":"100.0","order_class":"","order_type":"market","type":"market","side":"buy","time_in_force":"day","limit_price":null,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        original_id
    );
    send_order_event(&producer, &fill_message).await.unwrap();
    send_position(&producer, "AAPL", Decimal::new(150, 0))
        .await
        .unwrap();
    let order_intent = receive_oi(&consumer).await.unwrap();
    assert_eq!(order_intent.qty, 50);
    let new_id = order_intent.client_order_id.unwrap();
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"150","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"50","filled_qty":"50","filled_avg_price":"100.0","order_class":"","order_type":"market","type":"market","side":"buy","time_in_force":"day","limit_price":null,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        new_id
    );
    send_order_event(&producer, &fill_message).await.unwrap();

    // TEST 3: A change in net side generates one initial trade and one deferred trade
    info!("Test 3");
    send_position(&producer, "AAPL", Decimal::new(-100, 0))
        .await
        .unwrap();
    let order_intent = receive_oi(&consumer).await.unwrap();
    assert_eq!(order_intent.qty, 150);
    assert_eq!(order_intent.side, alpaca::common::Side::Sell);
    let new_id = order_intent.client_order_id.unwrap();
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"0","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"150","filled_qty":"150","filled_avg_price":"100.0","order_class":"","order_type":"market","type":"market","side":"sell","time_in_force":"day","limit_price":null,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        new_id
    );
    send_order_event(&producer, &fill_message).await.unwrap();
    let order_intent = receive_oi(&consumer).await.unwrap();
    assert_eq!(order_intent.qty, 100);
    assert_eq!(order_intent.side, alpaca::common::Side::Sell);
    let new_id = order_intent.client_order_id.unwrap();
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"-100","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"100","filled_qty":"100","filled_avg_price":"100.0","order_class":"","order_type":"market","type":"market","side":"sell","time_in_force":"day","limit_price":null,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        new_id
    );
    send_order_event(&producer, &fill_message).await.unwrap();

    // TEST 4: A fractional position intent still leads to integer trade
    info!("Test 4");
    send_position(&producer, "AAPL", Decimal::new(-1005, 1))
        .await
        .unwrap();
    let order_intent = receive_oi(&consumer).await.unwrap();
    assert_eq!(order_intent.qty, 1);
    assert_eq!(order_intent.side, alpaca::common::Side::Sell);
    let new_id = order_intent.client_order_id.unwrap();
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"-1","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"1","filled_qty":"1","filled_avg_price":"100.0","order_class":"","order_type":"market","type":"market","side":"sell","time_in_force":"day","limit_price":null,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        new_id
    );
    send_order_event(&producer, &fill_message).await.unwrap();

    teardown(&admin, &admin_options).await;
    Ok(())
}
