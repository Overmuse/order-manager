use alpaca::orders::OrderIntent;
use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use futures::FutureExt;
use order_manager::{run, Settings};
use position_intents::{AmountSpec, PositionIntent, TickerSpec, UpdatePolicy};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use rust_decimal::Decimal;
use tracing::{debug, info};
use tracing_log::LogTracer;
use uuid::Uuid;

use order_events::send_order_event;
use setup::setup;
use teardown::teardown;
mod order_events;
mod setup;
mod teardown;

async fn send_position(producer: &FutureProducer, intent: &PositionIntent) -> Result<()> {
    let payload = serde_json::to_vec(intent).unwrap();
    let key = match &intent.ticker {
        TickerSpec::Ticker(ticker) => ticker,
        TickerSpec::All => "",
    }
    .to_string();
    let message = FutureRecord::to("position-intents")
        .key(&key)
        .payload(&payload);
    producer
        .send_result(message)
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
    //LogTracer::init()?;
    let database_address = "postgres://postgres:password@localhost:5432";
    let database_name = Uuid::new_v4().to_string();
    let (admin, admin_options, consumer, producer) = setup(database_address, &database_name).await;
    debug!("Subscribing to topics");
    consumer.subscribe(&[&"order-intents"]).unwrap();
    consumer
        .subscription()
        .unwrap()
        .set_all_offsets(rdkafka::topic_partition_list::Offset::End)
        .unwrap();

    tokio::spawn(async move {
        std::env::set_var("DATABASE__NAME", database_name);
        std::env::set_var("DATABASE__URL", database_address);
        std::env::set_var("KAFKA__BOOTSTRAP_SERVER", "localhost:9094");
        std::env::set_var("KAFKA__GROUP_ID", Uuid::new_v4().to_string());
        std::env::set_var("KAFKA__INPUT_TOPICS", "overmuse-trades,position-intents");
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "localhost:9094");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");
        std::env::set_var("KAFKA__ACKS", "0");
        std::env::set_var("KAFKA__RETRIES", "0");
        let settings = Settings::new();
        tracing::debug!("{:?}", settings);
        let res = run(settings.unwrap()).await;
        tracing::error!("{:?}", res);
    });
    //
    // TODO: Replace this sleep with a liveness check
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // TEST 1: An initial position intent leads to an order intent for the full size of the
    // position intent.
    info!("Test 1");
    send_position(
        &producer,
        &PositionIntent::builder("S1", "AAPL", AmountSpec::Shares(Decimal::new(100, 0)))
            .build()
            .unwrap(),
    )
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
    send_position(
        &producer,
        &PositionIntent::builder("S1", "AAPL", AmountSpec::Shares(Decimal::new(150, 0)))
            .build()
            .unwrap(),
    )
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
    send_position(
        &producer,
        &PositionIntent::builder("S1", "AAPL", AmountSpec::Shares(Decimal::new(-100, 0)))
            .build()
            .unwrap(),
    )
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
    send_position(
        &producer,
        &PositionIntent::builder("S1", "AAPL", AmountSpec::Shares(Decimal::new(-1005, 1)))
            .build()
            .unwrap(),
    )
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

    // TEST 5: Can send Zero position size
    info!("Test 5");
    send_position(
        &producer,
        &PositionIntent::builder("S1", "AAPL", AmountSpec::Zero)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    let order_intent = receive_oi(&consumer).await.unwrap();
    assert_eq!(order_intent.qty, 101);
    assert_eq!(order_intent.side, alpaca::common::Side::Buy);
    let new_id = order_intent.client_order_id.unwrap();
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"0","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"101","filled_qty":"101","filled_avg_price":"100.0","order_class":"","order_type":"market","type":"market","side":"buy","time_in_force":"day","limit_price":null,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        new_id
    );
    send_order_event(&producer, &fill_message).await.unwrap();

    // TEST 6: Can deal with multiple strategies, dollar amounts and limit orders
    info!("Test 6");
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", AmountSpec::Dollars(Decimal::new(10000, 0)))
            .limit_price(Decimal::new(100, 0))
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    let order_intent = receive_oi(&consumer).await.unwrap();
    assert_eq!(order_intent.qty, 100);
    assert_eq!(order_intent.side, alpaca::common::Side::Buy);
    assert_eq!(
        order_intent.order_type,
        alpaca::OrderType::Limit {
            limit_price: Decimal::new(100, 0)
        }
    );
    let new_id = order_intent.client_order_id.unwrap();
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"100","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"100","filled_qty":"100","filled_avg_price":"100.0","order_class":"","order_type":"limit","type":"limit","side":"buy","time_in_force":"day","limit_price":100.0,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        new_id
    );
    send_order_event(&producer, &fill_message).await.unwrap();

    // Test 7: Can send `Retain` `AmountSpec`s and not generate orders
    info!("Test 7");
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", AmountSpec::Zero)
            .update_policy(UpdatePolicy::RetainLong)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(consumer.recv().now_or_never().is_none());
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", AmountSpec::Zero)
            .update_policy(UpdatePolicy::Retain)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(consumer.recv().now_or_never().is_none());
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", AmountSpec::Zero)
            .update_policy(UpdatePolicy::RetainShort)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    let order_intent = receive_oi(&consumer).await.unwrap();
    let new_id = order_intent.client_order_id.unwrap();
    assert_eq!(order_intent.qty, 100);
    assert_eq!(order_intent.side, alpaca::common::Side::Sell);
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"0","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"100","filled_qty":"100","filled_avg_price":"100.0","order_class":"","order_type":"market","type":"market","side":"sell","time_in_force":"day","limit_price":null,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        new_id
    );
    send_order_event(&producer, &fill_message).await.unwrap();

    // Test 8: Can send expired intent and have no generated orders
    info!("Test 8");
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", AmountSpec::Shares(Decimal::new(100, 0)))
            .before(Utc::now() - Duration::days(1))
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(consumer.recv().now_or_never().is_none());

    // Test 9: Can send intent to be scheduled
    info!("Test 9");
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", AmountSpec::Shares(Decimal::new(100, 0)))
            .after(Utc::now() + Duration::seconds(1))
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    let order_intent = receive_oi(&consumer).await.unwrap();
    let new_id = order_intent.client_order_id.unwrap();
    let fill_message = format!(
        r#"{{"stream":"trade_updates","data":{{"event":"fill","position_qty":"100","price":"100.0","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:39:00.0000000Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"AAPL","asset_class":"us_equity","notional":null,"qty":"100","filled_qty":"100","filled_avg_price":"100.0","order_class":"","order_type":"market","type":"market","side":"sell","time_in_force":"day","limit_price":null,"stop_price":null,"status":"filled","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
        new_id
    );
    send_order_event(&producer, &fill_message).await.unwrap();

    // Test 10: Can send intent to close all positions
    info!("Test 10");
    send_position(
        &producer,
        &PositionIntent::builder("S2", TickerSpec::All, AmountSpec::Zero)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    let order_intent = receive_oi(&consumer).await.unwrap();
    assert_eq!(order_intent.qty, 100);
    assert_eq!(order_intent.side, alpaca::common::Side::Sell);

    teardown(&admin, &admin_options).await;
    Ok(())
}
