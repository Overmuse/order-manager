use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use futures::FutureExt;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use rust_decimal::Decimal;
use tracing::info;
use trading_base::{Amount, Identifier, OrderType, PositionIntent, TradeIntent, UpdatePolicy};

use order_manager::types::{Allocation, Claim, Lot, Owner};
use order_manager::Event;
use order_message::*;
use setup::setup;
use teardown::teardown;
mod order_message;
mod setup;
mod teardown;

async fn send_position(producer: &FutureProducer, intent: &PositionIntent) -> Result<()> {
    let payload = serde_json::to_vec(intent)?;
    let key = match &intent.identifier {
        Identifier::Ticker(ticker) => ticker,
        Identifier::All => "",
    }
    .to_string();
    let message = FutureRecord::to("position-intents").key(&key).payload(&payload);
    producer
        .send_result(message)
        .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?
        .await?
        .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?;
    Ok(())
}

async fn receive_event(consumer: &StreamConsumer) -> Result<Event> {
    let msg = consumer.recv().await?;
    let payload = msg.payload().ok_or(anyhow!("Missing payload"))?;
    let event: Event = serde_json::from_slice(payload)?;
    Ok(event)
}

async fn receive_claim_and_trade_intent(consumer: &StreamConsumer) -> Result<(Claim, TradeIntent)> {
    let events = tokio::try_join!(receive_event(&consumer), receive_event(&consumer))?;
    match events {
        (Event::Claim(c), Event::TradeIntent(ti)) => Ok((c, ti)),
        (Event::TradeIntent(ti), Event::Claim(c)) => Ok((c, ti)),
        _ => return Err(anyhow!("Unexpected events")),
    }
}

async fn receive_lot_and_allocation(consumer: &StreamConsumer) -> Result<(Lot, Allocation)> {
    let events = tokio::try_join!(receive_event(&consumer), receive_event(&consumer))?;
    match events {
        (Event::Allocation(a), Event::Lot(l)) => Ok((l, a)),
        (Event::Lot(l), Event::Allocation(a)) => Ok((l, a)),
        _ => Err(anyhow!("Unexpected events")),
    }
}

async fn receive_lot_allocation_and_house_allocation(
    consumer: &StreamConsumer,
) -> Result<(Lot, Allocation, Allocation)> {
    let events = tokio::try_join!(
        receive_event(&consumer),
        receive_event(&consumer),
        receive_event(&consumer)
    )?;
    match events {
        (Event::Allocation(a1), Event::Allocation(a2), Event::Lot(l)) => {
            if a1.owner == Owner::House {
                Ok((l, a2, a1))
            } else {
                Ok((l, a1, a2))
            }
        }
        (Event::Allocation(a1), Event::Lot(l), Event::Allocation(a2)) => {
            if a1.owner == Owner::House {
                Ok((l, a2, a1))
            } else {
                Ok((l, a1, a2))
            }
        }
        (Event::Lot(l), Event::Allocation(a1), Event::Allocation(a2)) => {
            if a1.owner == Owner::House {
                Ok((l, a2, a1))
            } else {
                Ok((l, a1, a2))
            }
        }
        _ => Err(anyhow!("Unexpected events")),
    }
}

/// An initial position intent leads to an trade intent for the full size of the
/// position intent.
async fn test_1(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S1", "AAPL", Amount::Shares(Decimal::new(100, 0))).build()?,
    )
    .await?;
    let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Shares(Decimal::ONE_HUNDRED));
    assert_eq!(trade_intent.qty, 100);
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 100,
        position_qty: 100,
        price: 100.0,
        filled_qty: 100,
        filled_avg_price: 100.0,
        side: Side::Buy,
        limit_price: None,
    };

    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::ONE_HUNDRED);
    assert_eq!(allocation.shares, Decimal::ONE_HUNDRED);
    assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
    Ok(())
}

/// An additional position intent leads to an trade intent with only the _net_ size
/// difference.
async fn test_2(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S1", "AAPL", Amount::Shares(Decimal::new(150, 0))).build()?,
    )
    .await?;
    let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Shares(Decimal::new(50, 0)));
    assert_eq!(trade_intent.qty, 50);
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 50,
        position_qty: 150,
        price: 100.0,
        filled_qty: 50,
        filled_avg_price: 100.0,
        side: Side::Buy,
        limit_price: None,
    };
    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::new(50, 0));
    assert_eq!(allocation.shares, Decimal::new(50, 0));
    assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
    Ok(())
}

/// A change in net side generates one initial trade and one additional trade
async fn test_3(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S1", "AAPL", Amount::Shares(-Decimal::new(100, 0))).build()?,
    )
    .await?;
    let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Shares(-Decimal::new(250, 0)));
    assert_eq!(trade_intent.qty, -150);
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 150,
        position_qty: 0,
        price: 100.0,
        filled_qty: 150,
        filled_avg_price: 100.0,
        side: Side::Sell,
        limit_price: None,
    };
    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::new(-150, 0));
    assert_eq!(allocation.shares, Decimal::new(-150, 0));
    assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));

    let event = receive_event(&consumer).await?;
    let trade_intent = match event {
        Event::TradeIntent(ti) => ti,
        _ => return Err(anyhow!("Unexpected event")),
    };

    assert_eq!(trade_intent.qty, -100);
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 100,
        position_qty: 100,
        price: 100.0,
        filled_qty: 100,
        filled_avg_price: 100.0,
        side: Side::Sell,
        limit_price: None,
    };
    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::new(-100, 0));
    assert_eq!(allocation.shares, Decimal::new(-100, 0));
    assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
    Ok(())
}

/// A fractional position intent still leads to integer trades
async fn test_4(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S1", "AAPL", Amount::Shares(Decimal::new(-1005, 1))).build()?,
    )
    .await?;

    let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Shares(-Decimal::new(5, 1)));
    assert_eq!(trade_intent.qty, -1);
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 1,
        position_qty: -101,
        price: 100.0,
        filled_qty: 1,
        filled_avg_price: 100.0,
        side: Side::Sell,
        limit_price: None,
    };

    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation, house_alloc) = receive_lot_allocation_and_house_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::new(-1, 0));
    assert_eq!(allocation.shares, Decimal::new(-5, 1));
    assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
    assert_eq!(house_alloc.shares, Decimal::new(-5, 1));
    Ok(())
}

/// Can send Zero position size
async fn test_5(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(&producer, &PositionIntent::builder("S1", "AAPL", Amount::Zero).build()?).await?;

    let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Shares(Decimal::new(1005, 1)));
    assert_eq!(trade_intent.qty, 101);
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 101,
        position_qty: 0,
        price: 100.0,
        filled_qty: 101,
        filled_avg_price: 100.0,
        side: Side::Buy,
        limit_price: None,
    };

    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation, house_alloc) = receive_lot_allocation_and_house_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::new(101, 0));
    assert_eq!(allocation.shares, Decimal::new(1005, 1));
    assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
    assert_eq!(house_alloc.shares, Decimal::new(5, 1));
    Ok(())
}

/// Can deal with multiple strategies, dollar amount and limit orders
async fn test_6(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", Amount::Dollars(Decimal::new(10000, 0)))
            .limit_price(Decimal::new(100, 0))
            .build()?,
    )
    .await?;

    let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Dollars(Decimal::new(10000, 0)));
    assert_eq!(trade_intent.qty, 100);
    assert_eq!(
        trade_intent.order_type,
        OrderType::Limit {
            limit_price: Decimal::new(100, 0)
        }
    );
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 100,
        position_qty: 100,
        price: 100.0,
        filled_qty: 100,
        filled_avg_price: 100.0,
        side: Side::Buy,
        limit_price: Some(100.0),
    };

    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::new(100, 0));
    assert_eq!(allocation.shares, Decimal::new(100, 0));
    assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
    Ok(())
}

/// Can send `Retain` `Amount`s and not generate orders
async fn test_7(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", Amount::Zero)
            .update_policy(UpdatePolicy::RetainLong)
            .build()?,
    )
    .await?;
    assert!(consumer.recv().now_or_never().is_none());
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", Amount::Zero)
            .update_policy(UpdatePolicy::Retain)
            .build()?,
    )
    .await?;
    assert!(consumer.recv().now_or_never().is_none());
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", Amount::Zero)
            .update_policy(UpdatePolicy::RetainShort)
            .build()?,
    )
    .await?;

    let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Shares(-Decimal::ONE_HUNDRED));
    assert_eq!(trade_intent.qty, -100);
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 100,
        position_qty: 0,
        price: 100.0,
        filled_qty: 100,
        filled_avg_price: 100.0,
        side: Side::Sell,
        limit_price: None,
    };

    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::new(-100, 0));
    assert_eq!(allocation.shares, Decimal::new(-100, 0));
    assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
    Ok(())
}

/// Can send expired intent and have no generated orders
async fn test_8(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", Amount::Shares(Decimal::new(300, 0)))
            .before(Utc::now() - Duration::days(1))
            .build()?,
    )
    .await?;
    assert!(consumer.recv().now_or_never().is_none());
    Ok(())
}

/// Can schedule intent
async fn test_9(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", Amount::Shares(Decimal::new(100, 0)))
            .after(Utc::now() + Duration::seconds(1))
            .build()?,
    )
    .await?;

    let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Shares(Decimal::ONE_HUNDRED));
    assert_eq!(trade_intent.qty, 100);
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 100,
        position_qty: 100,
        price: 100.0,
        filled_qty: 100,
        filled_avg_price: 100.0,
        side: Side::Buy,
        limit_price: None,
    };

    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::new(100, 0));
    assert_eq!(allocation.shares, Decimal::new(100, 0));
    assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
    Ok(())
}

async fn test_10(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S2", "AAPL", Amount::Shares(Decimal::new(200, 0))).build()?,
    )
    .await?;

    let (claim, _trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Shares(Decimal::ONE_HUNDRED));
    info!("SLEEPING 1 SECOND TO LET UNREPORTED TRADE EXPIRE");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let record = FutureRecord::to("time")
        .key("")
        .payload(r#"{"state":"open","next_close":710}"#);
    producer.send_result(record).map_err(|x| x.0)?.await?.map_err(|x| x.0)?;
    //let event = receive_event(&consumer).await?;
    //let trade_intent = match event {
    //    Event::TradeIntent(ti) => ti,
    //    _ => return Err(anyhow!("Unexpected event")),
    //};
    //let client_order_id = trade_intent.id;
    //let fill_message = OrderMessage {
    //    client_order_id,
    //    event_type: EventType::Fill,
    //    ticker: "AAPL",
    //    qty: 100,
    //    position_qty: 200,
    //    price: 100.0,
    //    filled_qty: 100,
    //    filled_avg_price: 100.0,
    //    side: Side::Buy,
    //    limit_price: None,
    //};

    //send_order_message(&producer, &fill_message).await?;
    //let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
    //assert_eq!(lot.shares, Decimal::new(100, 0));
    //assert_eq!(allocation.shares, Decimal::new(100, 0));
    //assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
    Ok(())
}

async fn test_11(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
    send_position(
        &producer,
        &PositionIntent::builder("S2", Identifier::All, Amount::Zero).build()?,
    )
    .await?;

    let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
    assert_eq!(claim.amount, Amount::Shares(-Decimal::new(100, 0)));
    assert_eq!(trade_intent.qty, -100);
    let client_order_id = trade_intent.id;
    let fill_message = OrderMessage {
        client_order_id,
        event_type: EventType::Fill,
        ticker: "AAPL",
        qty: 100,
        position_qty: 0,
        price: 100.0,
        filled_qty: 100,
        filled_avg_price: 100.0,
        side: Side::Sell,
        limit_price: None,
    };

    send_order_message(&producer, &fill_message).await?;
    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
    assert_eq!(lot.shares, Decimal::new(-100, 0));
    assert_eq!(allocation.shares, Decimal::new(-100, 0));
    assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
    Ok(())
}

#[tokio::test]
async fn main() -> Result<()> {
    let (admin, admin_options, consumer, producer) = setup().await;
    // TODO: Replace this sleep with a liveness check
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Send initial time record in order to clean up pending trades
    let record = FutureRecord::to("time")
        .key("")
        .payload(r#"{"state":"open","next_close":710}"#);
    producer.send_result(record).map_err(|x| x.0)?.await?.map_err(|x| x.0)?;

    info!("TEST 1");
    test_1(&producer, &consumer).await?;
    info!("TEST 2");
    test_2(&producer, &consumer).await?;
    info!("TEST 3");
    test_3(&producer, &consumer).await?;
    info!("TEST 4");
    test_4(&producer, &consumer).await?;
    info!("TEST 5");
    test_5(&producer, &consumer).await?;
    info!("TEST 6");
    test_6(&producer, &consumer).await?;
    info!("TEST 7");
    test_7(&producer, &consumer).await?;
    info!("TEST 8");
    test_8(&producer, &consumer).await?;
    info!("TEST 9");
    test_9(&producer, &consumer).await?;
    info!("TEST 10");
    test_10(&producer, &consumer).await?;
    info!("TEST 11");
    test_11(&producer, &consumer).await?;

    teardown(&admin, &admin_options).await;
    Ok(())
}
