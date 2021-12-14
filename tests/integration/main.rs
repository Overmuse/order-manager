use anyhow::Result;
use risk_manager::RiskCheckResponse;
use rust_decimal::Decimal;
use trading_base::{Amount, PositionIntent};

use helpers::*;
use order_manager::types::Owner;
mod helpers;

/// An initial position intent leads to an trade intent for the full size of the
/// position intent.
#[tokio::test]
async fn one_position_intent_leads_to_one_trade() -> Result<()> {
    let app = spawn_app().await;
    tracing::info!("App spawned, ready to roll!");
    app.send_position(&PositionIntent::builder("S1", "AAPL", Amount::Shares(Decimal::new(100, 0))).build()?)
        .await?;
    tracing::info!("POSITIONS SENT");
    let (claim, trade_intent) = app.receive_claim_and_risk_check_request().await?;
    assert_eq!(claim.amount, Amount::Shares(Decimal::ONE_HUNDRED));
    assert_eq!(trade_intent.qty, 100);
    let client_order_id = trade_intent.id;
    let response = RiskCheckResponse::Granted { intent: trade_intent };
    app.send_risk_check_response(&response).await?;
    let _trade_intent = app.receive_event().await?;
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

    app.send_order_message(&fill_message).await?;
    let (lot, allocation) = app.receive_lot_and_allocation().await?;
    assert_eq!(lot.shares, Decimal::ONE_HUNDRED);
    assert_eq!(allocation.shares, Decimal::ONE_HUNDRED);
    assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
    Ok(())
}

// /// An additional position intent leads to an trade intent with only the _net_ size
// /// difference.
// #[tokio::test]
// async fn new_position_leads_to_net_trade_size() -> Result<()> {
//     let app = spawn_app().await;
//     app.send_position(&PositionIntent::builder("S1", "AAPL", Amount::Shares(Decimal::new(100, 0))).build()?)
//         .await?;
//     let (_, trade_intent) = app.receive_claim_and_risk_check_request().await?;
//     let client_order_id = trade_intent.id;
//     let response = RiskCheckResponse::Granted { intent: trade_intent };
//     app.send_risk_check_response(&response).await?;
//     app.receive_event().await?;
//     let fill_message = OrderMessage {
//         client_order_id,
//         event_type: EventType::Fill,
//         ticker: "AAPL",
//         qty: 100,
//         position_qty: 100,
//         price: 100.0,
//         filled_qty: 100,
//         filled_avg_price: 100.0,
//         side: Side::Buy,
//         limit_price: None,
//     };
//     app.send_order_message(&fill_message).await?;
//     app.receive_lot_and_allocation().await?;
//
//     app.send_position(&PositionIntent::builder("S1", "AAPL", Amount::Shares(Decimal::new(150, 0))).build()?)
//         .await?;
//     let (claim, trade_intent) = app.receive_claim_and_risk_check_request().await?;
//     assert_eq!(claim.amount, Amount::Shares(Decimal::new(50, 0)));
//     assert_eq!(trade_intent.qty, 50);
//     let client_order_id = trade_intent.id;
//     let response = RiskCheckResponse::Granted { intent: trade_intent };
//     app.send_risk_check_response(&response).await?;
//     let _trade_intent = app.receive_event().await?;
//     let fill_message = OrderMessage {
//         client_order_id,
//         event_type: EventType::Fill,
//         ticker: "AAPL",
//         qty: 50,
//         position_qty: 150,
//         price: 100.0,
//         filled_qty: 50,
//         filled_avg_price: 100.0,
//         side: Side::Buy,
//         limit_price: None,
//     };
//     app.send_order_message(&fill_message).await?;
//     let (lot, allocation) = app.receive_lot_and_allocation().await?;
//     assert_eq!(lot.shares, Decimal::new(50, 0));
//     assert_eq!(allocation.shares, Decimal::new(50, 0));
//     assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
//     Ok(())
// }

///// A change in net side generates one initial trade and one additional trade
//async fn test_3(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
//    send_position(
//        &producer,
//        &PositionIntent::builder("S1", "AAPL", Amount::Shares(-Decimal::new(100, 0))).build()?,
//    )
//    .await?;
//    let (claim, trade_intent) = receive_claim_and_risk_check_request(&consumer).await?;
//    assert_eq!(claim.amount, Amount::Shares(-Decimal::new(250, 0)));
//    assert_eq!(trade_intent.qty, -150);
//    let client_order_id = trade_intent.id;
//    let response = RiskCheckResponse::Granted { intent: trade_intent };
//    send_risk_check_response(producer, &response).await?;
//    let _trade_intent = receive_event(&consumer).await?;
//    let fill_message = OrderMessage {
//        client_order_id,
//        event_type: EventType::Fill,
//        ticker: "AAPL",
//        qty: 150,
//        position_qty: 0,
//        price: 100.0,
//        filled_qty: 150,
//        filled_avg_price: 100.0,
//        side: Side::Sell,
//        limit_price: None,
//    };
//    send_order_message(&producer, &fill_message).await?;
//    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
//    assert_eq!(lot.shares, Decimal::new(-150, 0));
//    assert_eq!(allocation.shares, Decimal::new(-150, 0));
//    assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
//
//    let event = receive_event(&consumer).await?;
//    let trade_intent = match event {
//        Event::TradeIntent(ti) => ti,
//        _ => return Err(anyhow!("Unexpected event")),
//    };
//
//    assert_eq!(trade_intent.qty, -100);
//    let client_order_id = trade_intent.id;
//    let fill_message = OrderMessage {
//        client_order_id,
//        event_type: EventType::Fill,
//        ticker: "AAPL",
//        qty: 100,
//        position_qty: 100,
//        price: 100.0,
//        filled_qty: 100,
//        filled_avg_price: 100.0,
//        side: Side::Sell,
//        limit_price: None,
//    };
//    send_order_message(&producer, &fill_message).await?;
//    let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
//    assert_eq!(lot.shares, Decimal::new(-100, 0));
//    assert_eq!(allocation.shares, Decimal::new(-100, 0));
//    assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
//    Ok(())
//}

// /// A fractional position intent still leads to integer trades
// async fn test_4(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
//     send_position(
//         &producer,
//         &PositionIntent::builder("S1", "AAPL", Amount::Shares(Decimal::new(-1005, 1))).build()?,
//     )
//     .await?;
//
//     let (claim, trade_intent) = receive_claim_and_risk_check_request(&consumer).await?;
//     assert_eq!(claim.amount, Amount::Shares(-Decimal::new(5, 1)));
//     assert_eq!(trade_intent.qty, -1);
//     let client_order_id = trade_intent.id;
//     let response = RiskCheckResponse::Granted { intent: trade_intent };
//     send_risk_check_response(producer, &response).await?;
//     let _trade_intent = receive_event(&consumer).await?;
//     let fill_message = OrderMessage {
//         client_order_id,
//         event_type: EventType::Fill,
//         ticker: "AAPL",
//         qty: 1,
//         position_qty: -101,
//         price: 100.0,
//         filled_qty: 1,
//         filled_avg_price: 100.0,
//         side: Side::Sell,
//         limit_price: None,
//     };
//
//     send_order_message(&producer, &fill_message).await?;
//     let (lot, allocation, house_alloc) = receive_lot_allocation_and_house_allocation(&consumer).await?;
//     assert_eq!(lot.shares, Decimal::new(-1, 0));
//     assert_eq!(allocation.shares, Decimal::new(-5, 1));
//     assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
//     assert_eq!(house_alloc.shares, Decimal::new(-5, 1));
//     Ok(())
// }

// /// Can send Zero position size
// async fn test_5(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
//     send_position(&producer, &PositionIntent::builder("S1", "AAPL", Amount::Zero).build()?).await?;
//
//     let (claim, trade_intent) = receive_claim_and_risk_check_request(&consumer).await?;
//     assert_eq!(claim.amount, Amount::Shares(Decimal::new(1005, 1)));
//     assert_eq!(trade_intent.qty, 101);
//     let client_order_id = trade_intent.id;
//     let response = RiskCheckResponse::Granted { intent: trade_intent };
//     send_risk_check_response(producer, &response).await?;
//     let _trade_intent = receive_event(&consumer).await?;
//     let fill_message = OrderMessage {
//         client_order_id,
//         event_type: EventType::Fill,
//         ticker: "AAPL",
//         qty: 101,
//         position_qty: 0,
//         price: 100.0,
//         filled_qty: 101,
//         filled_avg_price: 100.0,
//         side: Side::Buy,
//         limit_price: None,
//     };
//
//     send_order_message(&producer, &fill_message).await?;
//     let (lot, allocation, house_alloc) = receive_lot_allocation_and_house_allocation(&consumer).await?;
//     assert_eq!(lot.shares, Decimal::new(101, 0));
//     assert_eq!(allocation.shares, Decimal::new(1005, 1));
//     assert_eq!(allocation.owner, Owner::Strategy("S1".into(), None));
//     assert_eq!(house_alloc.shares, Decimal::new(5, 1));
//     Ok(())
// }

// /// Can deal with multiple strategies, dollar amount and limit orders
// async fn test_6(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
//     send_position(
//         &producer,
//         &PositionIntent::builder("S2", "AAPL", Amount::Dollars(Decimal::new(10000, 0)))
//             .limit_price(Decimal::new(100, 0))
//             .build()?,
//     )
//     .await?;
//
//     let (claim, trade_intent) = receive_claim_and_risk_check_request(&consumer).await?;
//     assert_eq!(claim.amount, Amount::Dollars(Decimal::new(10000, 0)));
//     assert_eq!(trade_intent.qty, 100);
//     assert_eq!(
//         trade_intent.order_type,
//         OrderType::Limit {
//             limit_price: Decimal::new(100, 0)
//         }
//     );
//     let client_order_id = trade_intent.id;
//     let response = RiskCheckResponse::Granted { intent: trade_intent };
//     send_risk_check_response(producer, &response).await?;
//     let _trade_intent = receive_event(&consumer).await?;
//     let fill_message = OrderMessage {
//         client_order_id,
//         event_type: EventType::Fill,
//         ticker: "AAPL",
//         qty: 100,
//         position_qty: 100,
//         price: 100.0,
//         filled_qty: 100,
//         filled_avg_price: 100.0,
//         side: Side::Buy,
//         limit_price: Some(100.0),
//     };
//
//     send_order_message(&producer, &fill_message).await?;
//     let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
//     assert_eq!(lot.shares, Decimal::new(100, 0));
//     assert_eq!(allocation.shares, Decimal::new(100, 0));
//     assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
//     Ok(())
// }

// /// Can send `Retain` `Amount`s and not generate orders
// async fn test_7(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
//     send_position(
//         &producer,
//         &PositionIntent::builder("S2", "AAPL", Amount::Zero)
//             .update_policy(UpdatePolicy::RetainLong)
//             .build()?,
//     )
//     .await?;
//     assert!(consumer.recv().now_or_never().is_none());
//     send_position(
//         &producer,
//         &PositionIntent::builder("S2", "AAPL", Amount::Zero)
//             .update_policy(UpdatePolicy::Retain)
//             .build()?,
//     )
//     .await?;
//     assert!(consumer.recv().now_or_never().is_none());
//     send_position(
//         &producer,
//         &PositionIntent::builder("S2", "AAPL", Amount::Zero)
//             .update_policy(UpdatePolicy::RetainShort)
//             .build()?,
//     )
//     .await?;
//
//     let (claim, trade_intent) = receive_claim_and_risk_check_request(&consumer).await?;
//     assert_eq!(claim.amount, Amount::Shares(-Decimal::ONE_HUNDRED));
//     assert_eq!(trade_intent.qty, -100);
//     let client_order_id = trade_intent.id;
//     let response = RiskCheckResponse::Granted { intent: trade_intent };
//     send_risk_check_response(producer, &response).await?;
//     let _trade_intent = receive_event(&consumer).await?;
//     let fill_message = OrderMessage {
//         client_order_id,
//         event_type: EventType::Fill,
//         ticker: "AAPL",
//         qty: 100,
//         position_qty: 0,
//         price: 100.0,
//         filled_qty: 100,
//         filled_avg_price: 100.0,
//         side: Side::Sell,
//         limit_price: None,
//     };
//
//     send_order_message(&producer, &fill_message).await?;
//     let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
//     assert_eq!(lot.shares, Decimal::new(-100, 0));
//     assert_eq!(allocation.shares, Decimal::new(-100, 0));
//     assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
//     Ok(())
// }

// /// Can schedule intent
// async fn test_9(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
//     send_position(
//         &producer,
//         &PositionIntent::builder("S2", "AAPL", Amount::Shares(Decimal::new(100, 0)))
//             .after(Utc::now() + Duration::seconds(1))
//             .build()?,
//     )
//     .await?;
//
//     let (claim, trade_intent) = receive_claim_and_risk_check_request(&consumer).await?;
//     assert_eq!(claim.amount, Amount::Shares(Decimal::ONE_HUNDRED));
//     assert_eq!(trade_intent.qty, 100);
//     let client_order_id = trade_intent.id;
//     let response = RiskCheckResponse::Granted { intent: trade_intent };
//     send_risk_check_response(producer, &response).await?;
//     let _trade_intent = receive_event(&consumer).await?;
//     let fill_message = OrderMessage {
//         client_order_id,
//         event_type: EventType::Fill,
//         ticker: "AAPL",
//         qty: 100,
//         position_qty: 100,
//         price: 100.0,
//         filled_qty: 100,
//         filled_avg_price: 100.0,
//         side: Side::Buy,
//         limit_price: None,
//     };
//
//     send_order_message(&producer, &fill_message).await?;
//     let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
//     assert_eq!(lot.shares, Decimal::new(100, 0));
//     assert_eq!(allocation.shares, Decimal::new(100, 0));
//     assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
//     Ok(())
// }

// async fn test_10(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
//     send_position(
//         &producer,
//         &PositionIntent::builder("S2", Identifier::All, Amount::Zero).build()?,
//     )
//     .await?;
//
//     let (claim, trade_intent) = receive_claim_and_trade_intent(&consumer).await?;
//     assert_eq!(claim.amount, Amount::Shares(-Decimal::new(100, 0)));
//     assert_eq!(trade_intent.qty, -100);
//     let client_order_id = trade_intent.id;
//     let fill_message = OrderMessage {
//         client_order_id,
//         event_type: EventType::Fill,
//         ticker: "AAPL",
//         qty: 100,
//         position_qty: 0,
//         price: 100.0,
//         filled_qty: 100,
//         filled_avg_price: 100.0,
//         side: Side::Sell,
//         limit_price: None,
//     };
//
//     send_order_message(&producer, &fill_message).await?;
//     let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
//     assert_eq!(lot.shares, Decimal::new(-100, 0));
//     assert_eq!(allocation.shares, Decimal::new(-100, 0));
//     assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
//     Ok(())
// }

// async fn test_11(producer: &FutureProducer, consumer: &StreamConsumer) -> Result<()> {
//     send_position(
//         &producer,
//         &PositionIntent::builder("S2", "AAPL", Amount::Shares(Decimal::new(200, 0))).build()?,
//     )
//     .await?;
//
//     let (claim, trade_intent) = receive_claim_and_risk_check_request(&consumer).await?;
//     assert_eq!(claim.amount, Amount::Shares(Decimal::new(200, 0)));
//     let response = RiskCheckResponse::Granted { intent: trade_intent };
//     send_risk_check_response(producer, &response).await?;
//     let _trade_intent = receive_event(&consumer).await?;
//     info!("SLEEPING 1 SECOND TO LET UNREPORTED TRADE EXPIRE");
//     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
//     let record = FutureRecord::to("time")
//         .key("")
//         .payload(r#"{"state":"open","next_close":710}"#);
//     producer.send_result(record).map_err(|x| x.0)?.await?.map_err(|x| x.0)?;
//     //let event = receive_event(&consumer).await?;
//     //let trade_intent = match event {
//     //    Event::TradeIntent(ti) => ti,
//     //    _ => return Err(anyhow!("Unexpected event")),
//     //};
//     //let client_order_id = trade_intent.id;
//     //let fill_message = OrderMessage {
//     //    client_order_id,
//     //    event_type: EventType::Fill,
//     //    ticker: "AAPL",
//     //    qty: 100,
//     //    position_qty: 200,
//     //    price: 100.0,
//     //    filled_qty: 100,
//     //    filled_avg_price: 100.0,
//     //    side: Side::Buy,
//     //    limit_price: None,
//     //};
//
//     //send_order_message(&producer, &fill_message).await?;
//     //let (lot, allocation) = receive_lot_and_allocation(&consumer).await?;
//     //assert_eq!(lot.shares, Decimal::new(100, 0));
//     //assert_eq!(allocation.shares, Decimal::new(100, 0));
//     //assert_eq!(allocation.owner, Owner::Strategy("S2".into(), None));
//     Ok(())
// }
