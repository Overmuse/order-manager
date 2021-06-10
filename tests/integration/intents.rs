use chrono::Utc;
use position_intents::{AmountSpec, PositionIntent};
use rust_decimal::Decimal;
use serde_json;
use uuid::Uuid;

pub fn position_payload(ticker: &str, qty: Decimal) -> Vec<u8> {
    let pi = PositionIntent {
        id: Uuid::new_v4().to_string(),
        strategy: "Test".into(),
        timestamp: Utc::now(),
        ticker: ticker.to_string(),
        amount: AmountSpec::Shares(qty),
        limit_price: None,
        decision_price: None,
        after: None,
        before: None,
    };
    serde_json::to_vec(&pi).unwrap()
}
