use chrono::Utc;
use order_manager::PositionIntent;
use serde_json;

pub fn position_payload(ticker: &str, qty: i32) -> Vec<u8> {
    let pi = PositionIntent {
        id: "A".into(),
        strategy: "Test".into(),
        timestamp: Utc::now(),
        ticker: ticker.to_string(),
        qty,
        condition: None,
    };
    serde_json::to_vec(&pi).unwrap()
}
