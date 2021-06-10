use position_intents::{AmountSpec, PositionIntent};
use serde_json;

pub fn position_payload(ticker: &str, amount: AmountSpec) -> Vec<u8> {
    let pi = PositionIntent::new("Test".to_string(), ticker.to_string(), amount);
    serde_json::to_vec(&pi).unwrap()
}
