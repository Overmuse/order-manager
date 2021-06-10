use position_intents::{AmountSpec, PositionIntent};
use rust_decimal::Decimal;
use serde_json;

pub fn position_payload(
    strategy: &str,
    ticker: &str,
    amount: AmountSpec,
    limit_price: Option<Decimal>,
) -> Vec<u8> {
    let mut pi = PositionIntent::new(strategy.to_string(), ticker.to_string(), amount);
    if let Some(limit) = limit_price {
        pi = pi.limit_price(limit)
    }
    serde_json::to_vec(&pi).unwrap()
}
