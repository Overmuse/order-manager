use lazy_static::lazy_static;
use prometheus::{IntCounter, Registry};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref NUM_TRADES: IntCounter =
        IntCounter::new("num_trades", "Number of trades").expect("Metric can be created");
}

pub fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(NUM_TRADES.clone()))
        .expect("collector can be registered");
}
