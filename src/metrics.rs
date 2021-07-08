use lazy_static::lazy_static;
use prometheus::{CounterVec, IntCounterVec, Opts, Registry};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref NUM_TRADES: IntCounterVec =
        IntCounterVec::new(Opts::new("num_trades", "Number of trades"), &["ticker"])
            .expect("Metric can be created");
    pub static ref GROSS_TRADE_AMOUNT: CounterVec = CounterVec::new(
        Opts::new("gross_trade_amount", "Gross dollar amount traded"),
        &["ticker"]
    )
    .expect("Metric can be created");
}

pub fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(NUM_TRADES.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(GROSS_TRADE_AMOUNT.clone()))
        .expect("collector can be registered");
}
