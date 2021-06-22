pub(super) struct PendingOrder {
    pub ticker: String,
    pub qty: isize,
    pub pending_qty: isize,
}

impl PendingOrder {
    pub fn new(ticker: String, qty: isize) -> Self {
        Self {
            ticker,
            qty,
            pending_qty: 0,
        }
    }
}
