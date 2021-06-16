pub(super) struct PendingOrder {
    pub id: String,
    pub ticker: String,
    pub qty: isize,
    pub pending_qty: isize,
}

impl PendingOrder {
    pub fn new(id: String, ticker: String, qty: isize) -> Self {
        Self {
            id,
            ticker,
            qty,
            pending_qty: 0,
        }
    }
}
