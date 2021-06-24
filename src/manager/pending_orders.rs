pub(crate) struct PendingOrder {
    pub id: String,
    pub ticker: String,
    pub qty: i32,
    pub pending_qty: i32,
}

impl PendingOrder {
    pub fn new(id: String, ticker: String, qty: i32) -> Self {
        Self {
            id,
            ticker,
            qty,
            pending_qty: qty,
        }
    }
}
