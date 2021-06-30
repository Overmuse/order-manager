use serde::Serialize;

#[derive(Serialize)]
pub(crate) struct PendingOrder {
    pub id: String,
    pub ticker: String,
    pub qty: i32,
    pub pending_qty: i32,
}

impl PendingOrder {
    #[tracing::instrument(skip(id, ticker, qty))]
    pub fn new(id: String, ticker: String, qty: i32) -> Self {
        tracing::trace!(%id, %ticker, %qty, "New PendingOrder");
        Self {
            id,
            ticker,
            qty,
            pending_qty: qty,
        }
    }
}
