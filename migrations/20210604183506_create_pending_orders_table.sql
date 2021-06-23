-- Add migration script here
CREATE TABLE IF NOT EXISTS pending_orders
(
    id               UUID PRIMARY KEY,
    ticker           TEXT NOT NULL,
    quantity         REAL NOT NULL,
    pending_quantity REAL NOT NULL
)
