-- Add migration script here
CREATE TABLE IF NOT EXISTS pending_orders
(
    order_id    TEXT PRIMARY KEY,
    strategy_id INT  NOT NULL,
    ticker      TEXT,
    quantity    REAL
)
