-- Add migration script here
CREATE TABLE IF NOT EXISTS pending_trades
(
    id               UUID PRIMARY KEY,
    ticker           TEXT NOT NULL,
    quantity         int  NOT NULL,
    pending_quantity int  NOT NULL
)
