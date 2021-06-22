-- Add migration script here
CREATE TABLE IF NOT EXISTS claims
(
    id           TEXT NOT NULL,
    strategy     TEXT NOT NULL,
    sub_strategy TEXT,
    ticker       TEXT NOT NULL,
    amount       NUMERIC NOT NULL,
    unit         TEXT NOT NULL
)
