-- Add migration script here
CREATE TABLE IF NOT EXISTS lots
(
    id        TEXT PRIMARY KEY,
    ticker    TEXT NOT NULL,
    fill_time TIMESTAMP NOT NULL,
    price     NUMERIC NOT NULL,
    shares    NUMERIC NOT NULL
)
