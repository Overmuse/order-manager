CREATE TABLE IF NOT EXISTS lots
(
    id        UUID PRIMARY KEY,
    order_id  TEXT NOT NULL,
    ticker    TEXT NOT NULL,
    fill_time TIMESTAMP WITH TIME ZONE NOT NULL,
    price     NUMERIC NOT NULL,
    shares    NUMERIC NOT NULL
)
