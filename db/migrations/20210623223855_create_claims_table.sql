-- migrate:up
CREATE TABLE IF NOT EXISTS claims
(
    id           UUID PRIMARY KEY,
    strategy     TEXT NOT NULL,
    sub_strategy TEXT,
    ticker       TEXT NOT NULL,
    amount       NUMERIC NOT NULL,
    unit         TEXT NOT NULL
)

-- migrate:down
DROP TABLE IF EXISTS claims;
