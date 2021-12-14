-- Add migration script here
CREATE TABLE IF NOT EXISTS dependent_trades
(
    dependent_id    UUID    NOT NULL,
    id              UUID    NOT NULL,
    ticker          TEXT    NOT NULL,
    qty             int     NOT NULL,
    order_type      TEXT    NOT NULL,
    limit_price     NUMERIC,
    stop_price      NUMERIC,
    time_in_force   TEXT    NOT NULL
)
