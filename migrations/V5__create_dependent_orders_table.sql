CREATE TABLE IF NOT EXISTS dependent_orders 
(
    dependent_id    TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    qty             int     NOT NULL,
    side            TEXT    NOT NULL,
    order_type      TEXT    NOT NULL,
    limit_price     NUMERIC,
    stop_price      NUMERIC,
    time_in_force   TEXT    NOT NULL,
    extended_hours  boolean NOT NULL,
    client_order_id TEXT,
    order_class     TEXT    NOT NULL
)
