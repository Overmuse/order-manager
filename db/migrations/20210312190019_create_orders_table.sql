-- migrate:up
CREATE TABLE orders(
    id text NOT NULL,
    PRIMARY KEY (id),
    strategy_id uuid NOT NULL,
    ticker_id uuid NOT NULL,
    quantity integer NOT NULL,
    filled_quantity integer NOT NULL,
    filled_avg_price float
);

-- migrate:down
DROP TABLE orders;
