-- migrate:up
CREATE TABLE positions(
    id uuid NOT NULL,
    PRIMARY KEY (id),
    strategy_id uuid NOT NULL,
    timestamp timestamp NOT NULL,
    ticker_id uuid NOT NULL,
    quantity integer NOT NULL
);

-- migrate:down
DROP TABLE positions;
