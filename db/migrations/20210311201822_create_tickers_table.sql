-- migrate:up
CREATE TABLE tickers(
    id uuid NOT NULL,
    PRIMARY KEY (id),
    symbol text NOT NULL
);


-- migrate:down
DROP TABLE tickers;
