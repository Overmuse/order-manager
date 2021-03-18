-- migrate:up
CREATE TABLE strategies(
    id uuid NOT NULL,
    PRIMARY KEY (id),
    name text NOT NULL
);


-- migrate:down
DROP table strategies;
