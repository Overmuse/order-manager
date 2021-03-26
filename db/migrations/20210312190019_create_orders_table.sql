-- migrate:up
CREATE TABLE orders(
    id uuid NOT NULL,
    PRIMARY KEY (id),
    client_order_id text NULL,
    created_at timestamp NOT NULL,
    filled_at timestamp NULL,
    expired_at timestamp NULL,
    canceled_at timestamp NULL,
    failed_at timestamp NULL,
    replaced_at timestamp NULL,
    replaced_by uuid NULL,
    replaces uuid NULL,
    asset_id uuid NULL,
    symbol text NOT NULL,
    qty int NOT NULL CHECK (qty >= 0),
    filled_qty int NOT NULL,
    order_type text NOT NULL,
    limit_price float NULL,
    stop_price float NULL,
    side text NOT NULL,
    time_in_force text NOT NULL,
    filled_avg_price float NULL,
    status text NOT NULL,
    extended_hours boolean NOT NULL

);

-- migrate:down
DROP TABLE orders;
