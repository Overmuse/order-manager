-- Add migration script here
CREATE TABLE IF NOT EXISTS allocations
(
    owner     TEXT NOT NULL,
    sub_owner TEXT,
    claim_id  UUID,
    lot_id    UUID NOT NULL,
    ticker    TEXT NOT NULL,
    shares    NUMERIC NOT NULL,
    basis     NUMERIC NOT NULL
)
