-- Add migration script here
ALTER TABLE trades ADD COLUMN broker_id UUID;
