-- Add migration script here
ALTER TABLE claims ADD COLUMN limit_price NUMERIC;
