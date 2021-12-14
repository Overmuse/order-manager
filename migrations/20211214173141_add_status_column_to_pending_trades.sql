-- Add migration script here
ALTER TABLE pending_trades ADD COLUMN status TEXT NOT NULL;
