-- Add migration script here
ALTER TABLE pending_trades ADD COLUMN claim_id UUID;
