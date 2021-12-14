-- Add migration script here
ALTER TABLE pending_trades ADD COLUMN datetime TIMESTAMP WITH TIME ZONE NOT NULL;
