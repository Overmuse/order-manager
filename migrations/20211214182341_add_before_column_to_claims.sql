-- Add migration script here
ALTER TABLE claims ADD COLUMN before TIMESTAMP WITH TIME ZONE;
