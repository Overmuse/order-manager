CREATE TYPE status AS ENUM ('unreported', 'accepted', 'partially_filled', 'filled', 'cancelled', 'dead');
ALTER TABLE trades DROP COLUMN status;
ALTER TABLE trades ADD COLUMN status status;
