-- Add migration script here
CREATE TABLE IF NOT EXISTS strategies
(
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
)
