-- Add migration script here
CREATE VIEW positions AS SELECT owner, sub_owner, ticker, sum(shares) as shares, sum(basis) as basis FROM allocations GROUP BY owner, sub_owner, ticker HAVING sum(shares) != 0;
