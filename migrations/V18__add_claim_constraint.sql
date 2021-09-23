CREATE UNIQUE INDEX strategy_substrategy_claims_idx ON claims (strategy, COALESCE(sub_strategy, ' '), ticker);
