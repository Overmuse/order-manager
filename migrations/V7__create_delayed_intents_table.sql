CREATE TABLE IF NOT EXISTS scheduled_intents
(
    id             UUID PRIMARY KEY,
    strategy       TEXT      NOT NULL,
    sub_strategy   TEXT,
    time_stamp     TIMESTAMP WITH TIME ZONE NOT NULL,
    ticker         TEXT      NOT NULL,
    amount         NUMERIC   NOT NULL,
    unit           TEXT      NOT NULL,
    update_policy  TEXT      NOT NULL,
    decision_price NUMERIC,
    limit_price    NUMERIC,
    stop_price     NUMERIC,
    before         TIMESTAMP WITH TIME ZONE,
    after          TIMESTAMP WITH TIME ZONE
)
