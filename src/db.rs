use crate::PositionIntent;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgQueryResult};

pub async fn get_all_positions(pool: &PgPool) -> Result<Vec<PositionIntent>, sqlx::Error> {
    sqlx::query_as!(
        PositionIntent,
        r#"SELECT s.name as "strategy!: String", p.timestamp as "timestamp!: DateTime<Utc>", t.symbol as "ticker!: String", p.quantity as "qty!: i32" 
        FROM positions p
        INNER JOIN strategies s ON p.strategy_id = s.id
        INNER JOIN tickers t ON p.ticker_id = t.id"#
    )
    .fetch_all(pool)
    .await
}

pub async fn post_position(
    pool: &PgPool,
    position: PositionIntent,
) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::query!(
        r#"INSERT INTO positions (id, strategy_id, timestamp, ticker_id, quantity)
        VALUES (gen_random_uuid(), (SELECT id FROM strategies WHERE name = $1), $2, (SELECT id FROM tickers WHERE symbol = $3), $4)
        "#,
        position.strategy,
        position.timestamp.naive_utc(),
        position.ticker,
        position.qty
    ).execute(pool)
        .await
}

pub async fn get_positions_by_strategy(
    pool: &PgPool,
    strategy: &str,
) -> Result<Vec<PositionIntent>, sqlx::Error> {
    sqlx::query_as!(
        PositionIntent,
        r#"SELECT s.name as strategy, p.timestamp as "timestamp!: DateTime<Utc>", t.symbol as "ticker!", p.quantity as "qty!: i32" 
        FROM positions p
        INNER JOIN tickers t ON p.ticker_id = t.id
        INNER JOIN strategies s ON p.strategy_id = s.id
        WHERE s.name = $1"#,
        strategy
    )
    .fetch_all(pool)
    .await
}

pub async fn get_ticker_position_by_strategy(
    pool: &PgPool,
    ticker: &str,
    strategy: &str,
) -> Result<Option<PositionIntent>, sqlx::Error> {
    sqlx::query_as!(
        PositionIntent,
        r#"SELECT s.name as strategy, p.timestamp as "timestamp!: DateTime<Utc>", t.symbol as "ticker!", p.quantity as "qty!: i32" 
        FROM positions p
        INNER JOIN tickers t ON p.ticker_id = t.id
        INNER JOIN strategies s ON p.strategy_id = s.id
        WHERE s.name = $1
        AND t.symbol = $2"#,
        strategy,
        ticker
    )
    .fetch_optional(pool)
    .await
}
