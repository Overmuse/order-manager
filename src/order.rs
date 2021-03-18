use alpaca::common::Order;
use sqlx::postgres::{PgArguments, PgPool, Postgres};
use sqlx::query::Query;

pub trait Db {
    fn fill(self: &Self) -> Query<Postgres, PgArguments>;
    fn partial_fill(self: &Self) -> Query<Postgres, PgArguments>;
}

impl Db for Order {
    fn fill(&self) -> Query<Postgres, PgArguments> {
        sqlx::query!(
            r#"INSERT INTO orders (id) VALUES ($1)"#,
            self.client_order_id
        )
    }

    fn partial_fill(&self) -> Query<Postgres, PgArguments> {
        sqlx::query!(
            r#"INSERT INTO orders (id) VALUES ($1)"#,
            self.client_order_id
        )
    }
}
