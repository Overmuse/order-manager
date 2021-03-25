use alpaca::common::{Order, OrderType};
use serde_plain::to_string;
use sqlx::postgres::{PgArguments, Postgres};
use sqlx::query::Query;

pub trait Db {
    fn save(self: &Self) -> Query<Postgres, PgArguments>;
}

impl Db for Order {
    fn save(&self) -> Query<Postgres, PgArguments> {
        let (order_type, limit_price, stop_price) = expand_order_type(&self.order_type);
        sqlx::query!(
            r#"INSERT INTO orders (
                id,
                client_order_id,
                created_at,
                filled_at,
                expired_at,
                canceled_at,
                failed_at,
                replaced_at,
                replaced_by,
                replaces,
                asset_id,
                symbol,
                qty,
                filled_qty,
                order_type,
                limit_price,
                stop_price,
                side,
                time_in_force,
                filled_avg_price,
                status,
                extended_hours
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                $14, $15, $16, $17, $18, $19, $20, $21, $22
            )
            ON CONFLICT (id)
            DO
                UPDATE SET 
                    filled_at = EXCLUDED.filled_at, 
                    expired_at = EXCLUDED.expired_at,
                    canceled_at = EXCLUDED.canceled_at,
                    failed_at = EXCLUDED.failed_at,
                    replaced_at = EXCLUDED.replaced_at,
                    replaced_by = EXCLUDED.replaced_by,
                    replaces = EXCLUDED.replaces,
                    filled_qty = EXCLUDED.filled_qty,
                    filled_avg_price = EXCLUDED.filled_avg_price,
                    status = EXCLUDED.status
            "#,
            self.id,
            self.client_order_id,
            self.created_at.naive_utc(),
            self.filled_at.map(|x| x.naive_utc()),
            self.expired_at.map(|x| x.naive_utc()),
            self.canceled_at.map(|x| x.naive_utc()),
            self.failed_at.map(|x| x.naive_utc()),
            self.replaced_at.map(|x| x.naive_utc()),
            self.replaced_by,
            self.replaces,
            self.asset_id,
            self.symbol,
            self.qty as i32,
            self.filled_qty as i32,
            order_type,
            limit_price,
            stop_price,
            to_string(&self.side).unwrap(),
            to_string(&self.time_in_force).unwrap(),
            self.filled_avg_price,
            to_string(&self.status).unwrap(),
            self.extended_hours
        )
    }
}

// TODO: (De)serialize automatically with serde somehow?
fn expand_order_type(ot: &OrderType) -> (&str, Option<f64>, Option<f64>) {
    match ot {
        OrderType::Market => ("market".into(), None, None),
        OrderType::Limit { limit_price } => ("limit".into(), Some(*limit_price), None),
        OrderType::Stop { stop_price } => ("stop".into(), None, Some(*stop_price)),
        OrderType::StopLimit {
            limit_price,
            stop_price,
        } => ("stop-limit".into(), Some(*limit_price), Some(*stop_price)),
        _ => panic!("Order type not implemented"),
    }
}
