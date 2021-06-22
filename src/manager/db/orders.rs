use crate::OrderManager;
use alpaca::{Order, OrderType};
use anyhow::Result;
use rust_decimal::prelude::*;
use serde_plain;

impl OrderManager {
    pub(crate) async fn upsert_order(&self, order: &Order) -> Result<()> {
        let query = r#"
            INSERT INTO orders (
                client_order_id,
                ticker,
                status,
                quantity,
                filled_quantity,
                filled_average_price,
                order_type,
                limit_price,
                stop_price
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9
            )"#;
        let (order_type, limit_price, stop_price) = serialize_order_type(&order);
        sqlx::query(query)
            .bind(&order.client_order_id)
            .bind(&order.symbol)
            .bind(serde_plain::to_string(&order.status)?)
            .bind(order_type)
            .bind(limit_price)
            .bind(stop_price)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

fn serialize_order_type(order: &Order) -> (String, Option<Decimal>, Option<Decimal>) {
    match order.order_type {
        OrderType::Market => ("market".into(), None, None),
        OrderType::Limit { limit_price } => ("limit".into(), Some(limit_price), None),
        OrderType::Stop { stop_price } => ("stop".into(), None, Some(stop_price)),
        OrderType::StopLimit {
            limit_price,
            stop_price,
        } => ("stop_limit".into(), Some(limit_price), Some(stop_price)),
        _ => unimplemented!(),
    }
}
