use super::super::OrderManager;
use alpaca::orders::OrderIntent;
use alpaca::OrderType;
use anyhow::{anyhow, Result};
use tracing::trace;

impl OrderManager {
    #[tracing::instrument(skip(self, id, dependent_order))]
    pub(crate) async fn save_dependent_order(
        &self,
        id: &str,
        dependent_order: OrderIntent,
    ) -> Result<()> {
        trace!(id, "Saving dependent order");
        let (order_type, limit_price, stop_price) = match dependent_order.order_type {
            OrderType::Market => ("market", None, None),
            OrderType::Limit { limit_price } => ("limit", Some(limit_price), None),
            OrderType::Stop { stop_price } => ("stop", None, Some(stop_price)),
            OrderType::StopLimit {
                limit_price,
                stop_price,
            } => ("stoplimit", Some(limit_price), Some(stop_price)),
            _ => unimplemented!(),
        };
        self.db_client
            .execute(
                "INSERT INTO dependent_orders (dependent_id, symbol, qty, side, order_type, limit_price, stop_price, time_in_force, extended_hours, client_order_id, order_class) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[
                    &id,
                    &dependent_order.symbol,
                    &(dependent_order.qty as i32),
                    &serde_plain::to_string(&dependent_order.side)?,
                    &order_type,
                    &limit_price,
                    &stop_price,
                    &serde_plain::to_string(&dependent_order.time_in_force)?,
                    &dependent_order.extended_hours,
                    &dependent_order.client_order_id,
                    &serde_plain::to_string(&dependent_order.order_class)?,
                ],
            )
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self, id))]
    pub(crate) async fn take_dependent_orders(&self, id: &str) -> Result<Vec<OrderIntent>> {
        trace!(id, "Saving dependent order");
        self.db_client
            .query(
                "DELETE FROM dependent_orders WHERE dependent_id = $1 RETURNING *",
                &[&id],
            )
            .await?
            .iter()
            .map(|row| -> Result<OrderIntent> {
                let order_type = match (row.try_get(4)?, row.try_get(5)?, row.try_get(6)?) {
                    ("market", _, _) => OrderType::Market,
                    ("limit", Some(limit_price), _) => OrderType::Limit { limit_price },
                    ("stop", None, Some(stop_price)) => OrderType::Stop { stop_price },
                    ("stoplimit", Some(limit_price), Some(stop_price)) => OrderType::StopLimit {
                        limit_price,
                        stop_price,
                    },
                    _ => {
                        return Err(anyhow!(
                        "Can only deal with market, limit, stop and stop-limit orders currently"
                    ))
                    }
                };
                let side = serde_plain::from_str(row.try_get(3)?)?;
                let time_in_force = serde_plain::from_str(row.try_get(7)?)?;
                let order_class = serde_plain::from_str(row.try_get(10)?)?;
                Ok(OrderIntent {
                    symbol: row.try_get(1)?,
                    qty: row.try_get::<usize, i32>(2)? as usize,
                    side,
                    order_type,
                    time_in_force,
                    extended_hours: row.try_get(8)?,
                    client_order_id: row.try_get(9)?,
                    order_class,
                })
            })
            .collect()
    }
}
