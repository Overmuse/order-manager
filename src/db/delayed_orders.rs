use alpaca::orders::OrderIntent;
use alpaca::OrderType;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio_postgres::Client;
use tracing::trace;

#[tracing::instrument(skip(client, id, delayed_order))]
pub async fn save_delayed_order(client: Arc<Client>, delayed_order: OrderIntent) -> Result<()> {
    trace!(id, "Saving delayed order");
    let (order_type, limit_price, stop_price) = match delayed_order.order_type {
        OrderType::Market => ("market", None, None),
        OrderType::Limit { limit_price } => ("limit", Some(limit_price), None),
        OrderType::Stop { stop_price } => ("stop", None, Some(stop_price)),
        OrderType::StopLimit {
            limit_price,
            stop_price,
        } => ("stoplimit", Some(limit_price), Some(stop_price)),
        _ => unimplemented!(),
    };
    client.execute(
                "INSERT INTO delayed_orders (symbol, qty, side, order_type, limit_price, stop_price, time_in_force, extended_hours, client_order_id, order_class) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                &[
                    &delayed_order.symbol,
                    &(delayed_order.qty as i32),
                    &serde_plain::to_string(&delayed_order.side)?,
                    &order_type,
                    &limit_price,
                    &stop_price,
                    &serde_plain::to_string(&delayed_order.time_in_force)?,
                    &delayed_order.extended_hours,
                    &delayed_order.client_order_id,
                    &serde_plain::to_string(&delayed_order.order_class)?,
                ],
            )
            .await?;
    Ok(())
}

#[tracing::instrument(skip(client, id))]
pub async fn take_delayed_orders(
    client: Arc<Client>,
    client_order_id: &str,
) -> Result<Vec<OrderIntent>> {
    trace!(id, "Saving delayed order");
    client
        .query(
            "DELETE FROM delayed_orders WHERE client_order_id = $1 RETURNING *",
            &[&client_order_id],
        )
        .await?
        .iter()
        .map(|row| -> Result<OrderIntent> {
            let order_type = match (row.try_get(3)?, row.try_get(4)?, row.try_get(5)?) {
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
            let side = serde_plain::from_str(row.try_get(2)?)?;
            let time_in_force = serde_plain::from_str(row.try_get(8)?)?;
            let order_class = serde_plain::from_str(row.try_get(9)?)?;
            Ok(OrderIntent {
                symbol: row.try_get(0)?,
                qty: row.try_get::<usize, i32>(1)? as usize,
                side,
                order_type,
                time_in_force,
                extended_hours: row.try_get(7)?,
                client_order_id: row.try_get(8)?,
                order_class,
            })
        })
        .collect()
}
