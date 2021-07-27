use anyhow::{anyhow, Result};
use tokio_postgres::GenericClient;
use tracing::trace;
use trading_base::{OrderType, TradeIntent};
use uuid::Uuid;

#[tracing::instrument(skip(client, id, dependent_trade))]
pub async fn save_dependent_trade<T: GenericClient>(
    client: &T,
    id: &Uuid,
    dependent_trade: &TradeIntent,
) -> Result<()> {
    trace!(%id, "Saving dependent trade");
    let (order_type, limit_price, stop_price) = match dependent_trade.order_type {
        OrderType::Market => ("market", None, None),
        OrderType::Limit { limit_price } => ("limit", Some(limit_price), None),
        OrderType::Stop { stop_price } => ("stop", None, Some(stop_price)),
        OrderType::StopLimit {
            limit_price,
            stop_price,
        } => ("stoplimit", Some(limit_price), Some(stop_price)),
    };
    client.execute(
                "INSERT INTO dependent_trades (dependent_id, id, ticker, qty, order_type, limit_price, stop_price, time_in_force) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &id,
                    &dependent_trade.id,
                    &dependent_trade.ticker,
                    &(dependent_trade.qty as i32),
                    &order_type,
                    &limit_price,
                    &stop_price,
                    &serde_plain::to_string(&dependent_trade.time_in_force)?,
                ],
            )
            .await?;
    Ok(())
}

#[tracing::instrument(skip(client, id))]
pub async fn take_dependent_trades<T: GenericClient>(
    client: &T,
    id: &Uuid,
) -> Result<Vec<TradeIntent>> {
    trace!(%id, "Saving dependent trade");
    client
        .query(
            "DELETE FROM dependent_trades WHERE dependent_id = $1 RETURNING *",
            &[&id],
        )
        .await?
        .iter()
        .map(|row| -> Result<TradeIntent> {
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
                        "Can only deal with market, limit, stop and stop-limit trades currently"
                    ))
                }
            };
            let time_in_force = serde_plain::from_str(row.try_get(7)?)?;
            Ok(TradeIntent {
                id: row.try_get(1)?,
                ticker: row.try_get(2)?,
                qty: row.try_get::<usize, i32>(3)? as isize,
                order_type,
                time_in_force,
            })
        })
        .collect()
}
