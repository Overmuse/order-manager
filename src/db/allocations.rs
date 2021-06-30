use crate::manager::{Allocation, Owner, Position};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tracing::trace;

#[tracing::instrument(skip(client))]
pub(crate) async fn get_allocations(client: Arc<Mutex<Client>>) -> Result<Vec<Allocation>> {
    trace!("Getting allocations");
    client
        .lock()
        .await
        .query("SELECT * FROM allocations", &[])
        .await?
        .into_iter()
        .map(|row| -> Result<Allocation> {
            let owner = if row.try_get::<usize, &str>(0)? == "House" {
                Owner::House
            } else {
                Owner::Strategy(row.try_get(0)?, row.try_get(1)?)
            };
            Ok(Allocation::new(
                owner,
                row.try_get(2)?,
                row.try_get(3)?,
                row.try_get(4)?,
                row.try_get(5)?,
                row.try_get(6)?,
            ))
        })
        .collect()
}

#[tracing::instrument(skip(client, owner))]
pub(crate) async fn get_positions_by_owner(
    client: Arc<Mutex<Client>>,
    owner: Owner,
) -> Result<Vec<Position>> {
    let (owner, sub_owner) = match owner {
        Owner::House => ("house".to_string(), None),
        Owner::Strategy(owner, sub_owner) => (owner, sub_owner),
    };
    let res = match sub_owner {
            Some(sub_owner) => {
            client.lock().await
                .query("SELECT owner, sub_owner, ticker, sum(shares), sum(basis) FROM allocations WHERE owner = $1 AND sub_owner = $2 GROUP BY owner, sub_owner, ticker", &[&owner, &sub_owner]).await?
            }
            None => {
            client.lock().await
                .query("SELECT owner, null, ticker, sum(shares), sum(basis) FROM allocations WHERE owner = $1 GROUP BY owner, ticker", &[&owner]).await?
            }
        };

    res.into_iter()
        .map(|row| -> Result<Position> {
            let owner = if row.try_get::<usize, &str>(0)? == "House" {
                Owner::House
            } else {
                Owner::Strategy(row.try_get(0)?, row.try_get(1)?)
            };
            Ok(Position::new(
                owner,
                row.try_get(2)?,
                row.try_get(3)?,
                row.try_get(4)?,
            ))
        })
        .collect()
}

#[tracing::instrument(skip(client, ticker))]
pub(crate) async fn get_positions_by_ticker(
    client: Arc<Mutex<Client>>,
    ticker: &str,
) -> Result<Vec<Position>> {
    client.lock().await
            .query("SELECT owner, sub_owner, ticker, sum(shares), sum(basis) FROM allocations WHERE ticker = $1 GROUP BY owner, sub_owner, ticker", &[&ticker])
            .await?
            .into_iter()
            .map(|row| -> Result<Position> {
                let owner = if row.try_get::<usize, &str>(0)? == "House" {
                    Owner::House
                } else {
                    Owner::Strategy(row.try_get(0)?, row.try_get(1)?)
                };
                Ok(Position::new(
                    owner,
                    row.try_get(2)?,
                    row.try_get(3)?,
                    row.try_get(4)?,
                ))
            })
            .collect()
}

#[tracing::instrument(skip(client, allocation))]
pub(crate) async fn save_allocation(
    client: Arc<Mutex<Client>>,
    allocation: Allocation,
) -> Result<()> {
    trace!("Saving allocation");
    let (owner, sub_owner) = match allocation.owner {
        Owner::House => ("house".to_string(), None),
        Owner::Strategy(owner, sub_owner) => (owner, sub_owner),
    };
    client.lock().await.execute("INSERT INTO allocations (owner, sub_owner, claim_id, lot_id, ticker, shares, basis) VALUES ($1, $2, $3, $4, $5, $6, $7);", &[
            &owner,
            &sub_owner,
            &allocation.claim_id,
            &allocation.lot_id,
            &allocation.ticker,
            &allocation.shares,
            &allocation.basis
        ])
            .await?;
    Ok(())
}
