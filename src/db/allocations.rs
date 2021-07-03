use crate::manager::{Allocation, Owner, Position};
use anyhow::Result;
use std::sync::Arc;
use tokio_postgres::Client;
use tracing::trace;
use uuid::Uuid;

#[tracing::instrument(skip(client))]
pub(crate) async fn get_allocations(client: Arc<Client>) -> Result<Vec<Allocation>> {
    trace!("Getting allocations");
    client
        .query("SELECT * FROM allocations", &[])
        .await?
        .into_iter()
        .map(|row| -> Result<Allocation> {
            let owner = if row.try_get::<usize, &str>(0)? == "House" {
                Owner::House
            } else {
                Owner::Strategy(row.try_get(0)?, row.try_get(1)?)
            };
            Ok(Allocation {
                id: row.try_get(7)?,
                owner,
                claim_id: row.try_get(2)?,
                lot_id: row.try_get(3)?,
                ticker: row.try_get(4)?,
                shares: row.try_get(5)?,
                basis: row.try_get(6)?,
            })
        })
        .collect()
}

#[tracing::instrument(skip(client, owner))]
pub(crate) async fn get_positions_by_owner(
    client: Arc<Client>,
    owner: Owner,
) -> Result<Vec<Position>> {
    let (owner, sub_owner) = match owner {
        Owner::House => ("House".to_string(), None),
        Owner::Strategy(owner, sub_owner) => (owner, sub_owner),
    };
    let res = match sub_owner {
        Some(sub_owner) => {
            client.query("SELECT owner, sub_owner, ticker, sum(shares), sum(basis) FROM allocations WHERE owner = $1 AND sub_owner = $2 GROUP BY owner, sub_owner, ticker", &[&owner, &sub_owner]).await?
        }
        None => {
            client.query("SELECT owner, null, ticker, sum(shares), sum(basis) FROM allocations WHERE owner = $1 GROUP BY owner, ticker", &[&owner]).await?
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

pub(crate) async fn set_allocation_owner(
    client: Arc<Client>,
    id: Uuid,
    owner: Owner,
) -> Result<()> {
    let (owner, sub_owner) = match owner {
        Owner::House => ("House".to_string(), None),
        Owner::Strategy(owner, sub_owner) => (owner, sub_owner),
    };
    match sub_owner {
        Some(sub_owner) => {
            client
                .query(
                    "UPDATE allocations SET owner = $1, sub_owner = $2 WHERE id = $3",
                    &[&owner, &sub_owner, &id],
                )
                .await?
        }
        None => {
            client
                .query(
                    "UPDATE allocations SET owner = $1 WHERE id = $2",
                    &[&owner, &id],
                )
                .await?
        }
    };
    Ok(())
}

#[tracing::instrument(skip(client, ticker))]
pub(crate) async fn get_positions_by_ticker(
    client: Arc<Client>,
    ticker: &str,
) -> Result<Vec<Position>> {
    client.query("SELECT owner, sub_owner, ticker, sum(shares), sum(basis) FROM allocations WHERE ticker = $1 GROUP BY owner, sub_owner, ticker", &[&ticker])
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
pub(crate) async fn save_allocation(client: Arc<Client>, allocation: Allocation) -> Result<()> {
    trace!("Saving allocation");
    let (owner, sub_owner) = match allocation.owner {
        Owner::House => ("House".to_string(), None),
        Owner::Strategy(owner, sub_owner) => (owner, sub_owner),
    };
    client.execute("INSERT INTO allocations (id, owner, sub_owner, claim_id, lot_id, ticker, shares, basis) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);", &[
            &allocation.id,
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
