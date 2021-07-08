use crate::types::{Allocation, Owner};
use std::convert::TryInto;
use std::sync::Arc;
use tokio_postgres::{Client, Error};
use tracing::trace;
use uuid::Uuid;

#[tracing::instrument(skip(client))]
pub async fn get_allocations(client: Arc<Client>) -> Result<Vec<Allocation>, Error> {
    trace!("Getting allocations");
    client
        .query("SELECT * FROM allocations", &[])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

pub async fn set_allocation_owner(
    client: Arc<Client>,
    id: Uuid,
    owner: Owner,
) -> Result<(), Error> {
    trace!("Updating allocation owner");
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

#[tracing::instrument(skip(client, allocation))]
pub async fn save_allocation(client: Arc<Client>, allocation: Allocation) -> Result<(), Error> {
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
