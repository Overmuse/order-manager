use crate::types::{Owner, Position};
use std::convert::TryInto;
use std::sync::Arc;
use tokio_postgres::{Client, Error};
use tracing::trace;

#[tracing::instrument(skip(client, owner))]
pub async fn get_positions_by_owner(
    client: Arc<Client>,
    owner: Owner,
) -> Result<Vec<Position>, Error> {
    trace!("Getting positions");
    let (owner, sub_owner) = match owner {
        Owner::House => ("House".to_string(), None),
        Owner::Strategy(owner, sub_owner) => (owner, sub_owner),
    };
    let res = match sub_owner {
        Some(sub_owner) => {
            client.query("SELECT owner, sub_owner, ticker, sum(shares) AS shares, sum(basis) AS basis FROM allocations WHERE owner = $1 AND sub_owner = $2 GROUP BY owner, sub_owner, ticker", &[&owner, &sub_owner]).await?
        }
        None => {
            client.query("SELECT owner, null AS sub_owner, ticker, sum(shares) AS shares, sum(basis) AS basis FROM allocations WHERE owner = $1 GROUP BY owner, ticker", &[&owner]).await?
        }
    };

    res.into_iter().map(TryInto::try_into).collect()
}

#[tracing::instrument(skip(client, ticker))]
pub async fn get_positions_by_ticker(
    client: Arc<Client>,
    ticker: &str,
) -> Result<Vec<Position>, Error> {
    trace!("Getting positions");
    client.query("SELECT owner, sub_owner, ticker, sum(shares) AS shares, sum(basis) AS basis FROM allocations WHERE ticker = $1 GROUP BY owner, sub_owner, ticker", &[&ticker])
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect()
}

#[tracing::instrument(skip(client))]
pub async fn get_positions(client: Arc<Client>) -> Result<Vec<Position>, Error> {
    trace!("Getting positions");
    client
        .query("SELECT * FROM allocations", &[])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}
