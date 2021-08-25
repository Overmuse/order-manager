use crate::types::{Owner, Position};
use std::convert::TryInto;
use tokio_postgres::{Error, GenericClient};
use tracing::trace;

#[tracing::instrument(skip(client, owner))]
pub async fn get_positions_by_owner<T: GenericClient>(client: &T, owner: &Owner) -> Result<Vec<Position>, Error> {
    trace!(%owner, "Fetching positions for owner");
    let (owner, sub_owner) = match owner {
        Owner::House => ("House", None),
        Owner::Strategy(owner, sub_owner) => (owner.as_str(), sub_owner.as_ref()),
    };
    let res = match sub_owner {
        Some(sub_owner) => {
            client.query("SELECT owner, sub_owner, ticker, sum(shares) AS shares, sum(basis) AS basis FROM allocations WHERE owner = $1 AND sub_owner = $2 GROUP BY owner, sub_owner, ticker", &[&owner, &sub_owner]).await?
        }
        None => {
            client.query("SELECT owner, sub_owner, ticker, sum(shares) AS shares, sum(basis) AS basis FROM allocations WHERE owner = $1 GROUP BY owner, sub_owner, ticker", &[&owner]).await?
        }
    };

    res.into_iter().map(TryInto::try_into).collect()
}

#[tracing::instrument(skip(client, ticker))]
pub async fn get_positions_by_ticker<T: GenericClient>(client: &T, ticker: &str) -> Result<Vec<Position>, Error> {
    trace!(ticker, "Fetching positions for ticker");
    client.query("SELECT owner, sub_owner, ticker, sum(shares) AS shares, sum(basis) AS basis FROM allocations WHERE ticker = $1 GROUP BY owner, sub_owner, ticker", &[&ticker])
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect()
}

#[tracing::instrument(skip(client, owner, ticker))]
pub async fn get_position_by_owner_and_ticker<T: GenericClient>(
    client: &T,
    owner: &Owner,
    ticker: &str,
) -> Result<Option<Position>, Error> {
    trace!(%owner, ticker, "Fetching positions for owner and ticker");
    let (owner, sub_owner) = match owner {
        Owner::House => ("House", None),
        Owner::Strategy(owner, sub_owner) => (owner.as_str(), sub_owner.as_ref()),
    };
    let res = match sub_owner {
        Some(sub_owner) => {
            client.query_opt("SELECT owner, sub_owner, ticker, sum(shares) AS shares, sum(basis) AS basis FROM allocations WHERE owner = $1 AND sub_owner = $2 AND ticker = $3 GROUP BY owner, sub_owner, ticker", &[&owner, &sub_owner, &ticker]).await?
        }
        None => {
            client.query_opt("SELECT owner, sub_owner, ticker, sum(shares) AS shares, sum(basis) AS basis FROM allocations WHERE owner = $1 AND ticker = $2 GROUP BY owner, sub_owner, ticker", &[&owner, &ticker]).await?
        }
    };

    res.map(TryInto::try_into).transpose()
}

#[tracing::instrument(skip(client))]
pub async fn get_positions<T: GenericClient>(client: &T) -> Result<Vec<Position>, Error> {
    trace!("Fetching all positions");
    client
        .query("SELECT * FROM allocations", &[])
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}
