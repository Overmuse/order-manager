use crate::manager::{Allocation, OrderManager, Owner};
use anyhow::Result;
use tracing::trace;

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub(crate) async fn get_allocations(&self) -> Result<Vec<Allocation>> {
        trace!("Getting allocations");
        self.db_client
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

    #[tracing::instrument(skip(self, allocation))]
    pub(crate) async fn save_allocation(&self, allocation: Allocation) -> Result<()> {
        trace!("Saving allocation");
        let (owner, sub_owner) = match allocation.owner {
            Owner::House => ("house".to_string(), None),
            Owner::Strategy(owner, sub_owner) => (owner, sub_owner),
        };
        self.db_client.execute("INSERT INTO allocations (owner, sub_owner, claim_id, lot_id, ticker, shares, basis) VALUES ($1, $2, $3, $4, $5, $6, $7);", &[
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
}