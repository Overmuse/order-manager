use crate::manager::{Allocation, OrderManager, Owner};
use anyhow::Result;
use tracing::trace;

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub(crate) async fn get_allocations(&self) -> Result<Vec<Allocation>> {
        trace!("Getting allocations");
        let rows = self
            .db_client
            .query("SELECT * FROM allocations", &[])
            .await?
            .into_iter()
            .map(|row| {
                let owner = if row.get::<usize, &str>(0) == "House" {
                    Owner::House
                } else {
                    Owner::Strategy(row.get(0), row.get(1))
                };
                Allocation::new(
                    owner,
                    row.get(2),
                    row.get(3),
                    row.get(4),
                    row.get(5),
                    row.get(6),
                )
            })
            .collect();
        Ok(rows)
    }

    // #[tracing::instrument(skip(self))]
    // pub(crate) async fn delete_claim_by_id(&self, id: Uuid) -> Result<()> {
    //     trace!("Deleting claim");
    //     self.db_client
    //         .execute("DELETE FROM claims WHERE id = $1;", &[&id])
    //         .await?;
    //     Ok(())
    // }

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
