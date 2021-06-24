use crate::manager::{Lot, OrderManager};
use anyhow::Result;
use tracing::trace;

impl OrderManager {
    #[tracing::instrument(skip(self))]
    pub(crate) async fn get_lots(&self) -> Result<Vec<Lot>> {
        trace!("Getting claims");
        self.db_client
            .query("SELECT * FROM lots", &[])
            .await?
            .into_iter()
            .map(|row| -> Result<Lot> {
                Ok(Lot {
                    id: row.try_get(0)?,
                    order_id: row.try_get(1)?,
                    ticker: row.try_get(2)?,
                    fill_time: row.try_get(3)?,
                    price: row.try_get(4)?,
                    shares: row.try_get(5)?,
                })
            })
            .collect()
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn get_lots_by_order_id(&self, order_id: &str) -> Result<Vec<Lot>> {
        trace!("Getting claims");
        self.db_client
            .query("SELECT * FROM lots WHERE order_id = $1", &[&order_id])
            .await?
            .into_iter()
            .map(|row| -> Result<Lot> {
                Ok(Lot {
                    id: row.try_get(0)?,
                    order_id: row.try_get(1)?,
                    ticker: row.try_get(2)?,
                    fill_time: row.try_get(3)?,
                    price: row.try_get(4)?,
                    shares: row.try_get(5)?,
                })
            })
            .collect()
    }

    #[tracing::instrument(skip(self, lot))]
    pub(crate) async fn save_lot(&self, lot: Lot) -> Result<()> {
        trace!("Saving lot");
        self.db_client.execute("INSERT INTO lots (id, order_id, ticker, fill_time, price, shares) VALUES ($1, $2, $3, $4, $5, $6);", &[
            &lot.id,
            &lot.order_id,
            &lot.ticker,
            &lot.fill_time,
            &lot.price,
            &lot.shares,
        ])
            .await?;
        Ok(())
    }
}
