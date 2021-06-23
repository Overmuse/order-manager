use super::super::OrderManager;
use crate::manager::PendingOrder;
use anyhow::Result;

impl OrderManager {
    pub(crate) async fn get_pending_orders(&self) -> Result<Vec<PendingOrder>> {
        self.db_client
            .query("SELECT * FROM pending_orders", &[])
            .await?
            .into_iter()
            .map(|row| -> Result<PendingOrder> {
                Ok(PendingOrder {
                    id: row.try_get(0)?,
                    ticker: row.try_get(1)?,
                    qty: row.try_get(2)?,
                    pending_qty: row.try_get(3)?,
                })
            })
            .collect()
    }

    pub(crate) async fn get_pending_order_by_id(&self, id: &str) -> Result<Option<PendingOrder>> {
        self.db_client
            .query_opt("SELECT * FROM pending_orders where id = $1", &[&id])
            .await?
            .map(|row| -> Result<PendingOrder> {
                Ok(PendingOrder {
                    id: row.try_get(0)?,
                    ticker: row.try_get(1)?,
                    qty: row.try_get(2)?,
                    pending_qty: row.try_get(3)?,
                })
            })
            .transpose()
    }

    pub(crate) async fn update_pending_order_qty(&self, id: &str, qty: i32) -> Result<()> {
        self.db_client
            .execute(
                "UPDATE pending_orders SET pending_qty = $1 WHERE id = $2",
                &[&qty, &id],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn save_pending_order(&self, pending_order: PendingOrder) -> Result<()> {
        self.db_client.execute("INSERT INTO pending_orders (id, ticker, quantity, pending_quantity) VALUES ($1, $2, $3, $4)", &[&pending_order.id, &pending_order.ticker, &pending_order.qty, &pending_order.pending_qty]).await?;
        Ok(())
    }

    pub(crate) async fn delete_pending_order_by_id(&self, id: &str) -> Result<()> {
        self.db_client
            .execute("DELETE FROM pending_orders WHERE id = $1", &[&id])
            .await?;
        Ok(())
    }
}
