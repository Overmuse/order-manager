use super::super::OrderManager;
use anyhow::Result;

impl OrderManager {
    pub(crate) async fn delete_pending_order_by_id(&self, id: String) -> Result<()> {
        sqlx::query("DELETE FROM pending_orders WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
