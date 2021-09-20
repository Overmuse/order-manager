use super::OrderManager;
use anyhow::Result;
use risk_manager::RiskCheckResponse;
use tracing::warn;

impl OrderManager {
    pub async fn handle_risk_check_response(&self, response: RiskCheckResponse) -> Result<()> {
        match response {
            RiskCheckResponse::Granted { intent } => self.send_trade(intent, None).await,
            RiskCheckResponse::Denied { intent, .. } => {
                warn!(?intent, "RiskCheck Denied");
                Ok(())
            }
        }
    }
}
