use anyhow::{anyhow, Result};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Fill,
}

impl EventType {
    fn format_for_order(&self) -> &'static str {
        match self {
            Self::Fill => "filled",
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

pub struct OrderMessage {
    pub client_order_id: Uuid,
    pub event_type: EventType,
    pub ticker: &'static str,
    pub qty: usize,
    pub position_qty: isize,
    pub price: f64,
    pub filled_qty: usize,
    pub filled_avg_price: f64,
    pub side: Side,
    pub limit_price: Option<f64>,
}

impl OrderMessage {
    pub fn format(&self) -> String {
        let limit_price = match self.limit_price {
            Some(limit_price) => format!("{}", limit_price),
            None => "null".to_string(),
        };
        let order_type = match self.limit_price {
            Some(_) => "limit",
            None => "market",
        };

        let msg = format!(
            r#"{{"stream":"trade_updates","data":{{"event":"{}","position_qty":"{}","price":"{}","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:38:01.937734Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"{}","asset_class":"us_equity","notional":null,"qty":"{}","filled_qty":"{}","filled_avg_price":"{}","order_class":"simple","order_type":"{}","type":"{}","side":"{}","time_in_force":"day","limit_price":{},"stop_price":null,"status":"{}","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
            serde_plain::to_string(&self.event_type).unwrap(),
            self.position_qty,
            self.price,
            self.client_order_id,
            self.ticker,
            self.qty,
            self.filled_qty,
            self.filled_avg_price,
            order_type,
            order_type,
            serde_plain::to_string(&self.side).unwrap(),
            serde_plain::to_string(&limit_price).unwrap(),
            self.event_type.format_for_order()
        );
        tracing::info!("{}", msg);
        msg
    }
}

pub async fn send_order_message(producer: &FutureProducer, message: &OrderMessage) -> Result<()> {
    let payload = message.format();
    let order_update = FutureRecord::to("overmuse-trades").key("ticker").payload(&payload);
    producer
        .send_result(order_update)
        .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?
        .await?
        .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?;
    Ok(())
}
