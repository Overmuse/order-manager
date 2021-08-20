use anyhow::{anyhow, Result};
use rdkafka::producer::{FutureProducer, FutureRecord};

pub async fn send_order_event(producer: &FutureProducer, message: &str) -> Result<()> {
    let order_update = FutureRecord::to("overmuse-trades").key("ticker").payload(message);
    producer
        .send_result(order_update)
        .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?
        .await?
        .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?;
    Ok(())
}
