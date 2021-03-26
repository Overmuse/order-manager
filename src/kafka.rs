use crate::settings::Kafka;
use anyhow::Result;
use rdkafka::{consumer::StreamConsumer, producer::FutureProducer, ClientConfig};

pub fn consumer(settings: &Kafka) -> Result<StreamConsumer> {
    let mut config = ClientConfig::new();
    let config = settings.config(&mut config);
    let consumer = config
        .set("group.id", &settings.group_id)
        .set("enable.ssl.certificate.verification", "false")
        .create()?;
    Ok(consumer)
}

pub fn producer(settings: &Kafka) -> Result<FutureProducer> {
    let mut config = ClientConfig::new();
    let config = settings.config(&mut config);
    let producer: FutureProducer = config
        .set("enable.ssl.certificate.verification", "false")
        .create()?;
    Ok(producer)
}
