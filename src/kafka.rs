use crate::settings::Kafka;
use rdkafka::{consumer::StreamConsumer, producer::FutureProducer, ClientConfig};

pub fn consumer(settings: &Kafka) -> Result<StreamConsumer, Box<dyn std::error::Error>> {
    let mut config = ClientConfig::new();
    let config = settings.config(&mut config);
    let consumer = config
        .set("enable.ssl.certificate.verification", "false")
        .create()?;
    Ok(consumer)
}

pub fn producer(settings: &Kafka) -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let mut config = ClientConfig::new();
    let config = settings.config(&mut config);
    let producer: FutureProducer = config
        .set("enable.ssl.certificate.verification", "false")
        .create()?;
    Ok(producer)
}
