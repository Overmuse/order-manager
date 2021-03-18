#![feature(type_alias_impl_trait)]
//use futures::channel::mpsc::unbounded;
//use futures::prelude::*;
use rdkafka::message::{BorrowedMessage, Message};
//use rdkafka::producer::FutureRecord;
use serde::{Deserialize, Serialize};
pub use settings::Settings;
//use std::sync::mpsc::channel;
//use std::time::Duration;

pub mod db;
pub mod kafka;
pub mod manager;
pub mod order;
pub mod policy;
pub mod settings;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Position {
    pub strategy: String,
    pub ticker: String,
    pub qty: i32,
}

pub fn handle_message(
    msg: Result<BorrowedMessage, rdkafka::error::KafkaError>,
) -> Result<String, Box<dyn std::error::Error>> {
    let owned = msg?.detach();
    let payload = owned.payload().unwrap();
    let msg: String = String::from_utf8(payload.into())?;
    Ok(msg)
}

// pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
//     let settings = Settings::new()?;
//     let consumer = kafka::consumer(&settings.kafka)?;
//     let producer = kafka::producer(&settings.kafka)?;
//     let (mut tx, rx) = unbounded::<String>();
//     let mut msg_stream = consumer.stream().map(handle_message).map(|x| x.unwrap());
//     tokio::spawn({
//         let stream = rx.map(|msg| async { FutureRecord::to("TEST").key(&msg).payload(&msg) });
//         producer.send(stream);
//     });
//     while let Some(msg) = msg_stream.next().await {
//         tx.send(msg).await.unwrap();
//     }
//     Ok(())
// }
