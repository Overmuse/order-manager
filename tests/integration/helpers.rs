use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use order_manager::settings::{DatabaseSettings, Settings};
use order_manager::types::{Allocation, Claim, Lot, Owner};
use order_manager::{run, Event};
use rdkafka::Message;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use risk_manager::RiskCheckResponse;
use serde::Serialize;
use tokio_postgres::{connect, Client, NoTls};
use tracing::subscriber::set_global_default;
use tracing::{debug, error};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use trading_base::{Identifier, PositionIntent, TradeIntent};
use uuid::Uuid;

static TRACING: Lazy<()> = Lazy::new(|| {
    if std::env::var("RUST_LOG").is_ok() {
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            .finish();
        set_global_default(subscriber).unwrap();
    }
});

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
    _Sell,
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
            r#"{{"stream":"trade_updates","data":{{"event":"{}","qty":"{}","position_qty":"{}","price":"{}","timestamp":"2021-03-16T18:39:00Z","order":{{"id":"61e69015-8549-4bfd-b9c3-01e75843f47d","client_order_id":"{}","created_at":"2021-03-16T18:38:01.942282Z","updated_at":"2021-03-16T18:38:01.942282Z","submitted_at":"2021-03-16T18:38:01.937734Z","filled_at":"2021-03-16T18:38:01.937734Z","expired_at":null,"canceled_at":null,"failed_at":null,"replaced_at":null,"replaced_by":null,"replaces":null,"asset_id":"b0b6dd9d-8b9b-48a9-ba46-b9d54906e415","symbol":"{}","asset_class":"us_equity","notional":null,"qty":"{}","filled_qty":"{}","filled_avg_price":"{}","order_class":"simple","order_type":"{}","type":"{}","side":"{}","time_in_force":"day","limit_price":{},"stop_price":null,"status":"{}","extended_hours":false,"legs":null,"trail_percent":null,"trail_price":null,"hwm":null}}}}}}"#,
            serde_plain::to_string(&self.event_type).unwrap(),
            self.qty,
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
        msg
    }
}

fn kafka_topic(topic: &str, test_id: &str) -> String {
    format!("{}-{}", topic, test_id)
}

pub struct TestApp {
    _db: Client,
    test_id: String,
    consumer: StreamConsumer,
    producer: FutureProducer,
}

impl TestApp {
    pub async fn send_order_message(&self, message: &OrderMessage) -> Result<()> {
        let payload = message.format();
        let topic = kafka_topic("overmuse-trades", &self.test_id);
        let order_update = FutureRecord::to(&topic).key("ticker").payload(&payload);
        self.producer
            .send_result(order_update)
            .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?
            .await?
            .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?;
        Ok(())
    }

    pub async fn send_risk_check_response(&self, response: &RiskCheckResponse) -> Result<()> {
        let payload = serde_json::to_string(response)?;
        let topic = kafka_topic("risk-check-response", &self.test_id);
        let record = FutureRecord::to(&topic).key("").payload(&payload);
        self.producer
            .send(record, std::time::Duration::from_secs(0))
            .await
            .map_err(|(e, _)| e)?;
        Ok(())
    }

    pub async fn send_position(&self, intent: &PositionIntent) -> Result<()> {
        let payload = serde_json::to_vec(intent)?;
        let key = match &intent.identifier {
            Identifier::Ticker(ticker) => ticker,
            Identifier::All => "",
        }
        .to_string();
        let topic = kafka_topic("position-intents", &self.test_id);
        let message = FutureRecord::to(&topic).key(&key).payload(&payload);
        self.producer
            .send_result(message)
            .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?
            .await?
            .map_err(|(e, m)| anyhow!("{:?}\n{:?}", e, m))?;
        Ok(())
    }

    pub async fn receive_event(&self) -> Result<Event> {
        let msg = self.consumer.recv().await?;
        let topic = msg.topic();
        let payload = msg.payload().ok_or(anyhow!("Missing payload"))?;
        let event = if topic.starts_with("allocations") {
            let allocation: Allocation = serde_json::from_slice(payload)?;
            Event::Allocation(allocation)
        } else if topic.starts_with("claims") {
            let claim: Claim = serde_json::from_slice(payload)?;
            Event::Claim(claim)
        } else if topic.starts_with("lots") {
            let lot: Lot = serde_json::from_slice(payload)?;
            Event::Lot(lot)
        } else if topic.starts_with("risk-check-request") {
            let intent: TradeIntent = serde_json::from_slice(payload)?;
            Event::RiskCheckRequest(intent)
        } else if topic.starts_with("trade-intents") {
            let intent: TradeIntent = serde_json::from_slice(payload)?;
            Event::TradeIntent(intent)
        } else {
            unreachable!()
        };
        Ok(event)
    }

    pub async fn _receive_claim_and_trade_intent(&self) -> Result<(Claim, TradeIntent)> {
        let events = tokio::try_join!(self.receive_event(), self.receive_event())?;
        match events {
            (Event::Claim(c), Event::TradeIntent(ti)) => Ok((c, ti)),
            (Event::TradeIntent(ti), Event::Claim(c)) => Ok((c, ti)),
            _ => {
                error!("UH OH");
                return Err(anyhow!("Unexpected events"));
            }
        }
    }

    pub async fn receive_claim_and_risk_check_request(&self) -> Result<(Claim, TradeIntent)> {
        let events = tokio::try_join!(self.receive_event(), self.receive_event())?;
        debug!("{:?}", events);
        match events {
            (Event::Claim(c), Event::RiskCheckRequest(ti)) => Ok((c, ti)),
            (Event::RiskCheckRequest(ti), Event::Claim(c)) => Ok((c, ti)),
            _ => {
                debug!("{:?}", events);
                return Err(anyhow!("Unexpected events"));
            }
        }
    }

    pub async fn receive_lot_and_allocation(&self) -> Result<(Lot, Allocation)> {
        let events = tokio::try_join!(self.receive_event(), self.receive_event())?;
        match events {
            (Event::Allocation(a), Event::Lot(l)) => Ok((l, a)),
            (Event::Lot(l), Event::Allocation(a)) => Ok((l, a)),
            _ => Err(anyhow!("Unexpected events")),
        }
    }

    pub async fn _receive_lot_allocation_and_house_allocation(&self) -> Result<(Lot, Allocation, Allocation)> {
        let events = tokio::try_join!(self.receive_event(), self.receive_event(), self.receive_event())?;
        match events {
            (Event::Allocation(a1), Event::Allocation(a2), Event::Lot(l)) => {
                if a1.owner == Owner::House {
                    Ok((l, a2, a1))
                } else {
                    Ok((l, a1, a2))
                }
            }
            (Event::Allocation(a1), Event::Lot(l), Event::Allocation(a2)) => {
                if a1.owner == Owner::House {
                    Ok((l, a2, a1))
                } else {
                    Ok((l, a1, a2))
                }
            }
            (Event::Lot(l), Event::Allocation(a1), Event::Allocation(a2)) => {
                if a1.owner == Owner::House {
                    Ok((l, a2, a1))
                } else {
                    Ok((l, a1, a2))
                }
            }
            _ => Err(anyhow!("Unexpected events")),
        }
    }
}

pub async fn spawn_app() -> TestApp {
    Lazy::force(&TRACING);
    let test_id = Uuid::new_v4().to_simple().to_string();
    tracing::info!("TEST ID: {}", test_id);
    let database_address = "postgres://postgres:password@localhost:5432";
    let database_name = format!("db_{}", test_id);
    let input_topics: Vec<String> = ["overmuse-trades", "position-intents", "risk-check-response", "time"]
        .iter()
        .map(|topic| kafka_topic(topic, &test_id))
        .collect();
    let input_topics = input_topics.join(",");
    std::env::set_var("APP__UNREPORTED_TRADE_EXPIRY_SECONDS", "1");
    std::env::set_var("APP__TOPICS__ALLOCATION", format!("allocations-{}", test_id));
    std::env::set_var("APP__TOPICS__CLAIM", format!("claims-{}", test_id));
    std::env::set_var("APP__TOPICS__LOT", format!("lots-{}", test_id));
    std::env::set_var("APP__TOPICS__RISK", format!("risk-check-request-{}", test_id));
    std::env::set_var("APP__TOPICS__TRADE", format!("trade-intents-{}", test_id));
    std::env::set_var("DATABASE__NAME", database_name);
    std::env::set_var("DATABASE__URL", database_address);
    std::env::set_var("REDIS__URL", "redis://localhost:6379");
    std::env::set_var("KAFKA__BOOTSTRAP_SERVER", "localhost:9094");
    std::env::set_var("KAFKA__GROUP_ID", Uuid::new_v4().to_string());
    std::env::set_var("KAFKA__INPUT_TOPICS", input_topics);
    std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "localhost:9094");
    std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");
    std::env::set_var("KAFKA__ACKS", "0");
    std::env::set_var("KAFKA__RETRIES", "0");
    std::env::set_var("WEBSERVER__PORT", "0");
    let settings = Settings::new().unwrap();
    debug!("Configuring database");
    let db = configure_database(&settings.database, &test_id).await;
    debug!("Configuring kafka");
    let (consumer, producer) = configure_kafka(&test_id).await;

    tokio::spawn(async move {
        let res = run(settings).await;
        tracing::error!("{:?}", res);
    });
    TestApp {
        _db: db,
        test_id,
        consumer,
        producer,
    }
}

async fn configure_database(settings: &DatabaseSettings, test_id: &str) -> Client {
    let db_name = format!("db_{}", test_id);
    {
        // There's no need to spawn the connection to another thread here as we will be dropping it
        // in a few lines anyway
        let (client, connection) = connect(&settings.url, NoTls)
            .await
            .expect("Failed to connect to Postgres");
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error: {}", e);
            }
        });
        let query = format!("CREATE DATABASE {}", db_name);
        client
            .execute(query.as_str(), &[])
            .await
            .expect("Failed to create database");
    }
    let (client, connection) = connect(&format!("{}/{}", &settings.url, &db_name), NoTls)
        .await
        .expect("Failed to connect to Postgres");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {}", e);
        }
    });
    client
}

async fn configure_kafka(test_id: &str) -> (StreamConsumer, FutureProducer) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("security.protocol", "PLAINTEXT")
        .create()
        .unwrap();
    let admin_options = AdminOptions::new();
    let topic_names: Vec<String> = [
        "allocations",
        "claims",
        "lots",
        "overmuse-trades",
        "position-intents",
        "risk-check-request",
        "risk-check-response",
        "trade-intents",
        "time",
    ]
    .iter()
    .map(|topic| kafka_topic(topic, test_id))
    .collect();
    let topics: Vec<NewTopic> = topic_names
        .iter()
        .map(|name| NewTopic::new(name, 1, TopicReplication::Fixed(1)))
        .collect();
    admin.create_topics(&topics, &admin_options).await.unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("security.protocol", "PLAINTEXT")
        .create()
        .expect("Failed to create kafka producer");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("security.protocol", "PLAINTEXT")
        .set("group.id", "test-consumer")
        .create()
        .expect("Failed to create kafka consumer");

    let consumer_topic_names: Vec<String> = ["allocations", "claims", "lots", "risk-check-request", "trade-intents"]
        .iter()
        .map(|topic| kafka_topic(topic, test_id))
        .collect();
    let half_borrowed: Vec<&str> = consumer_topic_names.iter().map(|x| x.as_str()).collect();
    consumer
        .subscribe(&half_borrowed)
        .expect("Failed to subscribe to topics");
    (consumer, producer)
}
