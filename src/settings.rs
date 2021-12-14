use config::{Config, ConfigError, Environment};
use kafka_settings::KafkaSettings;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct TopicSettings {
    pub allocation: String,
    pub claim: String,
    pub lot: String,
    pub risk: String,
    pub trade: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AppSettings {
    pub topics: TopicSettings,
    pub unreported_trade_expiry_seconds: usize,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseSettings {
    pub url: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct SentrySettings {
    pub environment: String,
    pub dsn: String,
}

#[derive(Debug, Deserialize)]
pub struct DatastoreSettings {
    pub base_url: String,
}

#[derive(Debug, Deserialize)]
pub struct WebServerSettings {
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub app: AppSettings,
    pub database: DatabaseSettings,
    pub kafka: KafkaSettings,
    pub datastore: DatastoreSettings,
    pub sentry: SentrySettings,
    pub webserver: WebServerSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(Environment::new().separator("__"))?;
        s.try_into()
    }
}
