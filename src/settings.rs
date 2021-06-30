use config::{Config, ConfigError, Environment};
use kafka_settings::KafkaSettings;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Database {
    pub url: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct WebServerSettings {
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub database: Database,
    pub kafka: KafkaSettings,
    pub webserver: WebServerSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(Environment::new().separator("__"))?;
        s.try_into()
    }
}
