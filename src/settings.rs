use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use stream_processor::KafkaSettings;

#[derive(Debug, Deserialize)]
pub struct Database {
    pub url: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub database: Database,
    pub kafka: KafkaSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(File::with_name("config/local").required(false))?;
        s.merge(Environment::with_prefix("app"))?;
        s.try_into()
    }
}
