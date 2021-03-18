use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Database {
    pub url: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "security.protocol")]
pub enum SecurityProtocol {
    #[serde(rename = "PLAINTEXT")]
    Plaintext,
    #[serde(rename = "SASL_SSL")]
    SaslSsl {
        #[serde(rename = "sasl.username")]
        sasl_username: String,
        #[serde(rename = "sasl.password")]
        sasl_password: String,
    },
}

#[derive(Debug, Deserialize)]
pub struct Kafka {
    #[serde(rename = "bootstrap.servers")]
    pub bootstrap_servers: String,
    #[serde(rename = "group.id")]
    pub group_id: String,
    #[serde(flatten)]
    pub security_protocol: SecurityProtocol,
}

impl Kafka {
    pub fn config<'a>(
        &self,
        config: &'a mut rdkafka::ClientConfig,
    ) -> &'a mut rdkafka::ClientConfig {
        config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", &self.group_id);
        match &self.security_protocol {
            SecurityProtocol::Plaintext => {
                config.set("security.protocol", "PLAINTEXT");
            }
            SecurityProtocol::SaslSsl {
                sasl_username,
                sasl_password,
            } => {
                config
                    .set("security.protocol", "SASL_SSL")
                    .set("sasl.mechanism", "PLAIN")
                    .set("sasl.username", sasl_username)
                    .set("sasl.password", sasl_password);
            }
        }
        config
    }
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub database: Database,
    pub kafka: Kafka,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(File::with_name("config/local").required(false))?;
        s.merge(Environment::with_prefix("app"))?;
        s.try_into()
    }
}
