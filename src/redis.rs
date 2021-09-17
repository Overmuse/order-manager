use crate::settings::RedisSettings;
use anyhow::Result;
use mobc::{Connection, Pool};
use mobc_redis::RedisConnectionManager;
use redis::{AsyncCommands, FromRedisValue};
use rust_decimal::prelude::*;
use std::time::Duration;

#[derive(Clone)]
pub struct Redis {
    pool: Pool<RedisConnectionManager>,
}

impl Redis {
    pub fn new(settings: RedisSettings) -> Result<Self> {
        let client = redis::Client::open(settings.url)?;
        let manager = RedisConnectionManager::new(client);
        let pool = Pool::builder()
            .get_timeout(Some(Duration::from_secs(1)))
            .max_open(16)
            .max_idle(8)
            .max_lifetime(Some(Duration::from_secs(60)))
            .build(manager);
        Ok(Self { pool })
    }

    async fn get_connection(&self) -> Result<Connection<RedisConnectionManager>> {
        Ok(self.pool.get().await?)
    }

    pub async fn get<T: Send + FromRedisValue>(&self, key: &str) -> Result<T> {
        let mut con = self.get_connection().await?;
        Ok(con.get::<&str, T>(key).await?)
    }

    pub async fn get_latest_price(&self, ticker: &str) -> Result<Option<Decimal>> {
        Ok(self
            .get::<Option<f64>>(&format!("price/{}", ticker))
            .await?
            .map(Decimal::from_f64)
            .flatten())
    }
}
