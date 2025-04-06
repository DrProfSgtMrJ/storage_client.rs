use crate::StorageFormat;
use async_trait::async_trait;
use sqlx::{Pool, Postgres};
use url::Url;


pub struct PostgresStorageCLient<F: StorageFormat> {
    storage_url: Url,
    pool: Pool<Postgres>,
    formatter: F,
}

impl<F: StorageFormat> PostgresStorageCLient<F> {
    pub fn new(storage_url: Url, formatter: F) -> anyhow::Result<Self>{

        let connection_string = storage_url.as_str();
        let pool = Pool::connect_lazy(&connection_string)
            .map_err(|e| anyhow::anyhow!("Failed to connect to Postgres: {}", e))?;

        Ok(Self {
            storage_url,
            pool,
            formatter,
        })
    }

    pub fn get_query(&self, table_name: &str, key: &str) -> String {
        format!("SELECT value FROM {} WHERE key = $1", table_name)
    }
}

