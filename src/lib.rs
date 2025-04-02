mod json;
mod file_stroage_client;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};


pub trait StorageObject: Send + Sync + DeserializeOwned + Serialize  {
    fn key(&self) -> &str;
}

pub trait StorageFormat {
    fn serialize<T: StorageObject>(&self, obj: &T) -> anyhow::Result<Vec<u8>>;
    fn deserialize<T: StorageObject>(&self, data: &[u8]) -> anyhow::Result<T>;
}

#[async_trait]
pub trait StorageClient: Send + Sync 
{

    fn path(&self, key: &str) -> anyhow::Result<String>;

     /// Retrieves the value associated with the key.
    /// - Returns `None` if the key does not exist.
    async fn get<O: StorageObject>(&self, key: &str) -> anyhow::Result<Option<O>>;

    /// Put a value associated with the key
    /// - If the key already exists, it will be overwritten
    async fn put<O: StorageObject>(&self, key: &str, value: O) -> anyhow::Result<()>;

    /// Delete the value associated with the key
    /// - Returns true if the key was deleted, false if it did not exist
    async fn delete(&self, key: &str) -> anyhow::Result<bool>;
}