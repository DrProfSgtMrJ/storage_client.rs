mod json;
mod file_stroage_client;
mod postgres_storage_client;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};


pub trait StorageObject: Send + Sync + DeserializeOwned + Serialize  {
    fn key(&self) -> &str;
    fn type_name() -> &'static str;
}

pub trait StorageFormat {
    fn serialize<T: StorageObject>(&self, obj: &T) -> anyhow::Result<Vec<u8>>;
    fn deserialize<T: StorageObject>(&self, data: &[u8]) -> anyhow::Result<T>;
}

#[async_trait]
pub trait StorageClient: Send + Sync 
{

    fn directory(&self) -> &str;

    fn object_directory<O: StorageObject>(&self) -> &str {
        O::type_name()
    }

    fn object_path<O: StorageObject>(&self, key: &str) -> String {
        let sub_directory = self.object_directory::<O>();
        let full_path = format!("{}/{}", self.directory(), sub_directory);
        let file_path = format!("{}/{}", full_path, key);
        file_path
    }

    /// Creates a subdirectory for the given object type.
    /// - The subdirectory name is the type name of the object.
    /// - Returns an error if the subdirectory already exists.
    async fn create_object_directory<O: StorageObject>(&self) -> anyhow::Result<()>;

     /// Retrieves the value associated with the key.
    /// - Returns `None` if the key does not exist.
    async fn get<O: StorageObject>(&self, key: &str) -> anyhow::Result<Option<O>>;

    /// Put a value associated with the key
    /// - If the key already exists, it will be overwritten
    async fn put<O: StorageObject>(&self, key: &str, value: O) -> anyhow::Result<()>;

    /// Delete the value associated with the key
    /// - Returns true if the key was deleted, false if it did not exist
    async fn delete<O: StorageObject>(&self, key: &str) -> anyhow::Result<bool>;

    /// Delete subdirectory
    /// - Returns true if the subdirectory was deleted, false if it did not exist
    async fn delete_object_directory<O: StorageObject>(&self) -> anyhow::Result<bool>;

    // /// Delete all objects in the storage
    async fn delete_all(&self) -> anyhow::Result<()>;

}