use std::any;

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{StorageClient, StorageFormat, StorageObject};
use tokio::io::AsyncWriteExt;

pub struct FileStorageClient<F: StorageFormat> {
    storage_url: Url,
    formatter: F,
}

impl<F: StorageFormat> FileStorageClient<F> {
    pub async fn new(storage_url: Url, formatter: F) -> anyhow::Result<Self> {
        // Creates the main directory if it does not exist
        let path = storage_url.path();
        if path.is_empty() {
            return Err(anyhow::anyhow!("Storage URL does not have a valid path"));
        }
        tokio::fs::create_dir_all(path).await.with_context(|| {
            format!("Failed to create directory at path: {}", path)
        })?;

        Ok(Self {
            storage_url,
            formatter,
        })
    }



    pub async fn remove_dir(&self) -> anyhow::Result<()> {
        let path = self.storage_url.path();
        if path.is_empty() {
            return Err(anyhow::anyhow!("Storage URL does not have a valid path"));
        }
        tokio::fs::remove_dir_all(path).await.with_context(|| {
            format!("Failed to remove directory at path: {}", path)
        })?;
        Ok(())
    }

    pub fn formatter(&self) -> &F {
        &self.formatter
    }
}

#[async_trait]
impl<F> StorageClient for FileStorageClient<F>
where 
    F: StorageFormat + Send + Sync,
{

    fn directory(&self) -> &str {
        self.storage_url.path()
    }

    async fn create_object_directory<O: StorageObject>(&self) -> anyhow::Result<()> {
        let full_path = format!("{}/{}", self.directory(), self.object_directory::<O>());

        tokio::fs::create_dir_all(&full_path).await.with_context(|| {
            format!("Failed to create subdirectory at path: {}", full_path)
        })?;

        Ok(())
    }
    // Retrieves the value associated with the key.
    // - Name of object = the subdirectory
    // - key = the file name
    async fn get<O: StorageObject>(&self, key: &str) -> anyhow::Result<Option<O>> {
        let file_path = self.object_path::<O>(key);
        match tokio::fs::read(file_path).await {
            Ok(data) => {
                let obj = self.formatter().deserialize(&data).with_context(|| {
                    format!("Failed to deserialize {} for key: {}", O::type_name(), key)
                })?;
                Ok(Some(obj))
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    async fn put<O: StorageObject>(&self, key: &str, value: O) -> anyhow::Result<()> {
        let file_path = self.object_path::<O>(key);
        let mut file = match tokio::fs::File::create(&file_path).await {
            Ok(file) => file,
            Err(e) => return Err(e.into()),
        };

        let data = self.formatter().serialize(&value).with_context(|| {
            format!("Failed to serialize object for key: {}", key)
        })?;

        file.write_all(&data).await.with_context(|| {
            format!("Failed to write object to file for key: {}", key)
        })?;

        Ok(())
    }

    async fn delete<O: StorageObject>(&self, key: &str) -> anyhow::Result<bool> {
        let file_path = self.object_path::<O>(key);
        tokio::fs::remove_file(file_path).await
            .map(|_| true)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            })
    }

    async fn delete_object_directory<O: StorageObject>(&self) -> anyhow::Result<bool> {
        let full_path = format!("{}/{}", self.directory(), self.object_directory::<O>());
        tokio::fs::remove_dir_all(full_path).await
            .map(|_| true)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            })
    }

    async fn delete_all(&self) -> anyhow::Result<()> {
        let path = self.storage_url.path();
        if path.is_empty() {
            return Err(anyhow::anyhow!("Storage URL does not have a valid path"));
        }
        tokio::fs::remove_dir_all(path).await.with_context(|| {
            format!("Failed to remove directory at path: {}", path)
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {


    use crate::json::JsonStorageFormat;

    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestObject {
        key: String,
        value: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestStorageKey {
        name: String,
        value: String,
    }


    impl StorageObject for TestObject {
        fn key(&self) -> &str {
            &self.key
        }

        fn type_name() -> &'static str {
            "TestObject"
        }
    }


    #[tokio::test]
    async fn test_file_storage_client_json() {
        let current_directory = std::env::current_dir().expect("Failed to get current directory"); 
        let test_directory = current_directory.join("test_dir");
        let url = Url::from_directory_path(test_directory).expect("Failed to create URL from directory path");
        let file_storage_client = FileStorageClient::<JsonStorageFormat>::new(url, JsonStorageFormat).await;
        assert!(file_storage_client.is_ok());

        let file_storage_client = file_storage_client.unwrap();
        // check if the directory exists
        let dir = file_storage_client.directory();
        assert!(tokio::fs::metadata(&dir).await.is_ok());

        // create subdirectory
        assert!(file_storage_client.create_object_directory::<TestObject>().await.is_ok());

        // check if the subdirectory exists
        let full_path = format!("{}/{}", dir, file_storage_client.object_directory::<TestObject>());
        assert!(tokio::fs::metadata(full_path).await.is_ok());

        // with lifetime parameter
        let obj = TestObject {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        };

        // Put the object
        file_storage_client
            .put("test_key", obj.clone())
            .await
            .expect("Failed to put object");

        // check the file exists
        let file_path = file_storage_client.object_path::<TestObject>("test_key");
        assert!(tokio::fs::metadata(&file_path).await.is_ok());

        // Get the object
        let retrieved_obj: Option<TestObject> = file_storage_client.get("test_key").await.unwrap();

        assert!(retrieved_obj.is_some());
        assert!(retrieved_obj.as_ref().unwrap().key == obj.key);
        assert!(retrieved_obj.as_ref().unwrap().value == obj.value);
        // Delete the object
        let result = file_storage_client.delete::<TestObject>("test_key").await.unwrap();
        assert!(result);

        // check the file does not exist
        assert!(tokio::fs::metadata(file_path).await.is_err());

        // remove the subdirectory
        assert!(file_storage_client.delete_object_directory::<TestObject>().await.is_ok());
        // check the subdirectory does not exist
        let sub_dir = format!("{}/{}", dir, file_storage_client.object_directory::<TestObject>());
        assert!(tokio::fs::metadata(sub_dir).await.is_err());

        // remove the directory
        assert!(file_storage_client.delete_all().await.is_ok());

        // check the directory does not exist
        assert!(tokio::fs::metadata(dir).await.is_err());
    }
}