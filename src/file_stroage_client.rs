use anyhow::Context;
use async_trait::async_trait;
use url::Url;

use crate::{StorageClient, StorageFormat, StorageObject};
use tokio::io::AsyncWriteExt;



pub struct FileStorageClient<F: StorageFormat> {
    storage_url: Url,
    formatter: F,
}

impl<F: StorageFormat> FileStorageClient<F> {
    pub fn new(storage_url: Url, formatter: F) -> Self {
        Self {
            storage_url,
            formatter,
        }
    }
    pub async fn create_dir(&self) -> anyhow::Result<()> {
        match self.dir() {
            Ok(path) => {
                tokio::fs::create_dir_all(&path).await.with_context(|| {
                    format!("Failed to create directory at path: {}", path)
                })?;
            }
            Err(e) => {
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn dir(&self) -> anyhow::Result<String> {
        let path = self.storage_url.path();
        if path.is_empty() {
            return Err(anyhow::anyhow!("Storage URL does not have a valid path"));
        }
        Ok(path.to_string())
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

    pub fn full_path(&self, key: &str) -> anyhow::Result<String> {
        let path = self.storage_url.clone();
        match path.join(key) {
            Ok(full_path) => Ok(full_path.to_file_path()
                .map_err(|_| anyhow::anyhow!("Failed to convert URL to file path"))?
                .to_string_lossy()
                .to_string()),
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to join path: {}", e));
            }
        }
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
    fn path(&self, key: &str) -> anyhow::Result<String> {
        self.full_path(key.as_ref())
    }


    async fn get<O: StorageObject>(&self, key: &str) -> anyhow::Result<Option<O>> {
        let path = self.full_path(key.as_ref())?;

        match tokio::fs::read(path).await {
            Ok(data) => {
                let obj = self.formatter().deserialize(&data).with_context(|| {
                    format!("Failed to deserialize object for key: {}", key)
                })?;
                Ok(Some(obj))
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    async fn put<O: StorageObject>(&self, key: &str, value: O) -> anyhow::Result<()> {
        let path = self.full_path(key.as_ref())?;
        let mut file = match tokio::fs::File::create(&path).await {
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

    async fn delete(&self, key: &str) -> anyhow::Result<bool> {
        tokio::fs::remove_file(self.full_path(key.as_ref())?).await
            .map(|_| true)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            })
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

    impl StorageObject for TestObject {
        fn key(&self) -> &str {
            &self.key
        }
    }


    #[tokio::test]
    async fn test_file_storage_client_json() {
        let current_directory = std::env::current_dir().expect("Failed to get current directory"); 
        let test_directory = current_directory.join("test_dir");
        let url = Url::from_directory_path(test_directory).expect("Failed to create URL from directory path");
        let file_storage_client = FileStorageClient::<JsonStorageFormat>::new(url, JsonStorageFormat);

        assert!(file_storage_client.dir().is_ok());
        assert!(file_storage_client.create_dir().await.is_ok());

        // check if the directory exists
        let dir = file_storage_client.dir().unwrap();
        assert!(tokio::fs::metadata(&dir).await.is_ok());

        // with lifetime parameter
        let obj = TestObject {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        };

        // Put the object
        assert!(file_storage_client.put("test_key", obj.clone()).await.is_ok());

        // check the file exists
        let file_path = file_storage_client.full_path("test_key").unwrap();
        assert!(tokio::fs::metadata(&file_path).await.is_ok());

        // Get the object
        let retrieved_obj: Option<TestObject> = file_storage_client.get("test_key").await.unwrap();

        assert!(retrieved_obj.is_some());
        assert!(retrieved_obj.as_ref().unwrap().key == obj.key);
        assert!(retrieved_obj.as_ref().unwrap().value == obj.value);
        // Delete the object
        let result = file_storage_client.delete("test_key").await.unwrap();
        assert!(result);

        // check the file does not exist
        assert!(tokio::fs::metadata(file_path).await.is_err());

        // remove the directory
        assert!(file_storage_client.remove_dir().await.is_ok());

        // check the directory does not exist
        assert!(tokio::fs::metadata(dir).await.is_err());
    }
}