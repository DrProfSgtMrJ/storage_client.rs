use crate::{StorageFormat, StorageObject};


#[derive(Debug, Clone)]
pub struct JsonStorageFormat;

impl StorageFormat for JsonStorageFormat {
    fn serialize<T: StorageObject>(&self, obj: &T) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(obj).map_err(|e| e.into())
    }

    fn deserialize<T: StorageObject>(&self, data: &[u8]) -> anyhow::Result<T> {
        serde_json::from_slice(data).map_err(|e| e.into())
    }
}