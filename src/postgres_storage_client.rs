use std::{fmt::{Display, Formatter}, marker::PhantomData};

use crate::{StorageClient, StorageFormat, StorageObject, StorageSchema};
use async_trait::async_trait;
use sqlx::{Pool, Postgres};
use url::Url;


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresType {
    // 2 bytes
    SmallInt,
    // 4 bytes
    Integer,
    // 8 bytes
    BigInt,
    Decimal,
    Numeric {
        precision: Option<u8>,
        scale: Option<i8>,
    },
    // 4 bytes, 6 decimal precision
    Real,
    // 8 bytes, 15 decimal precision
    DoublePrecision,
    // 1 byte
    SmallSerial,
    // 4 bytes
    Serial,
    // 8 bytes
    BigSerial,
    // Money - skipping for now
    // variable length with limit
    VARCHAR {
        n: u32,
    },
    // fixed length, blank-padded
    CHAR {
        n: u32,
    },
    BPCHAR {
        n: Option<u32>,
    },
    TEXT,
    // variable length binary string
    BYTEA,
    TIMESTAMP {
        with_time_zone: bool,
    },
    DATE,
    TIME {
        with_time_zone: bool,
    },
    // Interval - skipping for now
    BOOLEAN,
}

impl Display for PostgresType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresType::SmallInt => write!(f, "SMALLINT"),
            PostgresType::Integer => write!(f, "INTEGER"),
            PostgresType::BigInt => write!(f, "BIGINT"),
            PostgresType::Decimal => write!(f, "DECIMAL"),
            PostgresType::Numeric { precision, scale } => {
                if let (Some(p), Some(s)) = (precision, scale) {
                    write!(f, "NUMERIC({}, {})", p, s)
                } else {
                    write!(f, "NUMERIC")
                }
            }
            PostgresType::Real => write!(f, "REAL"),
            PostgresType::DoublePrecision => write!(f, "DOUBLE PRECISION"),
            PostgresType::SmallSerial => write!(f, "SMALLSERIAL"),
            PostgresType::Serial => write!(f, "SERIAL"),
            PostgresType::BigSerial => write!(f, "BIGSERIAL"),
            PostgresType::VARCHAR { n } => write!(f, "VARCHAR({})", n),
            PostgresType::CHAR { n } => write!(f, "CHAR({})", n),
            PostgresType::BPCHAR { n } => {
                if let Some(n) = n {
                    write!(f, "BPCHAR({})", n)
                } else {
                    write!(f, "BPCHAR")
                }
            }
            PostgresType::TEXT => write!(f, "TEXT"),
            PostgresType::BYTEA => write!(f, "BYTEA"),
            PostgresType::TIMESTAMP { with_time_zone } => {
                if *with_time_zone {
                    write!(f, "TIMESTAMP WITH TIME ZONE")
                } else {
                    write!(f, "TIMESTAMP WITHOUT TIME ZONE")
                }
            }
            PostgresType::DATE => write!(f, "DATE"),
            PostgresType::TIME { with_time_zone } => {
                if *with_time_zone {
                    write!(f, "TIME WITH TIME ZONE")
                } else {
                    write!(f, "TIME WITHOUT TIME ZONE")
                }
            }
            PostgresType::BOOLEAN => write!(f, "BOOLEAN"),
        }
    }
}


pub struct PostgresStorageClient<F: StorageFormat> {
    storage_url: Url,
    pool: Pool<Postgres>,
    _formatter: PhantomData<F>,
}

impl<F: StorageFormat> PostgresStorageClient<F> {

    /// CREATE TABLE IF NOT EXISTS table_name
    /// - (column_name1 column_type1, column_name2 column_type2, ...)
    /// - PRIMARY KEY (primary_key_name)
    pub fn create_table_if_not_exists_query<O: StorageObject>() -> anyhow::Result<String> {
        match O::schema() {
            StorageSchema::Postgres { schema, primary_key } => {
                let columns: Vec<String> = schema.iter()
                    .map(|(name, typ)| format!("{} {}", name, typ))
                    .collect();
                let columns_str = columns.join(", ");
                Ok(format!(
                    "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}))",
                    O::type_name(),
                    columns_str,
                    primary_key
                ))
            }
            _ => {
                Err(anyhow::anyhow!("Schema is not Postgres"))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use ordermap::OrderMap;
    use serde::{Deserialize, Serialize};

    use crate::{json::JsonStorageFormat, postgres_storage_client::PostgresStorageClient, StorageObject, StorageSchema};

    use super::PostgresType;


    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestObject {
        key: i32,
        value: String,
    }

    impl StorageObject for TestObject {
        fn type_name() -> &'static str {
            "TestObject"
        }

        fn schema() -> crate::StorageSchema {
            let mut schema = OrderMap::new();
            schema.insert("key".to_string(), PostgresType::Integer);
            schema.insert("value".to_string(), PostgresType::VARCHAR { n: 255 });
            StorageSchema::Postgres {
                schema: schema,
                primary_key: "key".to_string(),
            }
        }
    }

    #[test]
    fn test_create_table_if_not_exists_query() {
        let query = PostgresStorageClient::<JsonStorageFormat>::create_table_if_not_exists_query::<TestObject>();
        assert!(query.is_ok());
        let query = query.unwrap();
        assert_eq!(
            query,
            "CREATE TABLE IF NOT EXISTS TestObject (key INTEGER, value VARCHAR(255), PRIMARY KEY (key))"
        );
    }
}