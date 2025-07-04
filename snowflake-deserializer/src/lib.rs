use data_manipulation::DataManipulationResult;
use jwt::{KeyPairError, TokenFromFileError};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, USER_AGENT};
use serde::{Deserialize, Serialize};
pub use serde_json;
use std::{collections::HashMap, path::Path, str::FromStr};

use crate::bindings::{BindingType, BindingValue};

pub mod bindings;
pub mod data_manipulation;

mod jwt;

#[derive(Debug)]
pub struct SnowflakeConnector {
    host: String,
    client: reqwest::Client,
}

impl SnowflakeConnector {
    pub fn try_new(
        public_key: &str,
        private_key: &str,
        host: &str,
        account_identifier: &str,
        user: &str,
    ) -> Result<Self, NewSnowflakeConnectorError> {
        let token = jwt::create_token(
            public_key,
            private_key,
            &account_identifier.to_ascii_uppercase(),
            &user.to_ascii_uppercase(),
        )?;
        let headers = Self::get_headers(&token);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        Ok(SnowflakeConnector {
            host: format!("https://{host}.snowflakecomputing.com/api/v2/"),
            client,
        })
    }
    pub fn try_new_from_file<P: AsRef<Path>>(
        public_key_path: P,
        private_key_path: P,
        host: &str,
        account_identifier: &str,
        user: &str,
    ) -> Result<Self, NewSnowflakeConnectorFromFileError> {
        let token = jwt::create_token_from_file(
            public_key_path,
            private_key_path,
            &account_identifier.to_ascii_uppercase(),
            &user.to_ascii_uppercase(),
        )?;
        let headers = Self::get_headers(&token);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        Ok(SnowflakeConnector {
            host: format!("https://{host}.snowflakecomputing.com/api/v2/"),
            client,
        })
    }

    pub fn execute<D: ToString>(&self, database: D) -> SnowflakeExecutor<D> {
        SnowflakeExecutor {
            host: &self.host,
            database,
            client: &self.client,
        }
    }
    fn get_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::with_capacity(5);
        headers.append(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.append(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());
        headers.append(
            "X-Snowflake-Authorization-Token-Type",
            "KEYPAIR_JWT".parse().unwrap(),
        );
        headers.append(ACCEPT, "application/json".parse().unwrap());
        headers.append(
            USER_AGENT,
            concat!(env!("CARGO_PKG_NAME"), '/', env!("CARGO_PKG_VERSION"))
                .parse()
                .unwrap(),
        );
        headers
    }
}

#[derive(thiserror::Error, Debug)]
pub enum NewSnowflakeConnectorError {
    #[error(transparent)]
    KeyPair(#[from] KeyPairError),
    #[error(transparent)]
    ClientBuildError(#[from] reqwest::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum NewSnowflakeConnectorFromFileError {
    #[error(transparent)]
    Token(#[from] TokenFromFileError),
    #[error(transparent)]
    ClientBuildError(#[from] reqwest::Error),
}

#[derive(Debug)]
pub struct SnowflakeExecutor<'a, D: ToString> {
    host: &'a str,
    database: D,
    client: &'a reqwest::Client,
}

impl<'a, D: ToString> SnowflakeExecutor<'a, D> {
    pub fn sql(self, statement: &'a str) -> SnowflakeSQL<'a> {
        SnowflakeSQL {
            client: self.client,
            host: self.host,
            statement: SnowflakeExecutorSQLJSON {
                statement,
                timeout: None,
                database: self.database.to_string(),
                warehouse: None,
                role: None,
                bindings: None,
            },
            uuid: uuid::Uuid::new_v4(),
        }
    }
}

#[derive(Debug)]
pub struct SnowflakeSQL<'a> {
    client: &'a reqwest::Client,
    host: &'a str,
    statement: SnowflakeExecutorSQLJSON<'a>,
    uuid: uuid::Uuid,
}

impl<'a> SnowflakeSQL<'a> {
    pub async fn text(self) -> Result<String, SnowflakeSQLTextError> {
        self.client
            .post(self.get_url())
            .json(&self.statement)
            .send()
            .await
            .map_err(SnowflakeSQLTextError::Request)?
            .text()
            .await
            .map_err(SnowflakeSQLTextError::ToText)
    }
    /// Use with `SELECT` queries.
    pub async fn select<T: SnowflakeDeserialize>(
        self,
    ) -> Result<SnowflakeSQLResult<T>, SnowflakeSQLSelectError<T::Error>> {
        self.client
            .post(self.get_url())
            .json(&self.statement)
            .send()
            .await
            .map_err(SnowflakeSQLSelectError::Request)?
            .json::<SnowflakeSQLResponse>()
            .await
            .map_err(SnowflakeSQLSelectError::Decode)?
            .deserialize()
            .map_err(SnowflakeSQLSelectError::Deserialize)
    }
    /// Use with `DELETE`, `INSERT`, `UPDATE` queries.
    pub async fn manipulate(self) -> Result<DataManipulationResult, SnowflakeSQLManipulateError> {
        self.client
            .post(self.get_url())
            .json(&self.statement)
            .send()
            .await
            .map_err(SnowflakeSQLManipulateError::Request)?
            .json()
            .await
            .map_err(SnowflakeSQLManipulateError::Decode)
    }
    pub fn with_timeout(mut self, timeout: u32) -> Self {
        self.statement.timeout = Some(timeout);
        self
    }
    pub fn with_role<R: ToString>(mut self, role: R) -> Self {
        self.statement.role = Some(role.to_string());
        self
    }
    pub fn with_warehouse<W: ToString>(mut self, warehouse: W) -> Self {
        self.statement.warehouse = Some(warehouse.to_string());
        self
    }
    pub fn add_binding<T: Into<BindingValue>>(mut self, value: T) -> Self {
        let value: BindingValue = value.into();
        let value_str = value.to_string();
        let value_type: BindingType = value.into();
        let binding = Binding {
            value_type: value_type.to_string(),
            value: value_str,
        };
        if let Some(bindings) = &mut self.statement.bindings {
            bindings.insert((bindings.len() + 1).to_string(), binding);
        } else {
            self.statement.bindings = Some(HashMap::from([("1".into(), binding)]));
        }
        self
    }
    fn get_url(&self) -> String {
        // TODO: make another return type that allows retrying by calling same statement again with retry flag!
        format!(
            "{}statements?nullable=false&requestId={}",
            self.host, self.uuid
        )
    }
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub enum SnowflakeSQLTextError {
    Request(reqwest::Error),
    ToText(reqwest::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum SnowflakeSQLSelectError<DeserializeError> {
    #[error(transparent)]
    Request(reqwest::Error),
    #[error(transparent)]
    Decode(reqwest::Error),
    #[error(transparent)]
    Deserialize(#[from] DeserializeError),
}

#[derive(thiserror::Error, Debug)]
pub enum SnowflakeSQLManipulateError {
    #[error(transparent)]
    Request(reqwest::Error),
    #[error(transparent)]
    Decode(reqwest::Error),
}

#[derive(Serialize, Debug)]
pub struct SnowflakeExecutorSQLJSON<'a> {
    statement: &'a str,
    timeout: Option<u32>,
    database: String,
    warehouse: Option<String>,
    role: Option<String>,
    bindings: Option<HashMap<String, Binding>>,
}

#[derive(Serialize, Debug)]
pub struct Binding {
    #[serde(rename = "type")]
    value_type: String,
    value: String,
}

pub trait SnowflakeDeserialize {
    type Error;
    fn snowflake_deserialize(
        response: SnowflakeSQLResponse,
    ) -> Result<SnowflakeSQLResult<Self>, Self::Error>
    where
        Self: Sized;
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SnowflakeSQLResponse {
    pub result_set_meta_data: MetaData,
    pub data: Vec<Vec<String>>,
    pub code: String,
    pub statement_status_url: String,
    pub request_id: String,
    pub sql_state: String,
    pub message: String,
    //pub created_on: u64,
}

impl SnowflakeSQLResponse {
    pub fn deserialize<T: SnowflakeDeserialize>(self) -> Result<SnowflakeSQLResult<T>, T::Error> {
        T::snowflake_deserialize(self)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetaData {
    pub num_rows: usize,
    pub format: String,
    pub row_type: Vec<RowType>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RowType {
    pub name: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub precision: Option<u32>,
    pub byte_length: Option<usize>,
    #[serde(rename = "type")]
    pub data_type: String,
    pub scale: Option<i32>,
    pub nullable: bool,
    //pub collation: ???,
    //pub length: ???,
}

#[derive(Debug)]
pub struct SnowflakeSQLResult<T> {
    pub data: Vec<T>,
}

/// For custom data parsing,
/// ex. you want to convert the retrieved data (strings) to enums.
///
/// Data in cells are not their type, they are simply strings that need to be converted.
pub trait DeserializeFromStr {
    type Error;
    fn deserialize_from_str(s: &str) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

impl<T: DeserializeFromStr> DeserializeFromStr for Option<T> {
    type Error = <T as DeserializeFromStr>::Error;
    fn deserialize_from_str(s: &str) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        if s == "NULL" {
            Ok(None)
        } else {
            <T as DeserializeFromStr>::deserialize_from_str(s).map(|f| Some(f))
        }
    }
}
macro_rules! impl_deserialize_from_str {
    ($ty: ty) => {
        impl DeserializeFromStr for $ty {
            type Error = <$ty as FromStr>::Err;
            fn deserialize_from_str(s: &str) -> Result<Self, Self::Error> {
                <$ty>::from_str(s)
            }
        }
    };
}

impl_deserialize_from_str!(bool);
impl_deserialize_from_str!(usize);
impl_deserialize_from_str!(isize);
impl_deserialize_from_str!(u8);
impl_deserialize_from_str!(u16);
impl_deserialize_from_str!(u32);
impl_deserialize_from_str!(u64);
impl_deserialize_from_str!(u128);
impl_deserialize_from_str!(i16);
impl_deserialize_from_str!(i32);
impl_deserialize_from_str!(i64);
impl_deserialize_from_str!(i128);
impl_deserialize_from_str!(f32);
impl_deserialize_from_str!(f64);
impl_deserialize_from_str!(String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql() -> Result<(), anyhow::Error> {
        let connector = SnowflakeConnector::try_new_from_file(
            "./environment_variables/local/rsa_key.pub",
            "./environment_variables/local/rsa_key.p8",
            "HOST",
            "ACCOUNT",
            "USER",
        )?;
        let sql = connector
            .execute("DB")
            .sql("SELECT * FROM TEST_TABLE WHERE id = ? AND name = ?")
            .add_binding(69);
        if let Some(bindings) = &sql.statement.bindings {
            assert_eq!(bindings.len(), 1);
        } else {
            assert!(sql.statement.bindings.is_some());
        }
        let sql = sql.add_binding("JoMama");
        if let Some(bindings) = &sql.statement.bindings {
            assert_eq!(bindings.len(), 2);
        } else {
            assert!(sql.statement.bindings.is_some());
        }
        Ok(())
    }
}
