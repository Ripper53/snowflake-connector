use data_manipulation::DataManipulationResult;
use jwt::{KeyPairError, TokenFromFileError};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, USER_AGENT};
use serde::Serialize;
use snowflake_deserializer::{bindings::*, *};
use std::{collections::HashMap, path::Path};

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

// Features
#[cfg(feature = "derive")]
#[doc(hidden)]
pub use snowflake_connector_derive::*;
#[cfg(feature = "derive")]
pub use snowflake_deserializer::*;
