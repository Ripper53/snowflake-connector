use std::collections::HashMap;

use data_manipulation::DataManipulationResult;
use jwt_simple::algorithms::RS256KeyPair;
use reqwest::header::{HeaderMap, CONTENT_TYPE, AUTHORIZATION, ACCEPT, USER_AGENT};
use serde::Serialize;
use snowflake_deserializer::{*, bindings::*};
use errors::SnowflakeError;

pub mod data_manipulation;
pub mod errors;

mod jwt;

#[derive(Debug)]
pub struct SnowflakeConnector {
    token: String,
    host: String,
}

impl SnowflakeConnector {
    /// Create a new Snowflake connector.
    /// 
    /// This only supports key pair authentication.
    pub fn try_new(
        key_pair: &RS256KeyPair,
        host: String,
        account_identifier: String,
        user: String,
    ) -> Result<Self, SnowflakeError> {
        let token = jwt::create_token(
            key_pair,
            &account_identifier.to_ascii_uppercase(),
            &user.to_ascii_uppercase(),
        )?;
        Ok(SnowflakeConnector {
            token,
            host: format!("https://{host}.snowflakecomputing.com/api/v2/"),
        })
    }

    pub fn execute<D: ToString, W: ToString>(
        &self,
        database: D,
        warehouse: W,
    ) -> SnowflakeExecutor<D, W> {
        SnowflakeExecutor {
            token: &self.token,
            host: &self.host,
            database,
            warehouse,
        }
    }
}

#[derive(Debug)]
pub struct SnowflakeExecutor<'a, D: ToString, W: ToString> {
    token: &'a str,
    host: &'a str,
    database: D,
    warehouse: W,
}

impl<'a, D: ToString, W: ToString> SnowflakeExecutor<'a, D, W> {
    pub fn sql(self, statement: &'a str) -> Result<SnowflakeSQL<'a>, SnowflakeError> {
        let headers = self.get_headers()
            .map_err(SnowflakeError::SqlClient)?;
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|e| SnowflakeError::SqlClient(e.into()))?;
        Ok(SnowflakeSQL {
            client,
            host: self.host,
            statement: SnowflakeExecutorSQLJSON {
                statement,
                timeout: None,
                database: self.database.to_string(),
                warehouse: self.warehouse.to_string(),
                role: None,
                bindings: None,
            },
            uuid: uuid::Uuid::new_v4(),
        })
    }
    fn get_headers(&self) -> Result<HeaderMap, anyhow::Error> {
        let mut headers = HeaderMap::with_capacity(5);
        headers.append(CONTENT_TYPE, "application/json".parse()?);
        headers.append(AUTHORIZATION, format!("Bearer {}", self.token).parse()?);
        headers.append("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT".parse()?);
        headers.append(ACCEPT, "application/json".parse()?);
        headers.append(USER_AGENT, concat!(env!("CARGO_PKG_NAME"), '/', env!("CARGO_PKG_VERSION")).parse()?);
        Ok(headers)
    }
}

#[derive(Debug)]
pub struct SnowflakeSQL<'a> {
    client: reqwest::Client,
    host: &'a str,
    statement: SnowflakeExecutorSQLJSON<'a>,
    uuid: uuid::Uuid,
}

impl<'a> SnowflakeSQL<'a> {
    pub async fn text(self) -> Result<String, SnowflakeError> {
        self.client
            .post(self.get_url())
            .json(&self.statement)
            .send().await
            .map_err(|e| SnowflakeError::SqlExecution(e.into()))?
            .text().await
            .map_err(|e| SnowflakeError::SqlResultParse(e.into()))
    }
    pub async fn select<T: SnowflakeDeserialize>(self) -> Result<SnowflakeSQLResult<T>, SnowflakeError> {
        self.client
            .post(self.get_url())
            .json(&self.statement)
            .send().await
            .map_err(|e| SnowflakeError::SqlExecution(e.into()))?
            .json::<SnowflakeSQLResponse>().await
            .map_err(|e| SnowflakeError::SqlResultParse(e.into()))?
            .deserialize()
            .map_err(SnowflakeError::SqlResultParse)
    }
    /// Use with `delete`, `insert`, `update` row(s).
    pub async fn manipulate(self) -> Result<DataManipulationResult, SnowflakeError> {
        self.client
            .post(self.get_url())
            .json(&self.statement)
            .send().await
            .map_err(|e| SnowflakeError::SqlExecution(e.into()))?
            .json().await
            .map_err(|e| SnowflakeError::SqlExecution(e.into()))
    }
    pub fn with_timeout(mut self, timeout: u32) -> SnowflakeSQL<'a> {
        self.statement.timeout = Some(timeout);
        self
    }
    pub fn with_role<R: ToString>(mut self, role: R) -> SnowflakeSQL<'a> {
        self.statement.role = Some(role.to_string());
        self
    }
    pub fn add_binding<T: Into<BindingValue>>(mut self, value: T) -> SnowflakeSQL<'a> {
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
        format!("{}statements?nullable=false&requestId={}", self.host, self.uuid)
    }
}

#[derive(Serialize, Debug)]
pub struct SnowflakeExecutorSQLJSON<'a> {
    statement: &'a str,
    timeout: Option<u32>,
    database: String,
    warehouse: String,
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
        let key_pair = RS256KeyPair::generate(2048)?;
        let sql = SnowflakeConnector::try_new(
            &key_pair,
            "HOST".into(),
            "ACCOUNT".into(),
            "USER".into(),
        )?;
        let sql = sql.execute("DB", "WH")
            .sql("SELECT * FROM TEST_TABLE WHERE id = ? AND name = ?")?
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
pub use snowflake_deserializer::*;
#[cfg(feature = "derive")]
#[doc(hidden)]
pub use snowflake_connector_derive::*;
