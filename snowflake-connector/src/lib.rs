use data_manipulation::DataManipulationResult;
use errors::SnowflakeError;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use serde::Serialize;
use snowflake_deserializer::{bindings::*, *};
use std::{collections::HashMap, path::Path};

use anyhow::{bail, Result};
pub use snowflake_deserializer;

pub mod data_manipulation;
pub mod errors;

mod jwt;

#[derive(Debug)]
pub struct SnowflakeConnector {
    token: String,
    host: String,
}

impl SnowflakeConnector {
    pub fn try_new(
        public_key_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
        host: &str,
        account_identifier: &str,
        user: &str,
    ) -> Result<Self, SnowflakeError> {
        let token = jwt::create_token(
            public_key_path.as_ref(),
            private_key_path.as_ref(),
            &account_identifier.to_ascii_uppercase(),
            &user.to_ascii_uppercase(),
        )?;
        Ok(SnowflakeConnector {
            token,
            host: format!("https://{host}.snowflakecomputing.com/api/v2/"),
        })
    }

    pub fn execute(&self) -> SnowflakeExecutor {
        SnowflakeExecutor {
            token: &self.token,
            host: &self.host,
        }
    }
}

#[derive(Debug)]
pub struct SnowflakeExecutor<'a> {
    token: &'a str,
    host: &'a str,
}

impl<'a> SnowflakeExecutor<'a> {
    pub fn sql(self, statement: &'a str) -> Result<SnowflakeSQL<'a>> {
        let headers = self.get_headers()?;
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
                role: None,
                bindings: None,
            },
            uuid: uuid::Uuid::new_v4(),
        })
    }
    fn get_headers(&self) -> Result<HeaderMap> {
        let token = self.token;
        println!("tokinator: {token:?}");
        let mut headers = HeaderMap::with_capacity(5);
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.append(AUTHORIZATION, format!("Bearer {}", token).parse()?);
        headers.append(
            "X-Snowflake-Authorization-Token-Type",
            HeaderValue::from_static("KEYPAIR_JWT"),
        );
        headers.append(ACCEPT, HeaderValue::from_static("application/json"));
        headers.append(
            USER_AGENT,
            HeaderValue::from_static(concat!(
                env!("CARGO_PKG_NAME"),
                '/',
                env!("CARGO_PKG_VERSION")
            )),
        );
        Ok(headers)
    }
}

#[derive(Debug)]
pub struct SnowflakeSQL<'a> {
    host: &'a str,
    client: reqwest::Client,
    statement: SnowflakeExecutorSQLJSON<'a>,
    uuid: uuid::Uuid,
}

impl<'a> SnowflakeSQL<'a> {
    pub async fn text(self) -> Result<String> {
        let res = self
            .client
            .post(self.get_url())
            .json(&self.statement)
            .send()
            .await?
            .text()
            .await?;
        Ok(res)
    }

    pub async fn select<T: SnowflakeDeserialize>(self) -> Result<SnowflakeSQLResult<T>> {
        let s = serde_json::to_string_pretty(&self.statement).expect("serializing shit");

        println!("sending {s}");
        let res = self
            .client
            .post(self.get_url())
            .json(&self.statement)
            .send()
            .await
            .map_err(|e| SnowflakeError::SqlExecution(e.into()))?;

        let status = res.status();
        let bs = res.bytes().await?;

        if !status.is_success() {
            let body_as_text = String::from_utf8_lossy(&bs);
            bail!("got non 2xx response: {status}. Body:\n{body_as_text}");
        }

        let result = match serde_json::from_slice::<SnowflakeSQLResponse>(&bs) {
            Ok(deserialized) => deserialized,
            Err(err) => {
                let body_as_text = String::from_utf8_lossy(&bs);
                bail!("Error parsing result as SnowflakeSqlResponse: {err}. Body:\n{body_as_text}")
            }
        };

        result.deserialize()
    }
    /// Use with `delete`, `insert`, `update` row(s).
    pub async fn manipulate(self) -> Result<DataManipulationResult> {
        let res = self
            .client
            .post(self.get_url())
            .json(&self.statement)
            .send()
            .await?
            .json()
            .await?;
        Ok(res)
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
        dbg!(format!(
            "{}statements?nullable=false&requestId={}",
            self.host, self.uuid
        ))
    }
}

#[derive(Serialize, Debug)]
pub struct SnowflakeExecutorSQLJSON<'a> {
    statement: &'a str,
    timeout: Option<u32>,
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
        let sql = SnowflakeConnector::try_new(
            "./environment_variables/local/rsa_key.pub",
            "./environment_variables/local/rsa_key.p8",
            "HOST".into(),
            "ACCOUNT".into(),
            "USER".into(),
        )?;
        let sql = sql
            .execute()
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
#[doc(hidden)]
pub use snowflake_connector_derive::*;
#[cfg(feature = "derive")]
pub use snowflake_deserializer::*;
