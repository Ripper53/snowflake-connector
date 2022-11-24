use std::collections::HashMap;
use reqwest::header::{HeaderMap, CONTENT_TYPE, AUTHORIZATION, ACCEPT, USER_AGENT};
use serde::Serialize;
use snowflake_connector_macros::*;

mod jwt;

#[derive(Debug)]
pub struct SnowflakeConnector {
    token: String,
    host: String,
}

impl SnowflakeConnector {
    pub fn try_new(
        host: String,
        account_identifier: String,
        user: String,
    ) -> Result<Self, anyhow::Error> {
        Ok(SnowflakeConnector {
            token: jwt::create_token(&account_identifier.to_ascii_uppercase(), &user.to_ascii_uppercase())?,
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
    pub async fn sql(self, statement: &'a str) -> SnowflakeSQL<'a> {
        SnowflakeSQL {
            host: self.host,
            token: self.token,
            statement: SnowflakeExecutorSQLJSON {
                statement,
                timeout: None,
                database: self.database.to_string(),
                warehouse: self.warehouse.to_string(),
                role: None,
                bindings: None,
            },
            uuid: uuid::Uuid::new_v4(),
        }
    }
}

pub struct SnowflakeSQL<'a> {
    host: &'a str,
    token: &'a str,
    statement: SnowflakeExecutorSQLJSON<'a>,
    uuid: uuid::Uuid,
}

impl<'a> SnowflakeSQL<'a> {
    pub async fn text(self) -> Result<String, anyhow::Error> {
        let headers = self.get_headers()?;
        let client = reqwest::Client::new();
        Ok(client.post(self.get_url())
            .headers(headers)
            .json(&self.statement)
            .send().await?
            .text().await?)
    }
    pub async fn run<T: SnowflakeDeserialize>(self) -> Result<SnowflakeSQLResult<T>, anyhow::Error> {
        let headers = self.get_headers()?;
        let client = reqwest::Client::new();
        client.post(self.get_url())
            .headers(headers)
            .json(&self.statement)
            .send().await?
            .json::<SnowflakeSQLResponse>().await?
            .deserialize()
    }
    pub fn with_timeout(mut self, timeout: u32) -> SnowflakeSQL<'a> {
        self.statement.timeout = Some(timeout);
        self
    }
    pub fn with_role<R: ToString>(mut self, role: R) -> SnowflakeSQL<'a> {
        self.statement.role = Some(role.to_string());
        self
    }
    pub fn add_binding(mut self) -> SnowflakeSQL<'a> {
        // TODO
        todo!();
        self
    }
    fn get_url(&self) -> String {
        // TODO: make another return type that allows retrying by calling same statement again with retry flag!
        format!("{}statements?nullable=false&requestId={}", self.host, self.uuid)
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

#[derive(Serialize)]
struct SnowflakeExecutorSQLJSON<'a> {
    statement: &'a str,
    timeout: Option<u32>,
    database: String,
    warehouse: String,
    role: Option<String>,
    bindings: Option<HashMap<&'a str, &'a str>>,
}
