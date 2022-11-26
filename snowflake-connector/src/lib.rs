use std::collections::HashMap;
use reqwest::header::{HeaderMap, CONTENT_TYPE, AUTHORIZATION, ACCEPT, USER_AGENT};
use serde::Serialize;
use snowflake_deserializer::{*, bindings::*};

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
    pub fn sql(self, statement: &'a str) -> Result<SnowflakeSQL<'a>, anyhow::Error> {
        let headers = self.get_headers()?;
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
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
            binding_counter: 0,
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

pub struct SnowflakeSQL<'a> {
    client: reqwest::Client,
    host: &'a str,
    statement: SnowflakeExecutorSQLJSON<'a>,
    uuid: uuid::Uuid,
    binding_counter: usize,
}

impl<'a> SnowflakeSQL<'a> {
    pub async fn text(self) -> Result<String, anyhow::Error> {
        Ok(self.client
            .post(self.get_url())
            .json(&self.statement)
            .send().await?
            .text().await?)
    }
    pub async fn run<T: SnowflakeDeserialize>(self) -> Result<SnowflakeSQLResult<T>, anyhow::Error> {
        self.client
            .post(self.get_url())
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
    pub fn add_binding<T: Into<BindingValue>>(mut self, value: T) -> SnowflakeSQL<'a> {
        let value: BindingValue = value.into();
        let binding = Binding {
            value_type: value.to_type().to_string(),
            value: value.to_string(),
        };
        self.binding_counter += 1;
        let index = self.binding_counter.to_string();
        if let Some(bindings) = &mut self.statement.bindings {
            bindings.insert(index, binding);
        } else {
            self.statement.bindings = Some(HashMap::from([(index, binding)]));
        }
        self
    }
    fn get_url(&self) -> String {
        // TODO: make another return type that allows retrying by calling same statement again with retry flag!
        format!("{}statements?nullable=false&requestId={}", self.host, self.uuid)
    }
}

#[derive(Serialize)]
pub struct SnowflakeExecutorSQLJSON<'a> {
    statement: &'a str,
    timeout: Option<u32>,
    database: String,
    warehouse: String,
    role: Option<String>,
    bindings: Option<HashMap<String, Binding>>,
}

#[derive(Serialize)]
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
        let sql = SnowflakeConnector::try_new("HOST".into(), "ACCOUNT".into(), "USER".into())?;
        let sql = sql.execute("DB", "WH")
            .sql("SELECT * FROM TEST_TABLE WHERE id = ? AND name = ?")?
            .add_binding(27);
        assert_eq!(sql.binding_counter, 1);
        let sql = sql.add_binding("JoMama");
        assert_eq!(sql.binding_counter, 2);
        Ok(())
    }
}

// Features
#[cfg(feature = "derive")]
pub use snowflake_deserializer::*;
#[cfg(feature = "derive")]
#[doc(hidden)]
pub use snowflake_connector_derive::*;
