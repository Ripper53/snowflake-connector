use anyhow::{bail, Context as _};
use data_manipulation::DataManipulationResult;
use reqwest::header::{HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use serde::Serialize;
use std::{collections::HashMap, path::Path};

pub use snowflake_derive::*;
pub use snowflake_deserialize::*;

pub mod data_manipulation;
mod jwt;

type Result<T> = anyhow::Result<T>;

#[derive(Debug)]
pub struct SnowflakeConnector {
    host: String,

    client: reqwest::Client,
}

impl SnowflakeConnector {
    pub fn try_new(
        public_key_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
        host: &str,
        account_identifier: &str,
        user: &str,
    ) -> Result<Self> {
        let token = jwt::create_token(
            public_key_path.as_ref(),
            private_key_path.as_ref(),
            &account_identifier.to_ascii_uppercase(),
            &user.to_ascii_uppercase(),
        )?;

        let auth_header = HeaderValue::from_str(&format!("Bearer {}", token))?;
        let user_agent = concat!(env!("CARGO_PKG_NAME"), '/', env!("CARGO_PKG_VERSION"));

        let headers = [
            (CONTENT_TYPE, HeaderValue::from_static("application/json")),
            (AUTHORIZATION, auth_header),
            (ACCEPT, HeaderValue::from_static("application/json")),
            (USER_AGENT, HeaderValue::from_static(user_agent)),
            (
                HeaderName::from_static("x-snowflake-authorization-token-type"),
                HeaderValue::from_static("KEYPAIR_JWT"),
            ),
        ];

        let client = reqwest::Client::builder()
            .default_headers(headers.into_iter().collect())
            .build()?;

        Ok(SnowflakeConnector {
            host: format!("https://{host}.snowflakecomputing.com/api/v2/"),
            client,
        })
    }

    pub fn sql(&self, statement: impl Into<String>) -> PendingQuery<'_> {
        PendingQuery {
            client: &self.client,
            host: &self.host,
            query: SnowflakeQuery {
                statement: statement.into(),
                timeout: None,
                role: None,
                bindings: Default::default(),
            },
            uuid: uuid::Uuid::new_v4(),
        }
    }
}

#[derive(Debug)]
pub struct PendingQuery<'c> {
    client: &'c reqwest::Client,
    host: &'c str,
    query: SnowflakeQuery,
    uuid: uuid::Uuid,
}

impl<'c> PendingQuery<'c> {
    pub async fn text(self) -> Result<String> {
        let res = self
            .client
            .post(self.get_url())
            .json(&self.query)
            .send()
            .await?
            .text()
            .await?;
        Ok(res)
    }

    pub async fn select<T: SnowflakeDeserialize>(self) -> Result<SnowflakeSqlResult<T>> {
        let s = serde_json::to_string_pretty(&self.query).expect("serializing shit");

        println!("sending {s}");
        let res = self
            .client
            .post(self.get_url())
            .json(&self.query)
            .send()
            .await
            .context("sending query")?;

        let status = res.status();
        let bs = res.bytes().await?;

        if !status.is_success() {
            let body_as_text = String::from_utf8_lossy(&bs);
            bail!("got non 2xx response: {status}. Body:\n{body_as_text}");
        }

        let result = match serde_json::from_slice::<SnowflakeSqlResponse>(&bs) {
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
            .json(&self.query)
            .send()
            .await?
            .json()
            .await?;
        Ok(res)
    }
    pub fn with_timeout(mut self, timeout: u32) -> Self {
        self.query.timeout = Some(timeout);
        self
    }
    pub fn with_role<R: ToString>(mut self, role: R) -> Self {
        self.query.role = Some(role.to_string());
        self
    }
    pub fn add_binding<T: Into<BindingValue>>(mut self, value: T) -> Self {
        let value: BindingValue = value.into();

        let binding = Binding {
            kind: value.kind(),
            value: value.to_string(),
        };

        self.query
            .bindings
            .insert((self.query.bindings.len() + 1).to_string(), binding);

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

#[derive(Serialize, Debug)]
pub struct SnowflakeQuery {
    statement: String,
    timeout: Option<u32>,
    role: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    bindings: HashMap<String, Binding>,
}

#[derive(Serialize, Debug)]
pub struct Binding {
    #[serde(rename = "type")]
    kind: BindingKind,
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
            .sql("SELECT * FROM TEST_TABLE WHERE id = ? AND name = ?")
            .add_binding(69);

        assert_eq!(sql.query.bindings.len(), 1);
        let sql = sql.add_binding("JoMama");

        assert_eq!(sql.query.bindings.len(), 2);

        Ok(())
    }
}
