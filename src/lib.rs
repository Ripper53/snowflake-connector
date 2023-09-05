use anyhow::{bail, Context as _};
use data_manipulation::DataManipulationResult;
use reqwest::header::{HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use snowflake_derive::*;
pub use snowflake_deserialize::*;

pub mod data_manipulation;
mod jwt;

type Result<T> = anyhow::Result<T>;

#[derive(Debug)]
pub struct SnowflakeConnector {
    host: String,
    pub token: String,
    client: reqwest::Client,
}

pub struct PrivateKey(pub String);
pub struct PublicKey(pub String);

impl SnowflakeConnector {
    pub fn try_new(
        private_key: PrivateKey,
        public_key: PublicKey,
        host: &str,
        account_identifier: &str,
        user: &str,
    ) -> Result<Self> {
        let token = jwt::create_token(
            public_key,
            private_key,
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
            .gzip(true)
            .build()?;

        Ok(SnowflakeConnector {
            host: format!("https://{host}.snowflakecomputing.com/api/v2/"),
            token,
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

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Partition {
    data: Vec<Vec<Option<String>>>,
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

        let mut result = match serde_json::from_slice::<SnowflakeSqlResponse>(&bs) {
            Ok(deserialized) => deserialized,
            Err(err) => {
                let body_as_text = String::from_utf8_lossy(&bs);
                bail!("Error parsing result as SnowflakeSqlResponse: {err}. Body:\n{body_as_text}")
            }
        };

        if result.has_partitions() {
            self.fetch_and_merge_partitions(&mut result).await?;
        }

        result.deserialize()
    }

    async fn fetch_and_merge_partitions(&self, result: &mut SnowflakeSqlResponse) -> Result<()> {
        for index in 1..result.result_set_meta_data.partition_info.len() {

            println!("Getting partition {index}");
            let url = self.get_partition_url(&result.statement_handle, index);

            let res = self
                .client
                .get(url)
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

            let partition = match serde_json::from_slice::<Partition>(&bs) {
                Ok(deserialized) => deserialized,
                Err(err) => {
                    let body_as_text = String::from_utf8_lossy(&bs);
                    bail!("Error parsing result as SnowflakeSqlResponse: {err}. Body:\n{body_as_text}")
                }
            };

            result.data.extend(partition.data);
        }

        Ok(())
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
        format!("{}statements?requestId={}", self.host, self.uuid)
    }

    fn get_partition_url(&self, request_handle: &str, index: usize) -> String {
        format!("{}statements/{request_handle}?partition={}", self.host, index)
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

    mod test_derive {
        use super::*;
        use crate as snowflake_connector;

        #[derive(Debug, SnowflakeDeserialize)]
        struct SnowyRow {
            client_id: String,
            client_name: String,
            site_id: String,
            site_name: String,
            num_permits: Option<i64>,
        }

        #[test]
        fn deserialize_example() {
            let response = serde_json::from_str::<SnowflakeSqlResponse>(EXAMPLE)
                .expect("deserializing response");

            let sql_res = response
                .deserialize::<SnowyRow>()
                .expect("deserializing snowy rows");
        }

        static EXAMPLE: &str = r#"
{
  "resultSetMetaData": {
    "numRows": 2,
    "format": "jsonv2",
    "partitionInfo": [
      {
        "rowCount": 2,
        "uncompressedSize": 201
      }
    ],
    "rowType": [
      {
        "name": "CLIENT_ID",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": null,
        "type": "fixed",
        "scale": 0,
        "precision": 38,
        "nullable": false,
        "collation": null,
        "length": null
      },
      {
        "name": "CLIENT_NAME",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": 16777216,
        "type": "text",
        "scale": null,
        "precision": null,
        "nullable": true,
        "collation": null,
        "length": 16777216
      },
      {
        "name": "SITE_ID",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": 144,
        "type": "text",
        "scale": null,
        "precision": null,
        "nullable": true,
        "collation": null,
        "length": 36
      },
      {
        "name": "SITE_NAME",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": 16777216,
        "type": "text",
        "scale": null,
        "precision": null,
        "nullable": true,
        "collation": null,
        "length": 16777216
      },
      {
        "name": "NUM_PERMITS",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": null,
        "type": "fixed",
        "scale": 0,
        "precision": 38,
        "nullable": true,
        "collation": null,
        "length": null
      }
    ]
  },
  "data": [
    [
      "3",
      "Parkando",
      "7a7cb2b5-8f4b-4f49-9875-32576d808de2",
      "Grev Turegatan 29 - TCO-garaget",
      null
    ],
    [
      "3",
      "Parkando",
      "7a7cb2b5-8f4b-4f49-9875-32576d808de2",
      "Grev Turegatan 29 - TCO-garaget",
      null
    ]
  ],
  "code": "090001",
  "statementStatusUrl": "/api/v2/statements/01ad9ea3-3201-dca3-0000-a219000bb062?requestId=0a404baa-8f14-45f1-894c-a4f8ab7ca9de",
  "requestId": "0a404baa-8f14-45f1-894c-a4f8ab7ca9de",
  "sqlState": "00000",
  "statementHandle": "01ad9ea3-3201-dca3-0000-a219000bb062",
  "message": "Statement executed successfully.",
  "createdOn": 1689333321982
}

"#;
    }
}
