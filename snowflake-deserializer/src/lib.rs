pub use chrono;
use data_manipulation::DataManipulationResult;
use jwt::{KeyPairError, TokenFromFileError};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, USER_AGENT};
use serde::{Deserialize, Serialize};
pub use serde_json;
use std::{collections::HashMap, path::Path, str::FromStr};

use crate::bindings::{BindingType, BindingValue};

pub mod bindings;
pub mod data_manipulation;
#[cfg(feature = "insert")]
pub mod insert;
#[cfg(feature = "lazy")]
pub mod lazy;
#[cfg(feature = "multiple")]
pub mod multiple;

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

/// Error creating a new [SnowflakeConnector]
#[derive(thiserror::Error, Debug)]
pub enum NewSnowflakeConnectorError {
    #[error(transparent)]
    KeyPair(#[from] KeyPairError),
    #[error(transparent)]
    ClientBuildError(#[from] reqwest::Error),
}

/// Error creating a new [SnowflakeConnector] from key paths
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
    /// Generic types:
    /// - [SnowflakeSQLStr] for reference strings
    /// - [SnowflakeSQLString] for owned strings
    ///
    /// Look into [sql_ref](Self::sql_ref) and [sql_owned](Self::sql_owned)
    ///
    /// Example:
    /// - `sql::<SnowflakeSQLStr>("SELECT * FROM TEST.TABLE")`
    /// - `sql(SnowflakeSQLStr::from("SELECT * FROM TEST.TABLE"))`
    pub fn sql<Statement: SnowflakeStatement>(
        self,
        statement: impl Into<Statement>,
    ) -> SnowflakeSQL<'a, Statement> {
        SnowflakeSQL::new(
            self.client,
            self.host,
            SnowflakeExecutorSQLJSON::new(statement.into(), self.database.to_string()),
            uuid::Uuid::new_v4(),
        )
    }
    pub fn sql_ref(
        self,
        statement: impl Into<SnowflakeSQLStr<'a>>,
    ) -> SnowflakeSQL<'a, SnowflakeSQLStr<'a>> {
        self.sql(statement)
    }
    pub fn sql_owned(
        self,
        statement: impl Into<SnowflakeSQLString>,
    ) -> SnowflakeSQL<'a, SnowflakeSQLString> {
        self.sql(statement)
    }
}
#[derive(serde::Serialize, Debug)]
#[serde(transparent)]
pub struct SnowflakeSQLStr<'a>(&'a str);
impl<'a> From<&'a str> for SnowflakeSQLStr<'a> {
    fn from(value: &'a str) -> Self {
        SnowflakeSQLStr(value)
    }
}
#[derive(serde::Serialize, Debug)]
#[serde(transparent)]
pub struct SnowflakeSQLString(String);
impl<'a> From<String> for SnowflakeSQLString {
    fn from(value: String) -> Self {
        SnowflakeSQLString(value)
    }
}
pub trait SnowflakeStatement: serde::Serialize {
    fn statement(&self) -> &str;
}
impl<'a> SnowflakeStatement for SnowflakeSQLStr<'a> {
    fn statement(&self) -> &str {
        self.0
    }
}
impl SnowflakeStatement for SnowflakeSQLString {
    fn statement(&self) -> &str {
        &self.0
    }
}

#[derive(Debug)]
pub struct SnowflakeSQL<'a, Statement: SnowflakeStatement> {
    client: &'a reqwest::Client,
    host: &'a str,
    statement: SnowflakeExecutorSQLJSON<Statement>,
    uuid: uuid::Uuid,
}

impl<'a, Statement: SnowflakeStatement> SnowflakeSQL<'a, Statement> {
    pub(crate) fn new(
        client: &'a reqwest::Client,
        host: &'a str,
        statement: SnowflakeExecutorSQLJSON<Statement>,
        uuid: uuid::Uuid,
    ) -> Self {
        SnowflakeSQL {
            client,
            host,
            statement,
            uuid,
        }
    }
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
    ) -> Result<StatementResult<'a, T>, SnowflakeSQLSelectError<T::Error>> {
        let r = self
            .client
            .post(self.get_url())
            .json(&self.statement)
            .send()
            .await
            .map_err(SnowflakeSQLSelectError::Request)?;
        let status_code = r.status();
        match status_code {
            reqwest::StatusCode::OK => Ok(StatementResult::Result(
                r.json::<SnowflakeSQLResponse>()
                    .await
                    .map_err(SnowflakeSQLSelectError::Decode)?
                    .deserialize()
                    .map_err(SnowflakeSQLSelectError::Deserialize)?,
            )),
            reqwest::StatusCode::REQUEST_TIMEOUT | reqwest::StatusCode::ACCEPTED => {
                Ok(StatementResult::Status(SnowflakeQueryStatus {
                    client: self.client,
                    host: self.host,
                    query_status: r
                        .json::<QueryStatus>()
                        .await
                        .map_err(SnowflakeSQLSelectError::Decode)?,
                }))
            }
            reqwest::StatusCode::UNPROCESSABLE_ENTITY => Err(SnowflakeSQLSelectError::Query(
                r.json().await.map_err(SnowflakeSQLSelectError::Decode)?,
            )),
            status_code => Err(SnowflakeSQLSelectError::Unknown(status_code)),
        }
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
        get_url(self.host, &self.uuid)
    }
}

pub(crate) fn get_url(host: &str, uuid: &uuid::Uuid) -> String {
    // TODO: make another return type that allows retrying by calling same statement again with retry flag!
    format!("{host}statements?nullable=false&requestId={uuid}")
}

/// Error retrieving results of SQL statement as text
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub enum SnowflakeSQLTextError {
    Request(reqwest::Error),
    ToText(reqwest::Error),
}

/// Error retrieving results of SQL selection
#[derive(thiserror::Error, Debug)]
pub enum SnowflakeSQLSelectError<DeserializeError> {
    #[error(transparent)]
    Request(reqwest::Error),
    #[error(transparent)]
    Decode(reqwest::Error),
    #[error(transparent)]
    Deserialize(DeserializeError),
    #[error(transparent)]
    Query(QueryFailureStatus),
    #[error("unknown error with status code: {0}")]
    Unknown(reqwest::StatusCode),
}

/// Error retrieving results of SQL manipulation
#[derive(thiserror::Error, Debug)]
pub enum SnowflakeSQLManipulateError {
    #[error(transparent)]
    Request(reqwest::Error),
    #[error(transparent)]
    Decode(reqwest::Error),
}

#[derive(Serialize, Debug)]
pub struct SnowflakeExecutorSQLJSON<Statement: SnowflakeStatement> {
    statement: Statement,
    timeout: Option<u32>,
    database: String,
    warehouse: Option<String>,
    role: Option<String>,
    bindings: Option<HashMap<String, Binding>>,
}
impl<Statement: SnowflakeStatement> SnowflakeExecutorSQLJSON<Statement> {
    pub(crate) fn new(statement: Statement, database: String) -> Self {
        SnowflakeExecutorSQLJSON {
            statement,
            timeout: None,
            database,
            warehouse: None,
            role: None,
            bindings: None,
        }
    }
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

/// [ResultSetMetaData](https://docs.snowflake.com/en/developer-guide/sql-api/reference#label-sql-api-reference-resultset-resultsetmetadata)
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetaData {
    pub num_rows: usize,
    pub format: String,
    pub row_type: Vec<RowType>,
}

/// [RowType](https://docs.snowflake.com/en/developer-guide/sql-api/reference#label-sql-api-reference-resultset-resultsetmetadata-rowtype)
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

/// Whether the query is running or finished
#[derive(Debug)]
pub enum StatementResult<'a, T> {
    /// Query still in progress...
    Status(SnowflakeQueryStatus<'a>),
    /// Query finished!
    Result(SnowflakeSQLResult<T>),
}
#[derive(Debug)]
pub struct SnowflakeSQLResult<T> {
    pub data: Vec<T>,
}

#[derive(Debug)]
pub struct SnowflakeQueryStatus<'a> {
    client: &'a reqwest::Client,
    host: &'a str,
    query_status: QueryStatus,
}

impl<'a> SnowflakeQueryStatus<'a> {
    pub fn take_query_status(self) -> QueryStatus {
        self.query_status
    }
    pub async fn cancel(&self) -> Result<(), QueryCancelError> {
        let url = format!(
            "{}statements/{}/cancel",
            self.host, self.query_status.statement_handle
        );
        let response = self.client.post(url).send().await;
        match response {
            Ok(r) => match r.status() {
                reqwest::StatusCode::OK => Ok(()),
                status => Err(QueryCancelError::Unknown(status)),
            },
            Err(e) => Err(QueryCancelError::Request(e)),
        }
    }
}

/// Error canceling a query
#[derive(thiserror::Error, Debug)]
pub enum QueryCancelError {
    #[error(transparent)]
    Request(reqwest::Error),
    #[error("unknown error with status code: {0}")]
    Unknown(reqwest::StatusCode),
}

/// A unique tag that identifies a SQL statement request
#[derive(serde::Deserialize, Clone, Debug)]
#[serde(transparent)]
pub struct StatementHandle(String);
impl StatementHandle {
    pub fn handle(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Display for StatementHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// [QueryStatus](https://docs.snowflake.com/en/developer-guide/sql-api/reference#label-sql-api-reference-querystatus)
#[derive(serde::Deserialize, thiserror::Error, Debug)]
#[serde(rename_all = "camelCase")]
#[error("Error for statement {statement_handle}: {message}")]
pub struct QueryStatus {
    code: String,
    sql_state: String,
    message: String,
    statement_handle: StatementHandle,
    created_on: i64,
    statement_status_url: String,
}

impl QueryStatus {
    pub fn code(&self) -> &str {
        &self.code
    }
    pub fn sql_state(&self) -> &str {
        &self.sql_state
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn statement_handle(&self) -> &StatementHandle {
        &self.statement_handle
    }
    pub fn created_on(&self) -> i64 {
        self.created_on
    }
    pub fn statement_status_url(&self) -> &str {
        &self.statement_status_url
    }
}

/// [QueryFailureStatus](https://docs.snowflake.com/en/developer-guide/sql-api/reference#label-sql-api-reference-queryfailurestatus)
#[derive(serde::Deserialize, thiserror::Error, Debug)]
#[serde(rename_all = "camelCase")]
#[error("Error for statement {statement_handle}: {message}")]
pub struct QueryFailureStatus {
    code: String,
    sql_state: String,
    message: String,
    statement_handle: StatementHandle,
    created_on: Option<i64>,
    statement_status_url: Option<String>,
}

impl QueryFailureStatus {
    pub fn code(&self) -> &str {
        &self.code
    }
    pub fn sql_state(&self) -> &str {
        &self.sql_state
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn statement_handle(&self) -> &StatementHandle {
        &self.statement_handle
    }
    pub fn created_on(&self) -> Option<i64> {
        self.created_on
    }
    pub fn statement_status_url(&self) -> Option<&str> {
        self.statement_status_url.as_deref()
    }
}

/// For custom data parsing,
/// ex. you want to convert the retrieved data (strings) to enums
///
/// Data in cells are not their type, they are simply strings that need to be converted.
pub trait DeserializeFromStr {
    type Error;
    fn deserialize_from_str(value: &str) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

impl DeserializeFromStr for chrono::NaiveDate {
    type Error = chrono::ParseError;

    fn deserialize_from_str(s: &str) -> Result<Self, Self::Error> {
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
    }
}

impl DeserializeFromStr for chrono::NaiveDateTime {
    type Error = chrono::ParseError;

    fn deserialize_from_str(s: &str) -> Result<Self, Self::Error> {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
    }
}

impl DeserializeFromStr for chrono::DateTime<chrono::Utc> {
    type Error = chrono::ParseError;

    fn deserialize_from_str(value: &str) -> Result<Self, Self::Error> {
        // Parse any ISO 8601 / RFC3339 style string and convert to UTC
        chrono::DateTime::parse_from_rfc3339(value).map(|dt| dt.with_timezone(&chrono::Utc))
    }
}

impl DeserializeFromStr for chrono::DateTime<chrono::FixedOffset> {
    type Error = chrono::ParseError;

    fn deserialize_from_str(value: &str) -> Result<Self, Self::Error> {
        chrono::DateTime::parse_from_rfc3339(value)
    }
}

impl<T: DeserializeFromStr> DeserializeFromStr for Option<T> {
    type Error = <T as DeserializeFromStr>::Error;
    fn deserialize_from_str(value: &str) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        if value == "NULL" {
            Ok(None)
        } else {
            <T as DeserializeFromStr>::deserialize_from_str(value).map(|f| Some(f))
        }
    }
}
macro_rules! impl_deserialize_from_str {
    ($ty: ty) => {
        impl DeserializeFromStr for $ty {
            type Error = <$ty as FromStr>::Err;
            fn deserialize_from_str(value: &str) -> Result<Self, Self::Error> {
                <$ty>::from_str(value)
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
