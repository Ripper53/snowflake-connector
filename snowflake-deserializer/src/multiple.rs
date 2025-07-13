use std::collections::HashSet;
use std::num::NonZeroUsize;

use crate::data_manipulation::DataManipulationResult;
use crate::{
    SnowflakeDeserialize, SnowflakeExecutor, SnowflakeSQLResponse, SnowflakeSQLResult,
    SnowflakeSQLTextError, StatementHandle,
};

impl<'a, D: ToString> SnowflakeExecutor<'a, D> {
    /// [Multiple statements API](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-multiple-statements)
    pub fn multiple_statements(self) -> MultipleSnowflakeSQL<'a, D> {
        MultipleSnowflakeSQL {
            client: self.client,
            data: MultipleSnowflakeSQLData {
                database: self.database,
                host: self.host,
                statement: Vec::new(),
                additional_statements_count: 0,
                uuid: uuid::Uuid::new_v4(),
            },
        }
    }
}

#[derive(Debug)]
pub struct MultipleSnowflakeSQL<'a, D: ToString> {
    client: &'a reqwest::Client,
    data: MultipleSnowflakeSQLData<'a, D>,
}
#[derive(Debug)]
struct MultipleSnowflakeSQLData<'a, D> {
    database: D,
    host: &'a str,
    statement: Vec<&'a str>,
    additional_statements_count: usize,
    uuid: uuid::Uuid,
}

impl<'a, D: ToString> MultipleSnowflakeSQL<'a, D> {
    /// Add a **single** SQL statement.
    /// Use [add_multiple_sql](Self::add_multiple_sql) to add multiple SQL statement at once.
    ///
    /// SQL statement **must** end with a semicolon (;)
    pub fn add_sql(&mut self, sql: &'a str) {
        self.data.add_sql(sql)
    }
    /// Add **multiple** SQL statements, and you must specify how many there in `count`.
    /// Use [add_sql](Self::add_sql) to add a single SQL statement at a time.
    ///
    /// Each SQL statement **must** end with a semicolon (;)
    pub fn add_multiple_sql(&mut self, count: NonZeroUsize, sql: &'a str) {
        self.data.add_multiple_sql(count, sql)
    }
    pub fn finish(self) -> MultipleSnowflakeExecutorSQLJSON<'a> {
        MultipleSnowflakeExecutorSQLJSON {
            client: self.client,
            data: self.data.finished(),
        }
    }
}

impl<'a, D: ToString> MultipleSnowflakeSQLData<'a, D> {
    fn add_sql(&mut self, sql: &'a str) {
        self.statement.push(sql);
    }
    fn add_multiple_sql(&mut self, count: NonZeroUsize, sql: &'a str) {
        self.additional_statements_count += count.get() - 1;
        self.statement.push(sql);
    }
    fn finished(self) -> MultipleSnowflakeExecutorSQLJSONData<'a> {
        MultipleSnowflakeExecutorSQLJSONData {
            statement: self.statement,
            additional_statements_count: self.additional_statements_count,
            host: self.host,
            uuid: self.uuid,
            timeout: None,
            database: self.database.to_string(),
            warehouse: None,
            role: None,
        }
    }
}

#[derive(Debug)]
pub struct MultipleSnowflakeExecutorSQLJSON<'a> {
    client: &'a reqwest::Client,
    data: MultipleSnowflakeExecutorSQLJSONData<'a>,
}
#[derive(Debug)]
struct MultipleSnowflakeExecutorSQLJSONData<'a> {
    statement: Vec<&'a str>,
    additional_statements_count: usize,
    host: &'a str,
    uuid: uuid::Uuid,
    timeout: Option<u32>,
    database: String,
    warehouse: Option<String>,
    role: Option<String>,
}

impl<'a> MultipleSnowflakeExecutorSQLJSON<'a> {
    pub async fn text(self) -> Result<String, SnowflakeSQLTextError> {
        let (statement, parameters) = self.get_statement();
        self.client
            .post(self.get_url())
            .json(&Request {
                statement: &statement,
                timeout: self.data.timeout,
                database: self.data.database,
                warehouse: self.data.warehouse,
                role: self.data.role,
                parameters,
            })
            .send()
            .await
            .map_err(SnowflakeSQLTextError::Request)?
            .text()
            .await
            .map_err(SnowflakeSQLTextError::ToText)
    }
    /// Run all queries.
    pub async fn run(self) -> Result<MultipleSnowflakeSQLResponse<'a>, MultipleSnowflakeSQLError> {
        let (statement, parameters) = self.get_statement();
        let response = self
            .client
            .post(self.get_url())
            .json(&Request {
                statement: &statement,
                timeout: self.data.timeout,
                database: self.data.database,
                warehouse: self.data.warehouse,
                role: self.data.role,
                parameters,
            })
            .send()
            .await
            .map_err(MultipleSnowflakeSQLError::Request)?
            .json::<MultipleSQLResponse>()
            .await
            .map_err(MultipleSnowflakeSQLError::Decode)?;
        Ok(MultipleSnowflakeSQLResponse {
            client: self.client,
            host: self.data.host,
            concatenated_statement: statement,
            response,
        })
    }
    fn get_url(&self) -> String {
        crate::get_url(self.data.host, &self.data.uuid)
    }
    fn get_statement(&self) -> (String, Parameters) {
        let statement = self.data.statement.join(" ");
        let statement_count = self.data.statement.len() + self.data.additional_statements_count;
        (statement, Parameters { statement_count })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MultipleSnowflakeSQLError {
    #[error(transparent)]
    Request(reqwest::Error),
    #[error(transparent)]
    Decode(reqwest::Error),
}

#[derive(serde::Serialize, Debug)]
struct Request<'a> {
    statement: &'a str,
    timeout: Option<u32>,
    database: String,
    warehouse: Option<String>,
    role: Option<String>,
    parameters: Parameters,
}

#[derive(serde::Serialize, Debug)]
struct Parameters {
    #[serde(rename = "MULTI_STATEMENT_COUNT")]
    statement_count: usize,
}

#[derive(Debug)]
pub struct MultipleSnowflakeSQLResponse<'a> {
    client: &'a reqwest::Client,
    host: &'a str,
    concatenated_statement: String,
    response: MultipleSQLResponse,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct MultipleSQLResponse {
    statement_handles: Vec<StatementHandle>,
}

impl<'a> MultipleSnowflakeSQLResponse<'a> {
    /// All the SQL statements concatenated into a single,
    /// which is what the Snowflake API expects.
    ///
    /// There may be errors when calling [complete](Self::complete) that reference this SQL statement,
    /// indicating the exact position within this SQL statement where the error occured.
    pub fn concatenated_statement(&self) -> &str {
        &self.concatenated_statement
    }
    /// Check if all statements are complete.
    /// Updates after calling [complete](Self::complete).
    pub fn all_complete(&self) -> bool {
        self.response.statement_handles.is_empty()
    }
    pub fn unfinished_statements(&self) -> &[StatementHandle] {
        &self.response.statement_handles
    }
    /// Retrieve the status of the processing statement,
    /// and its result if complete.
    pub async fn statement_status(
        &self,
        statement_handle: &StatementHandle,
    ) -> Result<StatementStatus, StatementError> {
        let response = self
            .client
            .get(format!(
                "{}statements/{}?nullable=false",
                self.host,
                statement_handle.handle()
            ))
            .send()
            .await;
        match response {
            Ok(response) => {
                let status = response.status();
                match status {
                    reqwest::StatusCode::OK => Ok(StatementStatus::Parse(Parse {
                        statement_handle: statement_handle.clone(),
                        response,
                    })),
                    reqwest::StatusCode::UNPROCESSABLE_ENTITY => {
                        let e = match response.json::<QueryFailureStatus>().await {
                            Ok(e) => StatementCompletionError::Query(e),
                            Err(e) => StatementCompletionError::Decode(e),
                        };
                        Err(e.into())
                    }
                    reqwest::StatusCode::ACCEPTED => Ok(StatementStatus::InProgress),
                    reqwest::StatusCode::TOO_MANY_REQUESTS
                    | reqwest::StatusCode::SERVICE_UNAVAILABLE
                    | reqwest::StatusCode::GATEWAY_TIMEOUT => {
                        Err(StatementError::TooManyRequests(status))
                    }
                    status => Err(StatementCompletionError::Unknown(status).into()),
                }
            }
            Err(e) => {
                if let Some(status) = e.status() {
                    Err(StatementCompletionError::Unknown(status).into())
                } else {
                    panic!();
                }
            }
        }
    }
    /// Returns completed queries.
    ///
    /// Use [Parse::statement_handle] to check which statement finished.
    /// Call this function again to retrieve anymore that have been completed.
    /// Check when [all_complete](Self::all_complete) is `true`, then there is no need to call this function anymore.
    pub async fn complete(
        &mut self,
    ) -> impl Iterator<Item = Result<Parse, StatementCompletionError>> {
        let mut to_remove_index = HashSet::new();
        let mut statements = Vec::new();
        for (i, statement_handle) in self.response.statement_handles.iter().enumerate() {
            match self.statement_status(statement_handle).await {
                Ok(status) => match status {
                    StatementStatus::Parse(parse) => {
                        to_remove_index.insert(i);
                        statements.push(Ok(parse));
                    }
                    StatementStatus::InProgress => {}
                },
                Err(e) => {
                    if let StatementError::TooManyRequests(_) = e {
                        continue;
                    }
                    let e = match e {
                        StatementError::Completion(completion) => completion,
                        StatementError::TooManyRequests(_) => {
                            // Not a breaking error,
                            // caller simply needs to call
                            // this function again at a later time.
                            continue;
                        }
                    };
                    to_remove_index.insert(i);
                    statements.push(Err(e));
                }
            }
        }
        let mut index = 0;
        self.response.statement_handles.retain(|_statement_handle| {
            let r = !to_remove_index.contains(&index);
            index += 1;
            r
        });
        statements.into_iter()
    }
}

#[derive(Debug)]
pub enum StatementStatus {
    InProgress,
    Parse(Parse),
}

#[derive(Debug)]
pub struct Parse {
    statement_handle: StatementHandle,
    response: reqwest::Response,
}

impl Parse {
    pub fn statement_handle(&self) -> &StatementHandle {
        &self.statement_handle
    }
    /// Use with `SELECT` queries.
    pub async fn selected<T: SnowflakeDeserialize>(
        self,
    ) -> Result<SnowflakeSQLResult<T>, ParseSelect<T>> {
        let r = self
            .response
            .json::<SnowflakeSQLResponse>()
            .await
            .map_err(ParseSelect::Decode)?
            .deserialize()
            .map_err(ParseSelect::Deserialize)?;
        Ok(r)
    }
    /// Use with `DELETE`, `INSERT`, `UPDATE` queries.
    pub async fn manipulated(self) -> Result<DataManipulationResult, reqwest::Error> {
        self.response.json().await
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseSelect<T: SnowflakeDeserialize> {
    #[error(transparent)]
    Decode(reqwest::Error),
    #[error(transparent)]
    Deserialize(T::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum StatementError {
    #[error(transparent)]
    Completion(#[from] StatementCompletionError),
    #[error("too many requests with status code: {0}, try again shortly")]
    TooManyRequests(reqwest::StatusCode),
}
#[derive(thiserror::Error, Debug)]
pub enum StatementCompletionError {
    #[error(transparent)]
    Decode(reqwest::Error),
    #[error(transparent)]
    Query(#[from] QueryFailureStatus),
    #[error("unknown error occurred with status code: {0}")]
    Unknown(reqwest::StatusCode),
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
    created_on: i64,
    statement_status_url: String,
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
    pub fn created_on(&self) -> i64 {
        self.created_on
    }
    pub fn statement_status_url(&self) -> &str {
        &self.statement_status_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_insertion() {
        let mut data = create_data();
        data.add_sql("SELECT * FROM TEST_TABLE;");
        let data = data.finished();
        assert_eq!(1, data.statement.len());
        assert_eq!(0, data.additional_statements_count);
    }

    #[test]
    fn multiple_sql_insertion() {
        let mut data = create_data();
        data.add_multiple_sql(
            NonZeroUsize::new(2).unwrap(),
            "SELECT * FROM TEST_TABLE; SELECT * FROM TEST_TABLE;",
        );
        let data = data.finished();
        assert_eq!(2, data.statement.len() + data.additional_statements_count);
    }

    // UTILITY FUNCTIONS BELOW //

    fn create_data<'a>() -> MultipleSnowflakeSQLData<'a, &'static str> {
        MultipleSnowflakeSQLData {
            database: "TEST_DB",
            host: "TEST_HOST",
            statement: Vec::new(),
            additional_statements_count: 0,
            uuid: uuid::Uuid::nil(),
        }
    }
}
