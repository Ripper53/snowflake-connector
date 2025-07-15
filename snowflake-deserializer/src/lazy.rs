use std::collections::HashMap;

use crate::{MetaData, QueryFailureStatus, QueryStatus, SnowflakeSQL};

impl<'a> SnowflakeSQL<'a> {
    /// Use with `SELECT` queries.
    ///
    /// Lazy selection, meaning this is not parsed into a struct,
    /// rather, when a value is needed, the parse occurs there.
    pub async fn lazy_select(
        self,
    ) -> Result<LazySnowflakeSQLResult<'a>, LazySnowflakeSQLSelectRequestError> {
        let response = self
            .client
            .post(self.get_url())
            .json(&self.statement)
            .send()
            .await
            .map_err(LazySnowflakeSQLSelectRequestError)?;
        Ok(LazySnowflakeSQLResult {
            client: self.client,
            host: self.host,
            response,
        })
    }
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct LazySnowflakeSQLSelectRequestError(reqwest::Error);
impl std::ops::Deref for LazySnowflakeSQLSelectRequestError {
    type Target = reqwest::Error;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl LazySnowflakeSQLSelectRequestError {
    pub fn take_error(self) -> reqwest::Error {
        self.0
    }
}

#[derive(Debug)]
pub struct LazySnowflakeSQLResult<'a> {
    client: &'a reqwest::Client,
    host: &'a str,
    response: reqwest::Response,
}

impl<'a> LazySnowflakeSQLResult<'a> {
    pub async fn parse_rows(self) -> Result<ParseRows<'a>, LazyParseRowError> {
        match self.response.status() {
            reqwest::StatusCode::OK => {
                let rows: RowsData = self
                    .response
                    .json()
                    .await
                    .map_err(LazyParseRowError::Decode)?;
                let mut name_index_map = HashMap::with_capacity(rows.metadata.row_type.len());
                for (i, row_type) in rows.metadata.row_type.iter().enumerate() {
                    name_index_map.insert(row_type.name.clone(), i);
                }
                Ok(ParseRows::Parsed(LazyRows {
                    rows,
                    name_index_map,
                }))
            }
            reqwest::StatusCode::REQUEST_TIMEOUT | reqwest::StatusCode::ACCEPTED => {
                let response: QueryStatus = self
                    .response
                    .json()
                    .await
                    .map_err(LazyParseRowError::Decode)?;
                Ok(ParseRows::Status(LazySnowflakeRetrySQLResult {
                    client: self.client,
                    host: self.host,
                    query_status: response,
                }))
            }
            reqwest::StatusCode::UNPROCESSABLE_ENTITY => {
                let e = match self.response.json::<QueryFailureStatus>().await {
                    Ok(e) => LazyParseRowError::Query(e),
                    Err(e) => LazyParseRowError::Decode(e),
                };
                Err(e)
            }
            status => Err(LazyParseRowError::Unknown(status)),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LazyParseRowError {
    #[error(transparent)]
    Decode(reqwest::Error),
    #[error(transparent)]
    Query(#[from] QueryFailureStatus),
    #[error("unknown error with status code {0}")]
    Unknown(reqwest::StatusCode),
}

#[derive(Debug)]
pub struct LazySnowflakeRetrySQLResult<'a> {
    client: &'a reqwest::Client,
    host: &'a str,
    query_status: QueryStatus,
}
impl<'a> LazySnowflakeRetrySQLResult<'a> {
    pub async fn retry(
        self,
    ) -> Result<LazySnowflakeSQLResult<'a>, LazySnowflakeSQLRetryRequestError> {
        let response = self
            .client
            .post(format!(
                "{}statements/{}?nullable=false",
                self.host, self.query_status.statement_handle,
            ))
            .send()
            .await
            .map_err(LazySnowflakeSQLRetryRequestError)?;
        Ok(LazySnowflakeSQLResult {
            client: self.client,
            host: self.host,
            response,
        })
    }
    pub fn status(&self) -> &QueryStatus {
        &self.query_status
    }
}
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct LazySnowflakeSQLRetryRequestError(reqwest::Error);
#[derive(Debug)]
pub enum ParseRows<'a> {
    Status(LazySnowflakeRetrySQLResult<'a>),
    Parsed(LazyRows),
}
#[derive(Debug)]
pub struct LazyRows {
    rows: RowsData,
    name_index_map: HashMap<String, usize>,
}
#[derive(serde::Deserialize, Debug)]
struct RowsData {
    #[serde(rename = "resultSetMetaData")]
    metadata: MetaData,
    data: Vec<Vec<String>>,
}

impl LazyRows {
    pub fn at(&self, index: usize) -> Option<LazyRow> {
        if let Some(data) = self.rows.data.get(index) {
            let row = LazyRow {
                name_index_map: &self.name_index_map,
                data,
            };
            Some(row)
        } else {
            None
        }
    }
    pub fn get_index_of_column(&self, column_name: &str) -> Option<usize> {
        self.name_index_map.get(column_name).map(|index| *index)
    }
}

#[derive(Debug)]
pub struct LazyRow<'a> {
    name_index_map: &'a HashMap<String, usize>,
    data: &'a Vec<String>,
}

impl<'a> LazyRow<'a> {
    pub fn get<'de, T: serde::Deserialize<'de>>(
        &'de self,
        column_name: &'de str,
    ) -> Result<T, LazyRowParseError<'de>> {
        if let Some(index) = self.name_index_map.get(column_name) {
            let s = &self.data[*index];
            Ok(serde_json::from_str(s)?)
        } else {
            Err(LazyRowParseError::UnknownName(column_name))
        }
    }
    pub fn get_from_index<'de, T: serde::Deserialize<'de>>(
        &'de self,
        column_index: usize,
    ) -> Result<T, LazyRowIndexParseError> {
        if let Some(value) = self.data.get(column_index) {
            Ok(serde_json::from_str(value)?)
        } else {
            Err(LazyRowIndexParseError::InvalidIndex(column_index))
        }
    }
    pub fn get_index_of_column(&self, column_name: &str) -> Option<usize> {
        self.name_index_map.get(column_name).map(|index| *index)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LazyRowParseError<'a> {
    #[error("unknown name {0}")]
    UnknownName(&'a str),
    #[error(transparent)]
    Deserialize(#[from] serde_json::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum LazyRowIndexParseError {
    #[error("invalid index {0}")]
    InvalidIndex(usize),
    #[error(transparent)]
    Deserialize(#[from] serde_json::Error),
}
