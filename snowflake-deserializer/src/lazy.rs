use std::collections::HashMap;

use crate::{MetaData, SnowflakeSQL};

impl<'a> SnowflakeSQL<'a> {
    /// Use with `SELECT` queries.
    ///
    /// Lazy selection, meaning this is not parsed into a struct,
    /// rather, when a value is needed, the parse occurs there.
    pub async fn lazy_select(self) -> Result<LazySnowflakeSQLResult, LazySnowflakeSQLSelectError> {
        let response = self
            .client
            .post(self.get_url())
            .json(&self.statement)
            .send()
            .await
            .map_err(LazySnowflakeSQLSelectError::Request)?;
        let status_code = response.status();
        let data = response
            .text()
            .await
            .map_err(LazySnowflakeSQLSelectError::Decode)?;
        Ok(LazySnowflakeSQLResult { status_code, data })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LazySnowflakeSQLSelectError {
    #[error(transparent)]
    Request(reqwest::Error),
    #[error(transparent)]
    Decode(reqwest::Error),
}

#[derive(Debug)]
pub struct LazySnowflakeSQLResult {
    status_code: reqwest::StatusCode,
    data: String,
}

impl LazySnowflakeSQLResult {
    pub fn parse_rows(self) -> Result<LazyRows, LazyParseRowError> {
        match self.status_code {
            reqwest::StatusCode::OK => {
                let rows: RowsData =
                    serde_json::from_str(&self.data).map_err(LazyParseRowError::Decode)?;
                let mut name_index_map = HashMap::with_capacity(rows.metadata.row_type.len());
                for (i, row_type) in rows.metadata.row_type.iter().enumerate() {
                    name_index_map.insert(row_type.name.clone(), i);
                }
                Ok(LazyRows {
                    rows,
                    name_index_map,
                })
            }
            reqwest::StatusCode::ACCEPTED => {
                todo!()
            }
            _ => todo!(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LazyParseRowError {
    #[error(transparent)]
    Decode(serde_json::Error),
    //#[error(transparent)]
    //Query(QueryFailureStatus),
    #[error("unknown error with status code {0}")]
    Unknown(reqwest::StatusCode),
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
}

#[derive(thiserror::Error, Debug)]
pub enum LazyRowParseError<'a> {
    #[error("unknown name {0}")]
    UnknownName(&'a str),
    #[error(transparent)]
    Deserialize(#[from] serde_json::Error),
}
