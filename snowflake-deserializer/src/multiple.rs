use std::num::NonZeroUsize;

use crate::{SnowflakeExecutor, SnowflakeSQLTextError};

impl<'a, D: ToString> SnowflakeExecutor<'a, D> {
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
        let statement = self.data.statement.join(" ");
        self.client
            .post(self.get_url())
            .json(&Request {
                statement: &statement,
                timeout: self.data.timeout,
                database: self.data.database,
                warehouse: self.data.warehouse,
                role: self.data.role,
                parameters: Parameters {
                    statement_count: self.data.statement.len()
                        + self.data.additional_statements_count,
                },
            })
            .send()
            .await
            .map_err(SnowflakeSQLTextError::Request)?
            .text()
            .await
            .map_err(SnowflakeSQLTextError::ToText)
    }
    fn get_url(&self) -> String {
        crate::get_url(self.data.host, &self.data.uuid)
    }
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
