use serde::Deserialize;
use std::str::FromStr;

pub mod bindings;

pub use bindings::*;

pub trait SnowflakeDeserialize {
    fn snowflake_deserialize(
        response: SnowflakeSqlResponse,
    ) -> Result<SnowflakeSqlResult<Self>, anyhow::Error>
    where
        Self: Sized;
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SnowflakeSqlResponse {
    pub result_set_meta_data: MetaData,
    pub data: Vec<Vec<Option<String>>>,
    pub code: String,
    pub statement_status_url: String,
    pub request_id: String,
    pub sql_state: String,
    pub message: String,
    //pub created_on: u64,
}

impl SnowflakeSqlResponse {
    pub fn deserialize<T: SnowflakeDeserialize>(
        self,
    ) -> Result<SnowflakeSqlResult<T>, anyhow::Error> {
        T::snowflake_deserialize(self)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetaData {
    pub num_rows: usize,
    pub format: String,
    pub row_type: Vec<RowType>,
}

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

#[derive(Debug)]
pub struct SnowflakeSqlResult<T> {
    pub data: Vec<T>,
}

/// For custom data parsing,
/// ex. you want to convert the retrieved data (strings) to enums.
///
/// Data in cells are not their type, they are simply strings that need to be converted.
pub trait DeserializeFromStr {
    fn deserialize_from_str(s: Option<&str>) -> Result<Self, anyhow::Error>
    where
        Self: Sized;
}

impl<T> DeserializeFromStr for T
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
{
    fn deserialize_from_str(s: Option<&str>) -> Result<Self, anyhow::Error> {
        match s.map(T::from_str).transpose() {
            Ok(None) => Err(anyhow::anyhow!("unexpected null for non-nullable value")),
            Ok(Some(b)) => Ok(b),
            Err(err) => Err(anyhow::anyhow!("parse error: {err:?}")),
        }
    }
}
