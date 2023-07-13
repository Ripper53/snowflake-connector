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
    pub data: Vec<Vec<String>>,
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
    type Err;
    fn deserialize_from_str(s: &str) -> Result<Self, Self::Err>
    where
        Self: Sized;
}

impl DeserializeFromStr for bool {
    type Err = anyhow::Error;
    fn deserialize_from_str(s: &str) -> Result<Self, Self::Err> {
        match bool::from_str(s) {
            Ok(v) => Ok(v),
            Err(_) => Ok(s != "0"),
        }
    }
}

macro_rules! impl_deserialize_from_str {
    ($ty: ty) => {
        impl DeserializeFromStr for $ty {
            type Err = <$ty as FromStr>::Err;
            fn deserialize_from_str(s: &str) -> Result<Self, Self::Err> {
                <$ty>::from_str(s)
            }
        }
    };
}

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
