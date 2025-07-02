# Usage
Add following line to Cargo.toml:

```toml
snowflake-connector = { version = "0.2", features = ["derive"] }
```

Right now, only [key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) is supported.

You can pass the paths to your private and public key using `SnowflakeConnector::try_new_from_file`, or pass them directly using `SnowflakeConnector::try_new`.

## Dev Setup
Add your public and private key under a folder, and feed the paths into `SnowflakeConnector`.

**Make sure to ignore the keys. You do not want to commit your keys to a repository.**

## How it Works
Below example is not tested, but you get the gist:
```rust
use snowflake_connector::*;

fn get_from_snowflake() -> Result<SnowflakeSQLResult<Test>, SnowflakeSQLSelectError> {
    let connector = SnowflakeConnector::try_new_from_file(
        "PUBLIC/KEY/PATH",
        "PRIVATE/KEY/PATH",
        "COMPANY.ACCOUNT",
        "ACCOUNT",
        "USER@EXAMPLE.COM",
    )?;
    Ok(connector
        .execute("DB", "WH")
        .sql("SELECT * FROM TEST_TABLE WHERE id = ? LIMIT 69")
        .add_binding(420)
        .select::<Test>().await?)
}

#[derive(thiserror::Error, Debug)]
pub enum SnowflakeError {
    #[error(transparent)]
    New(#[from] NewSnowflakeConnectorFromFileError),
    #[error(transparent)]
    Select(#[from] SnowflakeSQLSelectError),
}

fn main() {
    if let Ok(data) = get_from_snowflake() {
        println!("{:#?}", data);
    } else {
        panic!("Failed to retrieve data from snowflake!");
    }
}

// Fields must be in order of columns!
#[derive(SnowflakeDeserialize, Debug)]
pub struct Test {
    pub id: u32,
    pub value1: bool,
    pub value2: String,
    pub value3: SomeEnumValue,
}

// Enum must implement DeserializeFromStr!
#[derive(Debug)]
pub enum SomeEnumValue {
    Value1,
    Value2,
}

// Snowflake sends each cell as a string,
// convert the string to the appropriate type!
impl DeserializeFromStr for SomeEnumValue {
    type Err = anyhow::Error;
    fn deserialize_from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "VALUE1" => Ok(SomeEnumValue::Value1),
            "VALUE2" => Ok(SomeEnumValue::Value2),
            _ => Err(anyhow::anyhow!("Failed to convert string to SomeEnumValue")),
        }
    }
}
```
Snowflake returns every value as a string. Implement `DeserializeFromStr` for types that can be parsed from a string. Add the `SnowflakeDeserialize` derive attribute to a `struct` to allow `SnowflakeConnector` to convert the data to that type. As of now, the order of the fields must correspond to the order of the columns. Let's assume the fields go top-to-bottom, so the top-most field must be the first column, the bottom-most field must be the last column, otherwise deserializing will fail.
