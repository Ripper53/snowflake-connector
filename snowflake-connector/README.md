# Snowflake Connector
**Who is this crate for?**
Developers who want to deserialize data into types and auto-generate tables into Rust structs from Snowflake. This crate focuses on type safety and encourages you to handle errors.

Use of [RustRover](https://www.jetbrains.com/rust/) is HIGHLY encouraged when using `derive` feature (enabled by default), otherwise false positive `proc_macro` errors may occur when using VS Code or other code editors, but builds will work fine.

# Usage
Add following line to `Cargo.toml`:

```toml
snowflake-connector = "0.3"
```

Right now, only [key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) is supported.

You can pass the paths to your private and public key using `SnowflakeConnector::try_new_from_file`, or pass them directly using `SnowflakeConnector::try_new`.

## Dev Setup
Add your public and private key under a folder, and feed the paths into `SnowflakeConnector::try_new_from_file`.

**Make sure to ignore the keys. You do not want to commit your keys to a repository.**

## Derive Features
`snowflake-connector` will attempt to auto-generate table structs for you. There are two requirements:
1. `SNOWFLAKE_PATH` environment variable must be defined
2. `snowflake_config.toml` must reside in the folder `SNOWFLAKE_PATH` points to

If `SNOWFLAKE_PATH` is not defined, the auto-generation of tables is simply skipped. This can be useful for environments that will build the crate, and should not regenerate tables.

Example `snowflake_config.toml`:
```toml
private_key_path = "./keys/local/rsa_key.p8"
public_key_path = "./keys/local/rsa_key.pub"
host = "FIRST-LAST"
account = "FIRST-LAST"
user = "USER"
role = "PUBLIC"
warehouse = "SNOWFLAKE_LEARNING_WH"

# First database we want to load tables from
[[databases]]
name = "SNOWFLAKE_LEARNING_DB" # Database name
# Tables from first database
[[databases.tables]]
name = "USER_SAMPLE_DATA_FROM_S3.MENU" # Schema.Table name
# By default, all numbers are signed, below marks certain columns as unsigned
unsigned = ["menu_id", "menu_type_id", "menu_item_id"]
# Custom struct below that will be parsed from json
[databases.tables.json]
menu_item_health_metrics_obj = "crate::snowflake::metrics::Metrics" # Full path to struct (must implement `serde::Deserialize`)
# Custom enums for columns below, array contains all the possible values for said column,
# each array element generates an enum variant
[databases.tables.enums]
menu_type = [
    "Variant_1", # MenuType::Variant1
    "Variant 2", # MenuType::Variant2
    "VARIANT 3", # MenuType::Variant3
    "Variant4",  # MenuType::Variant4
]

# Second database we want to load tables from
[[databases]]
name = "SNOWFLAKE_SAMPLE_DATA" # Database name
[[databases.tables]]
name = "TPCH_SF1.ORDERS" # Schema.Table name
```
This will create a `snowflake_tables.rs` file under the `SNOWFLAKE_PATH` folder which will contain two tables:
1. `snowflake_learning_db::Menu`
2. `snowflake_sample_data::Orders`

There are two ways to regenerate Snowflake tables:
1. `touch` or modify the `snowflake_config.toml` file
2. or run `cargo clean` and then `cargo build` to force rebuild dependencies

**BE WARY OF AUTO-GENERATED CODE DURING CODE REVIEWS.** Someone malicious may inject their own code into the auto-generated file. If you are someone trusted, regenerating the tables on your end and committing them into the branch is wise, or better yet, set up an automated process.

**Generating tables will send a query to Snowflake for every table to retrieve its metadata.**

## How it Works
Below example is not tested, but you get the gist:
```rust
use snowflake_connector::*;

fn get_from_snowflake() -> Result<SnowflakeSQLResult<Test>, SnowflakeError> {
    let connector = SnowflakeConnector::try_new_from_file(
        "PUBLIC/KEY/PATH",
        "PRIVATE/KEY/PATH",
        "COMPANY.ACCOUNT",
        "ACCOUNT",
        "USER@EXAMPLE.COM",
    )?;
    Ok(connector
        .execute("DB")
        .sql("SELECT * FROM TEST_TABLE WHERE id = ? LIMIT 69")
        .with_warehouse("WH")
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

// Manually creating tables instead of auto-generating.
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

// Manually creating table with more control
#[derive(SnowflakeDeserialize)]
#[snowflake_deserialize_error(FineControlDeserializeError)] // Specify which error to use
struct FineControlTable {
    // Error to return if this field fails to parse,
    // note `error` parameter is provided by macro
    #[snowflake(error = FineControlDeserializeError::CatFailedConversion { error })]
    cat: Option<usize>,
    #[snowflake(error = FineControlDeserializeError::DogFailedConversion { error })]
    dog: usize,
}

pub enum FineControlDeserializeError {
    CatFailedConversion { error: std::num::ParseIntError },
    DogFailedConversion { error: std::num::ParseIntError },
}
```
Snowflake returns every value as a string. Implement `DeserializeFromStr` for types that can be parsed from a string. Add the `SnowflakeDeserialize` derive attribute to a `struct` to allow `SnowflakeConnector` to convert the data to that type. As of now, the order of the fields must correspond to the order of the columns. Let's assume the fields go top-to-bottom, so the top-most field must be the first column, the bottom-most field must be the last column, otherwise deserializing will fail.

It is encouraged to make your crate as type safe as you can. Instead of using strings for your warehouses or databases, consider creating `enum`s that implement `ToString`, and use them instead of strings. This way, you know which warehouses or databases are available, can change their string representation in one place, and remove any obsolete values from your codebase entirely.
