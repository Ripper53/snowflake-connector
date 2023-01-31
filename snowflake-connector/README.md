# Snowflake Connector
**Under heavy development.** Might be wise to point to git instead of crates.io for now.

# Usage
Add following line to Cargo.toml:

```toml
snowflake-connector = { version = "0.2", features = ["derive"] }
```

Right now, only [key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) is supported.

You must have the text files `environment_variables/local/snowflake_private_key_path.txt` and `environment_variables/local/snowflake_public_key_path.txt`, where your application is executed, and these files must store the path to the keys.

## Dev Setup
Add your public and private key under `environment_variables/local` folder (you will have to create the `local` folder). Make sure your private key is named `rsa_key.p8` and your public key is `rsa_key.pub`.

Point `environment_variables/local/snowflake_private_key_path.txt` to your private key by changing its contents to: `./environment_variables/local/rsa_key.p8`

Point `environment_variables/local/snowflake_public_key_path.txt` to your public key: `./environment_variables/local/rsa_key.pub`

**Make sure to ignore the `environment_variables` directory. You do not want to commit your keys to a repository.**

## How it Works
Below example is not tested, but you get the gist:
```rust
use snowflake_connector::{*, errors::SnowflakeError};

fn get_from_snowflake() -> Result<SnowflakeSQLResult<Test>, SnowflakeError> {
    let connector = SnowflakeConnector::try_new("COMPANY.ACCOUNT", "ACCOUNT", "USER@EXAMPLE.COM")?;
    connector
        .execute("DB", "WH")
        .sql("SELECT * FROM TEST_TABLE WHERE id = ? LIMIT 69")?
        .add_binding(420)
        .select::<Test>().await
}

fn main() {
    if let Ok(data) = get_from_snowflake() {
        println!("{:#?}", data)
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
