//! ## Snowflake Connector
//! Snowflake Connector allows connecting to [Snowflake](https://www.snowflake.com/) using its [SQL API](https://docs.snowflake.com/en/developer-guide/sql-api/index).
//!
//!
//! ### Auto-Generating Tables
//! Snowflake Connector allows you to generate Snowflake tables by connecting to your database and reading its metadata.
//! The `SNOWFLAKE_PATH` environment variable must be defined for auto-generation of Snowflake tables to work.
//! Simply define the environment variable that points to a directory in your repository, and create a `snowflake_config.toml` file within that directory.
//! Then, run `cargo build`.
//! The tables will be output into a `snowflake_tables.rs` file under the `SNOWFLAKE_PATH` directory.
//!
//! Here is an example `snowflake_config.toml` file:
//! ```toml
//! private_key_path = "PATH_TO_PRIVATE_KEY"
//! public_key_path = "PATH_TO_PUBLIC_KEY"
//! host = "FIRST-LAST"
//! account = "FIRST-LAST"
//! user = "RUST_CLIENT"
//! role = "ROLE"
//! warehouse = "WAREHOUSE"
//!
//! [[databases]]
//! name = "DATABASE_NAME"
//! [[databases.tables]]
//! name = "TABLE_SCHEMA.TABLE_NAME"
//!
//! # By default, numbers will be signed, if you want them unsigned,
//! # put the column names in the array below
//! unsigned = [
//!     "number_type_column_name_that_should_be_unsigned",
//!     "another_one",
//!     "another_another_one"
//! ]
//!
//! # If a column stores a json value, it can be deserialized into a custom struct
//! # by providing an exact path to the custom struct that implements `serde::Deserialize`
//! [databases.tables.json]
//! menu_item_health_metrics_obj = "crate::snowflake::metrics::Metrics"
//!
//! # If you want a column to deserialize into an enum
//! # you can define it like below
//! [databases.tables.enums]
//! menu_type = [ # `menu_type` will be deserialized into an enum from the values in the array, each string generates a unique variant
//!    "Ice Cream",
//!    "BBQ",
//!    "Tacos",
//!    "Mac & Cheese",
//!    "Ramen",
//!    "Grilled Cheese",
//!    "Vegatarian",
//!    "Crepes",
//!    "Ethiopian",
//!    "Hot Dogs",
//!    "Poutine",
//!    "Gyros",
//!    "Chinese",
//!    "Indian",
//!    "Sandwiches",
//! ]
//!
//! # Defining another table all over again, like above
//! [[databases]]
//! name = "ANOTHER_DATABASE_NAME"
//! [[databases.tables]]
//! name = "ANOTHER_SCHEMA.ANOTHER_TABLE_NAME"
//! ```
//! To regenerate tables, either `touch` or modify `snowflake_config.toml`,
//! or run `cargo clean`, then `cargo build` to force rebuild dependencies which rebuilds the tables.

// Features
#[cfg(feature = "derive")]
pub use snowflake_connector_derive::*;
pub use snowflake_deserializer::*;
