use heck::{ToSnakeCase, ToUpperCamelCase};
use quote::quote;
use std::{
    collections::HashMap,
    io::{BufRead, Read, Write},
};

#[tokio::main]
async fn main() {
    let snowflake_path = std::env::var("SNOWFLAKE_PATH")
        .expect("Failed to find SNOWFLAKE_PATH environment variable");
    let info_path = std::path::Path::new(&snowflake_path).join("snowflake-info.toml");
    let mut info_file = std::fs::OpenOptions::new()
        .read(true)
        .open(info_path)
        .expect("Failed reading `snowflake-info.toml` file");
    let mut s = String::new();
    {
        let last_modified = info_file
            .metadata()
            .expect("Failed to fetch metadata")
            .modified()
            .expect("Failed to get modified time")
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Failed to get epoch")
            .as_secs() as usize;
        let debug_path =
            std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("snowflake-out.toml");
        let mut debug_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(debug_path)
            .expect("Failed to create `snowflake-out.toml` file");
        debug_file.read_to_string(&mut s);
        if s.is_empty() {
            let snowflake_data = SnowflakeData { last_modified };
            debug_file.write_all(toml::to_string(&snowflake_data).unwrap().as_bytes());
        } else {
            let snowflake_data: SnowflakeData = toml::from_str(&s).unwrap();
            if snowflake_data.last_modified == last_modified {
                // Cached.
                return;
            }
        }
    }

    s.clear();
    info_file.read_to_string(&mut s);
    let file: FileContent = toml::de::from_str(&s).unwrap();
    let connector = snowflake_deserializer::SnowflakeConnector::try_new_from_file(
        &file.public_key_path,
        &file.private_key_path,
        &file.host,
        &file.account,
        &file.user,
    )
    .unwrap();
    let mut structs = Vec::new();
    for database in file.databases {
        for table in database.tables {
            let table_name = table.name;
            let sql = format!("SELECT * FROM {table_name} LIMIT 0");
            let mut sql = connector.execute(&database.name).sql(&sql);
            if let Some(ref role) = file.role {
                sql = sql.with_role(role);
            }
            if let Some(ref warehouse) = file.warehouse {
                sql = sql.with_warehouse(warehouse);
            }
            let value = sql.text().await.unwrap();
            let value: serde_json::Value =
                serde_json::from_str(&value).expect("Failed to parse Snowflake result");
            let metadata = value
                .get("resultSetMetaData")
                .expect(&format!("Could not find metadata in response: {}", value));
            let row_types = metadata
                .get("rowType")
                .unwrap()
                .as_array()
                .expect("Failed to find `rowType`");
            let mut names = Vec::with_capacity(row_types.len());
            let mut attributes = Vec::with_capacity(row_types.len());
            let mut types = Vec::with_capacity(row_types.len());
            let mut tables = Vec::new();
            for row_type in row_types {
                let name = row_type
                    .get("name")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_snake_case();
                names.push(syn::Ident::new(&name, proc_macro2::Span::call_site()));
                let nullable = row_type.get("nullable").unwrap().as_bool().unwrap();
                let ty = row_type.get("type").unwrap().as_str().unwrap();
                let ty = match ty {
                    "fixed" => {
                        let scale = row_type.get("scale").unwrap().as_u64().unwrap();
                        if scale == 0 {
                            if table.unsigned.contains(&name) {
                                quote!(usize)
                            } else {
                                quote!(isize)
                            }
                        } else {
                            quote!(f64)
                        }
                    }
                    "text" | "variant" => {
                        if let Some(value) = table.json_rows.get(&name) {
                            attributes.push(quote!(#[snowflake(json)]));
                            if value == "--auto" {
                                todo!("AUTOMATICALLY FIGURE OUT JSON TYPE");
                            } else {
                                let value: syn::Path = syn::parse_str(value).expect(&format!(
                                    "Failed to parse path for custom value: {}",
                                    value
                                ));
                                quote!(#value)
                            }
                        } else {
                            quote!(::std::string::String)
                        }
                    }
                    unknown_type => panic!("unhandled unknown type: {unknown_type}"),
                };
                if nullable {
                    types.push(quote!(::std::option::Option<#ty>));
                } else {
                    types.push(ty);
                }
                let table = row_type
                    .get("table")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_upper_camel_case();
                tables.push(syn::Ident::new(&table, proc_macro2::Span::call_site()));
                if names.len() != attributes.len() {
                    attributes.push(proc_macro2::TokenStream::new());
                }
            }
            tables.dedup();
            if tables.is_empty() {
                panic!("No tables found for query");
            } else if tables.len() == 1 {
                let table = tables.pop().unwrap();
                structs.push(quote! {
                    /// Auto-generated table from `snowflake-connector`
                    #[derive(::snowflake_connector::SnowflakeDeserialize, Debug)]
                    pub struct #table {
                        #(
                            #attributes
                            pub #names: #types,
                        )*
                    }
                });
            } else {
                todo!("Unhandled multiple table query! Amount: {}", tables.len());
            }
        }
    }

    let output_path = std::path::Path::new(&snowflake_path).join("snowflake_tables.rs");
    let mut file = std::fs::OpenOptions::new()
        .truncate(true)
        .write(true)
        .create(true)
        .open(&output_path)
        .expect("Failed to create/open `snowflake_table.rs` file");
    let generated = quote! {
        #(
            #structs
        )*
    }
    .to_string();
    let generated = generated.as_bytes();
    file.write_all(generated);
}

#[derive(serde::Deserialize, Debug)]
struct FileContent {
    private_key_path: String,
    public_key_path: String,
    host: String,
    account: String,
    user: String,
    role: Option<String>,
    warehouse: Option<String>,
    #[serde(rename = "database")]
    databases: Vec<Database>,
}

#[derive(serde::Deserialize, Debug)]
struct Database {
    name: String,
    tables: Vec<Table>,
}

#[derive(serde::Deserialize, Debug)]
pub struct Table {
    name: String,
    #[serde(rename = "json")]
    #[serde(default)]
    json_rows: HashMap<String, String>,
    #[serde(default)]
    unsigned: Vec<String>,
}

/*#[derive(serde::Deserialize, Debug)]
pub struct JsonMap {
    #[serde(rename = "name")]
    row_name: String,
    #[serde(rename = "type")]
    type_name: String,
}*/

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct SnowflakeData {
    last_modified: usize,
}
