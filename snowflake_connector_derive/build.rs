use std::io::{Read, Write};

#[tokio::main]
async fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let data_file_path = std::path::Path::new(&out_dir).join("snowflake-data.json");
    if let Ok(true) = std::fs::exists(&data_file_path) {
        // Data is cached.
        return;
    }
    let info_path = std::env::var("SNOWFLAKE_INFO_PATH")
        .expect("Failed to find SNOWFLAKE_INFO_PATH environment variable");
    let Ok(mut s) = std::fs::read_to_string(info_path) else {
        panic!("FILE DOES NOT EXIST");
    };
    let mut write_file = match std::fs::OpenOptions::new()
        .truncate(true)
        .write(true)
        .create(true)
        .open(data_file_path)
    {
        Ok(r) => r,
        Err(e) => {
            panic!("FILE COULD NOT BE OPENED/CREATED: {e:?}");
        }
    };
    let file: FileContent = toml::de::from_str(&s).unwrap();
    let connector = snowflake_deserializer::SnowflakeConnector::try_new_from_file(
        &file.public_key_path,
        &file.private_key_path,
        &file.host,
        &file.account,
        &file.user,
    )
    .unwrap();
    for database in file.databases {
        for table in database.tables {
            let sql = format!("SELECT * FROM {table} LIMIT 0");
            let mut sql = connector.execute(&database.name).sql(&sql);
            if let Some(ref role) = file.role {
                sql = sql.with_role(role);
            }
            if let Some(ref warehouse) = file.warehouse {
                sql = sql.with_warehouse(warehouse);
            }
            let r = sql.text().await.unwrap();
            let b = r.as_bytes();
            let length = format!("{}\n", b.len());
            write_file.write(length.as_bytes()).unwrap();
            write_file.write(r.as_bytes()).unwrap();
            write_file.write("\n".to_string().as_bytes()).unwrap();
        }
    }
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
    tables: Vec<String>,
}
