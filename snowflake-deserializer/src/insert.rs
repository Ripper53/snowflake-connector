use crate::{SnowflakeExecutor, SnowflakeSQL, SnowflakeSQLString};

pub trait SnowflakeInsert {
    fn table_name() -> &'static str;
    fn column_index(index: usize) -> Option<&'static str>;
    fn insert_values(&self) -> impl Iterator<Item = Option<impl ToString>>;
}

pub struct Insert<'a> {
    column_name: &'a str,
    column_value: &'a str,
}

impl<'a, D: ToString> SnowflakeExecutor<'a, D> {
    pub fn insert<T: SnowflakeInsert>(self, insert_row: T) -> SnowflakeSQL<'a, SnowflakeSQLString> {
        let sql = format!("INSERT INTO {}", T::table_name());
        self.sql_owned(sql)
    }
}
