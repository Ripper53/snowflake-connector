use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Changes {
    #[serde(rename = "numRowsInserted")]
    pub rows_inserted: usize,
    #[serde(rename = "numRowsDeleted")]
    pub rows_deleted: usize,
    #[serde(rename = "numRowsUpdated")]
    pub rows_updated: usize,
    #[serde(rename = "numDmlDuplicates")]
    pub duplicates: usize,
}

#[derive(Deserialize, Debug)]
pub struct DataManipulationResult {
    pub message: String,
    pub stats: Changes,
}
