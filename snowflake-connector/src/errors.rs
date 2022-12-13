use crate::jwt::KeyPairError;

#[derive(thiserror::Error, Debug)]
pub enum SnowflakeError {
    #[error("could not generate JWT token")]
    Token(#[from] KeyPairError),
    #[error("could not start client—{0}")]
    SqlClient(anyhow::Error),
    #[error("could not execute sql statement—{0}")]
    SqlExecution(anyhow::Error),
    #[error("failed to parse returned json—{0}")]
    SqlResultParse(anyhow::Error),
}
