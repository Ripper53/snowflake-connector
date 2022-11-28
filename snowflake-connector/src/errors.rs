use crate::jwt::KeyPairError;

#[derive(thiserror::Error, Debug)]
pub enum SnowflakeError {
    #[error("could not generate JWT token")]
    Token(#[from] KeyPairError),
    #[error("could not start client")]
    SqlClient(anyhow::Error),
    #[error("could not execute sql statement")]
    SqlExecution(anyhow::Error),
    #[error("failed to parse returned json")]
    SqlResultParse(anyhow::Error),
}
