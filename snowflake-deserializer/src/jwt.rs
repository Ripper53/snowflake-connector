use std::path::Path;

use jwt_simple::prelude::*;

pub fn create_token(
    public_key: &str,
    private_key: &str,
    account_identifier: &str,
    user: &str,
) -> Result<String, KeyPairError> {
    let mut public_key_fingerprint = RS256PublicKey::from_pem(&public_key)
        .map_err(KeyPairError::FingerprintGeneration)?
        .sha256_thumbprint()
        // Snowflake expects slashes and not underscores for some reason...
        .replace('_', "/");
    // Snowflake requires padding for some reason...
    let padding = public_key_fingerprint.len() % 3;
    for _ in 0..padding {
        public_key_fingerprint.push('=');
    }
    let qualified_username = format!("{account_identifier}.{user}");
    let issuer = format!("{qualified_username}.SHA256:{public_key_fingerprint}");
    let claims = Claims::create(Duration::from_hours(1))
        .with_issuer(issuer)
        .with_subject(qualified_username);
    let key_pair = RS256KeyPair::from_pem(&private_key).map_err(KeyPairError::KayPairGeneration)?;
    key_pair
        .sign(claims)
        .map_err(KeyPairError::KayPairGeneration)
}

pub fn create_token_from_file<P: AsRef<Path>>(
    public_key_path: P,
    private_key_path: P,
    account_identifier: &str,
    user: &str,
) -> Result<String, TokenFromFileError> {
    let private_key = get_private_key(private_key_path)?;
    let public_key = get_public_key(public_key_path)?;
    Ok(create_token(
        &public_key,
        &private_key,
        account_identifier,
        user,
    )?)
}

fn get_private_key<P: AsRef<Path>>(path: P) -> Result<String, KeyFileReadError> {
    std::fs::read_to_string(&path).map_err(|error| KeyFileReadError::PrivateKeyRead {
        error,
        path: if let Some(path) = path.as_ref().to_str() {
            path
        } else {
            "N/A"
        }
        .into(),
    })
}

fn get_public_key<P: AsRef<Path>>(path: P) -> Result<String, KeyFileReadError> {
    std::fs::read_to_string(&path).map_err(|error| KeyFileReadError::PublicKeyRead {
        error,
        path: if let Some(path) = path.as_ref().to_str() {
            path
        } else {
            "N/A"
        }
        .into(),
    })
}

#[derive(thiserror::Error, Debug)]
pub enum TokenFromFileError {
    #[error(transparent)]
    KeyPair(#[from] KeyPairError),
    #[error(transparent)]
    KeyFileRead(#[from] KeyFileReadError),
}

#[derive(thiserror::Error, Debug)]
pub enum KeyFileReadError {
    #[error("failed to read public key, path: {error}—{path}")]
    PublicKeyRead { error: std::io::Error, path: String },
    #[error("failed to read private key, path: {error}—{path}")]
    PrivateKeyRead { error: std::io::Error, path: String },
}

#[derive(thiserror::Error, Debug)]
pub enum KeyPairError {
    #[error("failed to generate fingerprint from public key—{0}")]
    FingerprintGeneration(anyhow::Error),
    #[error("failed to generate key pair from private key—{0}")]
    KayPairGeneration(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_jwt() -> Result<(), anyhow::Error> {
        let public_key_path = "./environment_variables/local/rsa_key.pub";
        let token = create_token_from_file(
            public_key_path,
            "./environment_variables/local/rsa_key.p8",
            "TEST_ACCOUNT",
            "TEST_USER",
        )?;
        let public_key = get_public_key(public_key_path)?;
        let public_key = RS256PublicKey::from_pem(&public_key)?;
        let verified = public_key.verify_token::<JWTClaims<NoCustomClaims>>(&token, None);
        assert!(verified.is_ok());
        Ok(())
    }
}
