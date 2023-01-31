use jwt_simple::prelude::*;

pub fn create_token(account_identifier: &str, user: &str) -> Result<String, KeyPairError> {
    let private_key = get_private_key()?;
    let public_key_fingerprint = get_public_key()?;
    let mut public_key_fingerprint = RS256PublicKey::from_pem(&public_key_fingerprint)
        .map_err(KeyPairError::FingerprintGeneration)?
        .sha256_thumbprint();
    let padding = public_key_fingerprint.len() % 3;
    for _ in 0..padding {
        public_key_fingerprint.push('=');
    }
    let qualified_username  = format!("{account_identifier}.{user}");
    let issuer = format!("{qualified_username}.SHA256:{public_key_fingerprint}");
    let claims = Claims::create(Duration::from_hours(1))
        .with_issuer(issuer)
        .with_subject(qualified_username);
    let key_pair = RS256KeyPair::from_pem(&private_key)
        .map_err(KeyPairError::KayPairGeneration)?;
    key_pair.sign(claims)
        .map_err(KeyPairError::KayPairGeneration)
}

fn get_private_key() -> Result<String, KeyPairError> {
    let path = "./environment_variables/local/snowflake_private_key_path.txt";
    let private_key = std::fs::read_to_string(path)
        .map_err(|e| KeyPairError::PrivateKeyRead(e, path.into()))?;
    std::fs::read_to_string(&private_key)
        .map_err(|e| KeyPairError::PrivateKeyRead(e, private_key))
}

fn get_public_key() -> Result<String, KeyPairError> {
    let path = "./environment_variables/local/snowflake_public_key_path.txt";
    let public_key = std::fs::read_to_string(path)
        .map_err(|e| KeyPairError::PublicKeyRead(e, path.into()))?;
    std::fs::read_to_string(&public_key)
        .map_err(|e| KeyPairError::PublicKeyRead(e, public_key))
}

#[derive(thiserror::Error, Debug)]
pub enum KeyPairError {
    #[error("failed to read public key, path: {1}—{0}")]
    PublicKeyRead(std::io::Error, String),
    #[error("failed to read private key, path: {1}—{0}")]
    PrivateKeyRead(std::io::Error, String),
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
        let token = create_token("TEST_ACCOUNT", "TEST_USER")?;
        let public_key = get_public_key()?;
        let public_key = RS256PublicKey::from_pem(&public_key)?;
        let verified = public_key.verify_token::<JWTClaims<NoCustomClaims>>(&token, None);
        assert!(verified.is_ok());
        Ok(())
    }
}
