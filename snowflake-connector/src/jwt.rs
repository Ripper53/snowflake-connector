use jwt_simple::prelude::*;
/// Re-export the `RS256KeyPair` type from `jwt_simple` to ease loading
pub use jwt_simple::algorithms::RS256KeyPair;

pub fn create_token(
    key_pair: &RS256KeyPair,
    account_identifier: &str,
    user: &str,
) -> Result<String, KeyPairError> {
    let mut public_key_fingerprint = key_pair.public_key()
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
    key_pair.sign(claims)
        .map_err(KeyPairError::KeyPairGeneration)
}

#[derive(thiserror::Error, Debug)]
pub enum KeyPairError {
    #[error("failed to generate fingerprint from public key—{0}")]
    FingerprintGeneration(anyhow::Error),
    #[error("failed to generate key pair from private key—{0}")]
    KeyPairGeneration(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_jwt() -> Result<(), anyhow::Error> {
        let key = RS256KeyPair::generate(2048)?;
        let token = create_token(
            &key,
            "TEST_ACCOUNT",
            "TEST_USER",
        )?;
        let verified = key.public_key().verify_token::<JWTClaims<NoCustomClaims>>(&token, None);
        assert!(verified.is_ok());
        Ok(())
    }
}
