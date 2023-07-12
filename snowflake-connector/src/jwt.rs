use base64::Engine;
use std::path::Path;

use jwt_simple::prelude::*;

pub fn create_token(
    public_key_path: &Path,
    private_key_path: &Path,
    account_identifier: &str,
    user: &str,
) -> Result<String, KeyPairError> {
    let private_key = get_private_key(private_key_path)?;
    let public_key_fingerprint = get_public_key(public_key_path)?;
    let fp = RS256PublicKey::from_pem(&public_key_fingerprint)
        .map_err(KeyPairError::FingerprintGeneration)?
        .sha256_thumbprint();

    let new_bs = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&fp)
        .expect("decoding shit");

    let correct_fp = base64::engine::general_purpose::STANDARD.encode(new_bs);

    println!("fp: {fp}");
    println!("correct_fp: {correct_fp}");

    let qualified_username = format!("{account_identifier}.{user}");
    let issuer = format!("{qualified_username}.SHA256:{correct_fp}");

    let claims = Claims::create(Duration::from_hours(1))
        .with_issuer(issuer)
        .with_subject(qualified_username);

    let key_pair = RS256KeyPair::from_pem(&private_key).map_err(KeyPairError::KayPairGeneration)?;
    key_pair
        .sign(claims)
        .map_err(KeyPairError::KayPairGeneration)
}

fn get_private_key<P: AsRef<Path>>(path: P) -> Result<String, KeyPairError> {
    std::fs::read_to_string(&path).map_err(|e| {
        KeyPairError::PrivateKeyRead(
            e,
            if let Some(path) = path.as_ref().to_str() {
                path
            } else {
                "N/A"
            }
            .into(),
        )
    })
}

fn get_public_key<P: AsRef<Path>>(path: P) -> Result<String, KeyPairError> {
    std::fs::read_to_string(&path).map_err(|e| {
        KeyPairError::PublicKeyRead(
            e,
            if let Some(path) = path.as_ref().to_str() {
                path
            } else {
                "N/A"
            }
            .into(),
        )
    })
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
        let public_key_path = "./environment_variables/local/rsa_key.pub";
        let token = create_token(
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
