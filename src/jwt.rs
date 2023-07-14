use anyhow::Context;
use base64::Engine;
use jwt_simple::prelude::*;

use crate::{PrivateKey, PublicKey, Result};

pub fn create_token(
    public_key: PublicKey,
    private_key: PrivateKey,
    account_identifier: &str,
    user: &str,
) -> Result<String> {
    let fp = RS256PublicKey::from_pem(&public_key.0)
        .context("Creating PublicKey PEM")?
        .sha256_thumbprint();

    let new_bs = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(fp)
        .context("Decoding non padded url safe thumbprint")?;

    let correct_fp = base64::engine::general_purpose::STANDARD.encode(new_bs);

    let qualified_username = format!("{account_identifier}.{user}");
    let issuer = format!("{qualified_username}.SHA256:{correct_fp}");

    let claims = Claims::create(Duration::from_hours(24 * 365 * 10))
        .with_issuer(issuer)
        .with_subject(qualified_username);

    let key_pair =
        RS256KeyPair::from_pem(&private_key.0).context("Creating key par from private key pem")?;

    key_pair.sign(claims).context("Signing Claims")
}
