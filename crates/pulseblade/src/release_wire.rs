//! Signed release offer. Shared by the node and by `pulseblade-sign`.
//!
//! The canonical bytes do not include the signature. A field that contains a
//! newline is rejected so the signed text cannot be split into a second record.

use serde::{Deserialize, Serialize};

/// What the hub may forward on poll. The signature is over [`canonical`].
/// There is no private key field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseOffer {
    pub sequence: u64,
    pub version: String,
    pub build_id: String,
    pub digest: String,
    pub expires_ms: i64,
    pub url: String,
    #[serde(default)]
    pub rollback: bool,
    pub signature: String,
}

impl ReleaseOffer {
    pub fn single_line(&self) -> bool {
        [
            &self.version,
            &self.build_id,
            &self.digest,
            &self.url,
            &self.signature,
        ]
        .iter()
        .all(|field| !field.contains('\n') && !field.contains('\r'))
    }
}

/// Bytes the release key signs. The signature field is not part of this text.
pub fn canonical(offer: &ReleaseOffer) -> Vec<u8> {
    format!(
        "pulseblade-release-v1\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        offer.sequence,
        offer.version,
        offer.build_id,
        normalize_digest(&offer.digest),
        offer.expires_ms,
        offer.url,
        if offer.rollback { "1" } else { "0" },
    )
    .into_bytes()
}

pub fn normalize_digest(digest: &str) -> String {
    let digest = digest.trim();
    digest
        .strip_prefix("sha256:")
        .unwrap_or(digest)
        .to_ascii_lowercase()
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[allow(dead_code)]
pub fn hex_decode(text: &str) -> Option<Vec<u8>> {
    let text = text.trim();
    if !text.len().is_multiple_of(2) {
        return None;
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_val(bytes[i])?;
        let lo = hex_val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

#[allow(dead_code)]
fn hex_val(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
