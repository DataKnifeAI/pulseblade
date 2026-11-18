//! Node-side check of a hub release offer, and the systemd file swap.
//!
//! The hub never signs. Verification uses a public key pinned in node config.
//! A Kubernetes runtime can be told an offer is valid, and still will not
//! replace its image.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ring::digest::{digest, SHA256};
use ring::signature::{UnparsedPublicKey, ED25519};

#[path = "release_wire.rs"]
mod wire;

pub use wire::ReleaseOffer;

/// How long a node may stay silent after it announces `applying`.
pub const UPDATE_WINDOW_MS: i64 = 180_000;
/// Largest artifact the node will hold while hashing.
const MAX_ARTIFACT_BYTES: u64 = 80 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum UpdateError {
    Signature,
    Digest,
    Replay,
    Expired,
    Url,
    Kubernetes,
    Io(String),
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Signature => write!(f, "release signature rejected"),
            Self::Digest => write!(f, "release digest mismatch"),
            Self::Replay => write!(f, "release sequence is a replay"),
            Self::Expired => write!(f, "release offer expired"),
            Self::Url => write!(f, "release url rejected"),
            Self::Kubernetes => write!(f, "kubernetes node does not apply an image"),
            Self::Io(err) => write!(f, "release file error: {err}"),
        }
    }
}

/// What the node should do with one offer. `Apply` still requires a second
/// call with the downloaded bytes before any file is replaced.
#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    Ignore,
    Report,
    Apply,
    Reject(UpdateError),
}

pub struct OfferCheck<'a> {
    pub mode: &'a str,
    pub runtime: &'a str,
    pub running_seq: u64,
    pub now_ms: i64,
    pub public_key_hex: &'a str,
    pub allowed_hosts: &'a [String],
    pub offer: &'a ReleaseOffer,
    pub bytes: Option<&'a [u8]>,
}

pub fn evaluate(check: &OfferCheck<'_>) -> Decision {
    if check.mode == "off" || check.mode.is_empty() {
        return Decision::Ignore;
    }
    if let Err(err) = verify_signature(check.offer, check.public_key_hex) {
        return Decision::Reject(err);
    }
    if let Err(err) = check_url(&check.offer.url, check.allowed_hosts) {
        return Decision::Reject(err);
    }
    if check.offer.expires_ms <= check.now_ms {
        return Decision::Reject(UpdateError::Expired);
    }
    if replayed(check.offer, check.running_seq) {
        return Decision::Reject(UpdateError::Replay);
    }
    if let Some(bytes) = check.bytes {
        if !digest_matches(bytes, &check.offer.digest) {
            return Decision::Reject(UpdateError::Digest);
        }
    }
    if check.runtime == "kubernetes" || check.mode != "apply" {
        return Decision::Report;
    }
    Decision::Apply
}

fn verify_signature(offer: &ReleaseOffer, public_key_hex: &str) -> Result<(), UpdateError> {
    if !offer.single_line() {
        return Err(UpdateError::Signature);
    }
    let key = wire::hex_decode(public_key_hex).ok_or(UpdateError::Signature)?;
    let sig = wire::hex_decode(&offer.signature).ok_or(UpdateError::Signature)?;
    if key.len() != 32 || sig.len() != 64 {
        return Err(UpdateError::Signature);
    }
    let public = UnparsedPublicKey::new(&ED25519, key);
    public
        .verify(&wire::canonical(offer), &sig)
        .map_err(|_| UpdateError::Signature)
}

/// `https` only, host on the node allow-list, no userinfo, no query, no fragment.
pub fn check_url(url: &str, allowed_hosts: &[String]) -> Result<(), UpdateError> {
    if url.chars().any(|c| c.is_whitespace() || c == '\\') {
        return Err(UpdateError::Url);
    }
    let rest = url.strip_prefix("https://").ok_or(UpdateError::Url)?;
    if rest.is_empty() || rest.contains('?') || rest.contains('#') || rest.contains('@') {
        return Err(UpdateError::Url);
    }
    let hostport = rest.split_once('/').map(|(host, _)| host).unwrap_or(rest);
    if hostport.is_empty() || hostport.contains('@') {
        return Err(UpdateError::Url);
    }
    let host = hostport.split(':').next().unwrap_or("");
    let host = host.trim().to_ascii_lowercase();
    if host.is_empty()
        || !allowed_hosts
            .iter()
            .any(|item| item.eq_ignore_ascii_case(&host))
    {
        return Err(UpdateError::Url);
    }
    Ok(())
}

fn replayed(offer: &ReleaseOffer, running_seq: u64) -> bool {
    if offer.rollback {
        offer.sequence >= running_seq
    } else {
        offer.sequence <= running_seq
    }
}

pub fn digest_matches(bytes: &[u8], expected: &str) -> bool {
    let got = sha256_hex(bytes);
    got == wire::normalize_digest(expected) && !got.is_empty()
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    wire::hex_encode(digest(&SHA256, bytes).as_ref())
}

pub fn fetch_artifact(url: &str) -> Result<Vec<u8>, UpdateError> {
    let agent = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout(Duration::from_secs(60))
        .build();
    let response = agent.get(url).call().map_err(|_| UpdateError::Url)?;
    let mut reader = response.into_reader();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = reader
            .read(&mut chunk)
            .map_err(|err| UpdateError::Io(err.to_string()))?;
        if n == 0 {
            break;
        }
        if buf.len() + n > MAX_ARTIFACT_BYTES as usize {
            return Err(UpdateError::Url);
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(buf)
}

/// Replace `dest` with `bytes` after the digest matches. The previous file is
/// kept beside it. A kubernetes runtime is refused before any write.
pub fn install_for_runtime(
    runtime: &str,
    dest: &Path,
    bytes: &[u8],
    expected_digest: &str,
) -> Result<(), UpdateError> {
    if runtime == "kubernetes" {
        return Err(UpdateError::Kubernetes);
    }
    if runtime != "systemd" {
        return Err(UpdateError::Kubernetes);
    }
    if !digest_matches(bytes, expected_digest) {
        return Err(UpdateError::Digest);
    }
    let new_path = sibling(dest, ".new");
    let prev_path = sibling(dest, ".prev");
    {
        let mut file = File::create(&new_path).map_err(|err| UpdateError::Io(err.to_string()))?;
        file.write_all(bytes)
            .map_err(|err| UpdateError::Io(err.to_string()))?;
        file.sync_all()
            .map_err(|err| UpdateError::Io(err.to_string()))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = dest
            .metadata()
            .map(|meta| meta.permissions().mode())
            .unwrap_or(0o755);
        let _ = fs::set_permissions(&new_path, fs::Permissions::from_mode(mode));
    }
    if dest.exists() {
        fs::copy(dest, &prev_path).map_err(|err| UpdateError::Io(err.to_string()))?;
    }
    fs::rename(&new_path, dest).map_err(|err| UpdateError::Io(err.to_string()))?;
    Ok(())
}

pub fn restore_previous(dest: &Path) -> Result<(), UpdateError> {
    let prev = sibling(dest, ".prev");
    if !prev.exists() {
        return Err(UpdateError::Io("no previous binary".into()));
    }
    fs::copy(&prev, dest).map_err(|err| UpdateError::Io(err.to_string()))?;
    let seq_prev = sibling(dest, ".sequence.prev");
    if seq_prev.exists() {
        let _ = fs::copy(&seq_prev, sibling(dest, ".sequence"));
    }
    Ok(())
}

pub fn write_sequence(binary: &Path, sequence: u64) -> Result<(), UpdateError> {
    let path = sibling(binary, ".sequence");
    let prev = sibling(binary, ".sequence.prev");
    if path.exists() {
        let _ = fs::copy(&path, &prev);
    }
    fs::write(&path, sequence.to_string()).map_err(|err| UpdateError::Io(err.to_string()))
}

pub fn read_sequence(binary: &Path) -> u64 {
    fs::read_to_string(sibling(binary, ".sequence"))
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0)
}

pub fn mark_pending(binary: &Path) -> Result<(), UpdateError> {
    fs::write(sibling(binary, ".pending"), b"1").map_err(|err| UpdateError::Io(err.to_string()))
}

pub fn pending(binary: &Path) -> bool {
    sibling(binary, ".pending").exists()
}

pub fn clear_pending(binary: &Path) {
    let _ = fs::remove_file(sibling(binary, ".pending"));
}

pub fn binary_path(configured: &str) -> PathBuf {
    if !configured.trim().is_empty() {
        return PathBuf::from(configured);
    }
    std::env::current_exe().unwrap_or_else(|_| PathBuf::from("pulseblade"))
}

pub fn identity_for(
    runtime: &str,
    binary: &Path,
    image_digest: &str,
) -> pulseblade_store::NodeBuild {
    let digest = if runtime == "kubernetes" {
        image_digest.trim().to_string()
    } else {
        fs::read(binary)
            .map(|bytes| format!("sha256:{}", sha256_hex(&bytes)))
            .unwrap_or_default()
    };
    pulseblade_store::NodeBuild {
        runtime: runtime.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_id: option_env!("PULSEBLADE_BUILD_ID")
            .unwrap_or("dev")
            .to_string(),
        digest,
        sequence: read_sequence(binary),
        update_state: String::new(),
        update_deadline_ms: 0,
    }
}

pub fn local_identity(binary_path_config: &str) -> pulseblade_store::NodeBuild {
    let in_cluster = std::env::var_os("KUBERNETES_SERVICE_HOST").is_some();
    let runtime = if in_cluster { "kubernetes" } else { "systemd" };
    let image = std::env::var("PULSEBLADE_IMAGE_DIGEST").unwrap_or_default();
    identity_for(runtime, &binary_path(binary_path_config), &image)
}

/// Replace this process with `path`. The caller keeps going only when exec fails.
pub fn exec_binary(path: &Path) -> std::io::Error {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        std::process::Command::new(path)
            .args(std::env::args_os().skip(1))
            .exec()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        std::io::Error::new(std::io::ErrorKind::Unsupported, "exec is unix-only")
    }
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "pulseblade".into());
    path.with_file_name(format!("{name}{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    struct Keys {
        public_hex: String,
        pair_pkcs8: Vec<u8>,
    }

    fn keys() -> Keys {
        let doc = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(doc.as_ref()).unwrap();
        Keys {
            public_hex: wire::hex_encode(pair.public_key().as_ref()),
            pair_pkcs8: doc.as_ref().to_vec(),
        }
    }

    fn signed(keys: &Keys, offer: &mut ReleaseOffer) {
        let pair = Ed25519KeyPair::from_pkcs8(&keys.pair_pkcs8).unwrap();
        let sig = pair.sign(&wire::canonical(offer));
        offer.signature = wire::hex_encode(sig.as_ref());
    }

    fn offer(seq: u64, digest: &str) -> ReleaseOffer {
        ReleaseOffer {
            sequence: seq,
            version: "0.1.0".into(),
            build_id: "abc123".into(),
            digest: digest.into(),
            expires_ms: 9_000_000_000_000,
            url: "https://releases.example/pulseblade".into(),
            rollback: false,
            signature: String::new(),
        }
    }

    #[test]
    fn rejects_a_bad_signature_a_mismatched_digest_and_a_replay() {
        let keys = keys();
        let hosts = vec!["releases.example".into()];
        let mut good = offer(3, "sha256:abcd");
        signed(&keys, &mut good);
        let bytes = b"new-binary";
        let digest = format!("sha256:{}", sha256_hex(bytes));
        good.digest = digest.clone();
        signed(&keys, &mut good);

        let mut bad_sig = good.clone();
        let flip = if bad_sig.signature.starts_with("00") {
            "ff"
        } else {
            "00"
        };
        bad_sig.signature.replace_range(0..2, flip);
        let check = |seq, offer: &ReleaseOffer, body: &[u8]| {
            evaluate(&OfferCheck {
                mode: "apply",
                runtime: "systemd",
                running_seq: seq,
                now_ms: 1_000,
                public_key_hex: &keys.public_hex,
                allowed_hosts: &hosts,
                offer,
                bytes: Some(body),
            })
        };
        assert_eq!(
            check(1, &bad_sig, bytes),
            Decision::Reject(UpdateError::Signature)
        );

        let wrong = b"other-binary";
        assert_eq!(
            check(1, &good, wrong),
            Decision::Reject(UpdateError::Digest)
        );

        assert_eq!(
            check(3, &good, bytes),
            Decision::Reject(UpdateError::Replay)
        );
        assert_eq!(
            check(4, &good, bytes),
            Decision::Reject(UpdateError::Replay)
        );

        let mut token_url = good.clone();
        token_url.url = "https://releases.example/pulseblade?token=secret".into();
        signed(&keys, &mut token_url);
        assert_eq!(
            check(1, &token_url, bytes),
            Decision::Reject(UpdateError::Url)
        );
    }

    #[test]
    fn kubernetes_runtime_refuses_to_apply() {
        let keys = keys();
        let hosts = vec!["releases.example".into()];
        let bytes = b"image-bytes-are-not-used";
        let mut good = offer(2, &format!("sha256:{}", sha256_hex(bytes)));
        signed(&keys, &mut good);
        assert_eq!(
            evaluate(&OfferCheck {
                mode: "apply",
                runtime: "kubernetes",
                running_seq: 0,
                now_ms: 1_000,
                public_key_hex: &keys.public_hex,
                allowed_hosts: &hosts,
                offer: &good,
                bytes: Some(bytes),
            }),
            Decision::Report
        );

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("pulseblade");
        fs::write(&dest, b"old").unwrap();
        let err = install_for_runtime("kubernetes", &dest, bytes, &good.digest).unwrap_err();
        assert_eq!(err, UpdateError::Kubernetes);
        assert_eq!(fs::read(&dest).unwrap(), b"old");
        assert!(!sibling(&dest, ".prev").exists());
        assert!(!sibling(&dest, ".new").exists());
    }

    #[test]
    fn systemd_apply_keeps_the_previous_binary() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("pulseblade");
        fs::write(&dest, b"old-binary").unwrap();
        let bytes = b"new-binary";
        let digest = format!("sha256:{}", sha256_hex(bytes));
        install_for_runtime("systemd", &dest, bytes, &digest).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"new-binary");
        assert_eq!(fs::read(sibling(&dest, ".prev")).unwrap(), b"old-binary");
        restore_previous(&dest).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"old-binary");
    }

    #[test]
    fn identity_is_not_the_crate_version_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pulseblade");
        fs::write(&path, b"build-bytes").unwrap();
        let id = identity_for("systemd", &path, "");
        assert_eq!(id.version, "0.1.0");
        assert_ne!(id.digest, id.version);
        assert!(id.digest.starts_with("sha256:"));
        assert_eq!(id.digest, format!("sha256:{}", sha256_hex(b"build-bytes")));
        assert_eq!(id.runtime, "systemd");
        assert!(!id.build_id.is_empty());

        let cluster = identity_for("kubernetes", &path, "sha256:image");
        assert_eq!(cluster.runtime, "kubernetes");
        assert_eq!(cluster.digest, "sha256:image");
        assert_ne!(cluster.digest, cluster.version);
    }
}
