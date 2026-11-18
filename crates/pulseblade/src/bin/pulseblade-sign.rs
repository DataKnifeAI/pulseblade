//! Sign a release manifest. This binary is not the hub and is not copied into
//! the hub image. The signing key is the path in `PULSEBLADE_RELEASE_SIGNING_KEY`.
//! The hub never reads that variable and cannot mint a signature.

use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::process::ExitCode;

use ring::signature::{Ed25519KeyPair, KeyPair};
use rustls::pki_types::PrivateKeyDer;
use rustls_pemfile::private_key;

#[path = "../release_wire.rs"]
mod wire;

use wire::{canonical, hex_encode, ReleaseOffer};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("pulseblade-sign: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    let sequence = required(&args, "--sequence")?
        .parse::<u64>()
        .map_err(|_| "sequence must be an integer".to_string())?;
    let version = required(&args, "--version")?;
    let build_id = required(&args, "--build-id")?;
    let url = required(&args, "--url")?;
    let out = required(&args, "--out")?;
    let digest = match optional(&args, "--digest") {
        Some(digest) => digest,
        None => {
            let binary = required(&args, "--binary")?;
            let bytes =
                std::fs::read(&binary).map_err(|_| "reading --binary failed".to_string())?;
            format!("sha256:{}", sha256(&bytes))
        }
    };
    let expires_secs = optional(&args, "--expires-secs")
        .unwrap_or_else(|| "604800".into())
        .parse::<i64>()
        .map_err(|_| "expires-secs must be an integer".to_string())?;
    let rollback = args.iter().any(|arg| arg == "--rollback");
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before the unix epoch".to_string())?
        .as_millis() as i64;
    let mut offer = ReleaseOffer {
        sequence,
        version,
        build_id,
        digest,
        expires_ms: now_ms.saturating_add(expires_secs.saturating_mul(1000)),
        url,
        rollback,
        signature: String::new(),
    };
    if !offer.single_line() {
        return Err("release fields must be a single line".into());
    }
    let key_path = env::var("PULSEBLADE_RELEASE_SIGNING_KEY")
        .map_err(|_| "PULSEBLADE_RELEASE_SIGNING_KEY is unset".to_string())?;
    let pair = load_key(Path::new(&key_path))?;
    let public = hex_encode(pair.public_key().as_ref());
    offer.signature = hex_encode(pair.sign(&canonical(&offer)).as_ref());
    let text = serde_json::to_string_pretty(&offer)
        .map_err(|_| "encoding the manifest failed".to_string())?;
    std::fs::write(&out, text).map_err(|_| "writing the manifest failed".to_string())?;
    let pub_path = format!("{out}.pub");
    std::fs::write(&pub_path, format!("{public}\n"))
        .map_err(|_| "writing the public key file failed".to_string())?;
    eprintln!("wrote {out}");
    eprintln!("public_key={public}");
    Ok(())
}

fn load_key(path: &Path) -> Result<Ed25519KeyPair, String> {
    let file = File::open(path)
        .map_err(|_| "opening PULSEBLADE_RELEASE_SIGNING_KEY failed".to_string())?;
    let mut reader = BufReader::new(file);
    let key = private_key(&mut reader).map_err(|_| "reading the signing key failed".to_string())?;
    let Some(PrivateKeyDer::Pkcs8(der)) = key else {
        return Err("signing key must be a PKCS#8 ed25519 PEM".into());
    };
    Ed25519KeyPair::from_pkcs8(der.secret_pkcs8_der())
        .map_err(|_| "signing key was rejected".into())
}

fn sha256(bytes: &[u8]) -> String {
    hex_encode(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
}

fn required(args: &[String], name: &str) -> Result<String, String> {
    optional(args, name).ok_or_else(|| format!("missing {name}"))
}

fn optional(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|idx| args.get(idx + 1))
        .cloned()
}
