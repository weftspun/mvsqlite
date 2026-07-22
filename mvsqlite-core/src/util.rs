use std::time::Duration;

use anyhow::{Context, Result};
use foundationdb::{options::ConflictRangeType, FdbError, Transaction};
use thiserror::Error;

use crate::keys::KeyCodec;

pub async fn get_txn_read_version_as_versionstamp(txn: &Transaction) -> Result<[u8; 10]> {
    let read_version = txn.get_read_version().await? as u64;
    let mut buf = [0u8; 10];
    buf[0..8].copy_from_slice(&read_version.to_be_bytes());

    // Now we can observe all changes with `committed_version == read_version`.
    buf[8] = 255;
    buf[9] = 255;
    Ok(buf)
}

pub fn decode_version(version: &str) -> Result<[u8; 10]> {
    let mut bytes = [0u8; 10];
    hex::decode_to_slice(version, &mut bytes).with_context(|| "cannot decode version")?;
    Ok(bytes)
}

pub fn generate_suffix_versionstamp_atomic_op(template: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(template.len() + 4);
    out.extend_from_slice(template);
    out.extend_from_slice(&(template.len() as u32 - 10).to_le_bytes());
    out
}

pub struct ContentIndex {
    pub time: Duration,
    pub versionstamp: [u8; 10],
}

impl ContentIndex {
    pub fn generate_mutation_payload(now: Duration) -> [u8; 22] {
        let mut buf = [0u8; 22];
        buf[0..8].copy_from_slice(&now.as_secs().to_be_bytes());
        buf[18..22].copy_from_slice(&8u32.to_le_bytes()[..]);
        buf
    }

    pub fn decode(x: &[u8]) -> Result<Self> {
        if x.len() != 18 {
            return Err(anyhow::anyhow!("invalid content index"));
        }
        let time = Duration::from_secs(u64::from_be_bytes(x[0..8].try_into().unwrap()));
        let versionstamp = x[8..18].try_into().unwrap();
        Ok(Self { time, versionstamp })
    }
}

#[derive(Error, Debug)]
#[error("gone: {0}")]
pub struct GoneError(pub &'static str);

/// Wraps the `Arc<anyhow::Error>` that `moka`'s `try_get_with` returns on
/// error, preserving it as a proper `source()` instead of losing its type
/// via string interpolation (`anyhow!("...: {}", e)`). That distinction
/// matters: something further up the call chain - notably
/// `mvclient::is_retryable`'s search for a `FdbError` anywhere in the
/// chain - needs the original error type to still be reachable, not just
/// its formatted message.
#[derive(Debug)]
pub struct CacheError(pub std::sync::Arc<anyhow::Error>);

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CacheError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&**self.0)
    }
}

pub fn add_single_key_read_conflict_range(txn: &Transaction, key: &[u8]) -> Result<(), FdbError> {
    txn.add_conflict_range(
        key,
        &key.iter()
            .copied()
            .chain(std::iter::once(0u8))
            .collect::<Vec<u8>>(),
        ConflictRangeType::Read,
    )?;
    Ok(())
}

pub async fn get_last_write_version(
    txn: &Transaction,
    key_codec: &KeyCodec,
    ns_id: [u8; 10],
    snapshot: bool,
) -> Result<[u8; 10], FdbError> {
    let mut version = [0u8; 10];
    let last_write_version_key = key_codec.construct_last_write_version_key(ns_id);
    if let Some(t) = txn.get(&last_write_version_key, snapshot).await? {
        if t.len() == 16 + 10 {
            version = <[u8; 10]>::try_from(&t[16..26]).unwrap();
        }
    }

    Ok(version)
}

pub fn truncate_10_byte_suffix(data: &[u8]) -> &[u8] {
    assert!(data.len() >= 10);
    &data[..data.len() - 10]
}

pub fn extract_10_byte_suffix(data: &[u8]) -> [u8; 10] {
    assert!(data.len() >= 10);
    <[u8; 10]>::try_from(&data[data.len() - 10..]).unwrap()
}

pub fn extract_beu32_suffix(data: &[u8]) -> u32 {
    assert!(data.len() >= 4);
    u32::from_be_bytes(<[u8; 4]>::try_from(&data[data.len() - 4..]).unwrap())
}

/// The PLCC/LWV conflict-detection rule, shared by every conflict check in
/// `commit.rs`: a version observed by a transaction conflicts with the
/// current version of the same resource iff the current version is strictly
/// newer. Versions are 10-byte big-endian (8-byte FDB commit version + 2-byte
/// in-batch order), so byte-lexicographic comparison is numeric comparison.
pub fn version_conflicts(current_version: [u8; 10], observed_version: [u8; 10]) -> bool {
    current_version > observed_version
}
