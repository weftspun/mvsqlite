use anyhow::{Context, Result};
use foundationdb::{options::MutationType, options::TransactionOption, Transaction};

use crate::{
    metadata::{NamespaceMetadata, NamespaceOverlayBase},
    util::{decode_version, generate_suffix_versionstamp_atomic_op},
    Core,
};

#[derive(Debug)]
pub enum CreateNamespaceError {
    AlreadyExist,
}

impl std::fmt::Display for CreateNamespaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for CreateNamespaceError {}

enum GetError {
    NotFound,
    Other(anyhow::Error),
}

impl Core {
    pub fn is_read_only(&self) -> bool {
        self.replica_manager.is_some()
    }

    pub async fn create_namespace(
        &self,
        key: &str,
        overlay_base: Option<NamespaceOverlayBase>,
    ) -> Result<()> {
        let nskey_key = self.key_codec.construct_nskey_key(&key);
        let nsmd = NamespaceMetadata {
            lock: None,
            overlay_base,
        };
        if let Some(base) = &nsmd.overlay_base {
            decode_version(&base.ns_id).with_context(|| "overlay_base: invalid ns_id")?;
            decode_version(&base.snapshot_version)
                .with_context(|| "overlay_base: invalid snapshot_version")?;
        }
        let nsmd = serde_json::to_string(&nsmd)?;
        let mut txn = self.db.create_trx()?;

        loop {
            if txn.get(&nskey_key, false).await?.is_some() {
                return Err(anyhow::Error::new(CreateNamespaceError::AlreadyExist));
            }

            let nsmd_atomic_op_key = generate_suffix_versionstamp_atomic_op(
                &self.key_codec.construct_nsmd_key([0u8; 10]),
            );
            let nskey_atomic_op_value = [0u8; 14];
            txn.atomic_op(
                &nsmd_atomic_op_key,
                nsmd.as_bytes(),
                MutationType::SetVersionstampedKey,
            );
            txn.atomic_op(
                &nskey_key,
                &nskey_atomic_op_value,
                MutationType::SetVersionstampedValue,
            );
            match txn.commit().await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    txn = match e.on_error().await {
                        Ok(x) => x,
                        Err(e) => {
                            return Err(e.into());
                        }
                    };
                }
            }
        }
    }

    pub async fn lookup_nskey(&self, nskey: &str, hashproof: Option<&str>) -> Result<Option<[u8; 10]>> {
        let hashproof_hash = {
            let segs = nskey.split(":").collect::<Vec<_>>();
            if segs.len() < 2 {
                None
            } else {
                Some(segs[1])
            }
        };
        if let Some(hashproof_hash) = hashproof_hash {
            let mut hash: [u8; 32] = [0u8; 32];
            if hex::decode_to_slice(hashproof_hash, &mut hash).is_err() {
                tracing::error!(nskey, "hashproof_hash hex decode failed");
                return Ok(None);
            }
            let proof = match hex::decode(hashproof.unwrap_or("")) {
                Ok(x) => x,
                Err(_) => {
                    tracing::error!(nskey, "hashproof hex decode failed");
                    return Ok(None);
                }
            };
            let hashed_proof = blake3::hash(&proof);
            if !constant_time_eq::constant_time_eq_n(hashed_proof.as_bytes(), &hash) {
                tracing::error!(nskey, "hashproof mismatch");
                return Ok(None);
            }
        }

        let res = self
            .nskey_cache
            .try_get_with(nskey.to_string(), async {
                let txn = self.db.create_trx();
                match txn {
                    Ok(txn) => {
                        if self.is_read_only() {
                            txn.set_option(TransactionOption::ReadLockAware).unwrap();
                        }
                        match txn
                            .get(&self.key_codec.construct_nskey_key(nskey), false)
                            .await
                        {
                            Ok(Some(x)) => <[u8; 10]>::try_from(&x[..])
                                .with_context(|| "invalid namespace id")
                                .map_err(GetError::Other),
                            Ok(None) => Err(GetError::NotFound),
                            Err(e) => Err(GetError::Other(
                                anyhow::Error::from(e).context("transaction failed"),
                            )),
                        }
                    }
                    Err(e) => Err(GetError::Other(
                        anyhow::Error::from(e).context("transaction creation failed"),
                    )),
                }
            })
            .await;
        match res.as_ref().map_err(|e| &**e) {
            Ok(x) => Ok(Some(*x)),
            Err(GetError::NotFound) => Ok(None),
            Err(GetError::Other(x)) => Err(anyhow::anyhow!("nskey lookup failed: {}", x)),
        }
    }

    pub async fn create_versioned_read_txn(&self, version: &str) -> Result<Transaction> {
        let version = decode_version(version)?;
        let txn = self.db.create_trx()?;
        if self.is_read_only() {
            txn.set_option(TransactionOption::ReadLockAware).unwrap();
        }

        // It's safe to set CRR here. We do our own version check below.
        txn.set_option(TransactionOption::CausalReadRisky).unwrap();

        let mut grv_called = false;
        let fdb_rv = self
            .read_version_cache
            .try_get_with(version, async {
                grv_called = true;
                let fdb_rv = txn.get_read_version().await;
                match fdb_rv {
                    Ok(fdb_rv) => {
                        // XXX: We are only checking for primary read here due to performance reasons
                        if !self.is_read_only() && fdb_rv < i64::from_be_bytes(<[u8; 8]>::try_from(&version[0..8]).unwrap()) {
                            Err(anyhow::anyhow!("fdb read version older than requested version - causal read fault?"))
                        } else {
                            Ok(fdb_rv)
                        }
                    },
                    Err(e) => Err(anyhow::Error::from(e))
                }
            })
            .await
            .map_err(|e| anyhow::anyhow!("cannot get read version: {}", e))?;
        if !grv_called {
            txn.set_read_version(fdb_rv);
        }
        Ok(txn)
    }
}
