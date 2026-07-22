use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use blake3::Hash;
use bytes::Bytes;
use foundationdb::{options::MutationType, Transaction};
use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
use moka::future::Cache;

use crate::{
    delta::writer::DeltaWriter,
    fixed::FixedKeyVec,
    keys::KeyCodec,
    page::{Page, MAX_PAGE_SIZE},
    util::ContentIndex,
    Core,
};
use anyhow::{Context, Result as AnyResult};
use foundationdb::{options::TransactionOption, FdbError};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct WriteRequest<'a> {
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],

    pub delta_base: Option<u32>,
}

#[derive(Serialize)]
pub struct WriteResponse {
    #[serde(with = "serde_bytes")]
    pub hash: Vec<u8>,
}

pub struct WriteApplier<'a> {
    txn: &'a Transaction,
    ns_id: [u8; 10],
    key_codec: &'a KeyCodec,
    now: Duration,
    seen_hashes: HashSet<Hash>,
    content_cache: Option<&'a Cache<[u8; 32], Bytes>>,
}

pub struct WriteApplierContext<'a> {
    pub txn: &'a Transaction,
    pub ns_id: [u8; 10],
    pub key_codec: &'a KeyCodec,
    pub now: Duration,
    pub content_cache: Option<&'a Cache<[u8; 32], Bytes>>,
}

struct WriteContext<'a> {
    req: &'a WriteRequest<'a>,
    hash: Hash,
    content_key: FixedKeyVec,
    content_index_key: FixedKeyVec,
    early_completion: AtomicBool,
}

impl<'a> WriteApplier<'a> {
    pub fn new(ctx: WriteApplierContext<'a>) -> Self {
        Self {
            txn: ctx.txn,
            ns_id: ctx.ns_id,
            key_codec: ctx.key_codec,
            now: ctx.now,
            seen_hashes: HashSet::new(),
            content_cache: ctx.content_cache,
        }
    }

    pub async fn apply_write<'b>(
        &mut self,
        write_reqs: &[WriteRequest<'b>],
    ) -> anyhow::Result<Vec<WriteResponse>> {
        for req in write_reqs {
            if req.data.len() > MAX_PAGE_SIZE {
                anyhow::bail!(
                    "page is too large: {} bytes (limit {})",
                    req.data.len(),
                    MAX_PAGE_SIZE
                );
            }
        }

        let write_reqs = write_reqs
            .iter()
            .map(|req| (req, blake3::hash(req.data)))
            .collect::<Vec<_>>();
        let pregenerated_res = write_reqs
            .iter()
            .map(|req| WriteResponse {
                hash: req.1.as_bytes().to_vec(),
            })
            .collect::<Vec<_>>();

        // Here we filter by `seen_hashes`, because writing a same hash twice in the same transaction will fail
        // since we cannot read from a key previously written with `SetVersionstampedValue`.
        let write_reqs = write_reqs
            .iter()
            .dedup_by(|a, b| a.1 == b.1)
            .filter(|x| !self.seen_hashes.contains(&x.1))
            .map(|x| WriteContext {
                req: x.0,
                hash: x.1,
                content_key: self
                    .key_codec
                    .construct_content_key(self.ns_id, *x.1.as_bytes()),
                content_index_key: self
                    .key_codec
                    .construct_contentindex_key(self.ns_id, *x.1.as_bytes()),
                early_completion: AtomicBool::new(false),
            })
            .collect::<Vec<_>>();

        // This is not only an optimization. Without doing this check it is possible to form
        // loops in delta page construction.
        {
            let mut fut = FuturesUnordered::new();
            for req in &write_reqs {
                fut.push(async {
                    match self.txn.get(&req.content_index_key, false).await {
                        Ok(x) => {
                            if x.is_some() {
                                req.early_completion.store(true, Ordering::Relaxed);
                            }
                            Ok(())
                        }
                        Err(e) => {
                            tracing::warn!(ns = hex::encode(&self.ns_id), error = %e, "error getting content");
                            Err(anyhow::Error::from(e))
                        }
                    }
                });
            }
            while let Some(res) = fut.next().await {
                res?;
            }
        }

        // Attempt delta-encoding
        {
            let mut fut = FuturesUnordered::new();
            for req in &write_reqs {
                if req.early_completion.load(Ordering::Relaxed) {
                    continue;
                }

                fut.push(async {
                    let writer = DeltaWriter {
                        txn: self.txn,
                        ns_id: self.ns_id,
                        key_codec: self.key_codec,
                        content_cache: self.content_cache,
                    };
                    if let Some(delta_base_index) = req.req.delta_base {
                        match writer.delta_encode(delta_base_index, &req.req.data).await {
                            Ok(x) => {
                                if let Some((x, delta_base_hash)) = x {
                                    let delta_referrer_key =
                                        self.key_codec.construct_delta_referrer_key(
                                            self.ns_id,
                                            *req.hash.as_bytes(),
                                        );
                                    self.txn.set(&req.content_key, &x);
                                    self.txn.set(&delta_referrer_key, &delta_base_hash);
                                    let base_content_index_key = self
                                        .key_codec
                                        .construct_contentindex_key(self.ns_id, delta_base_hash);
                                    self.txn.atomic_op(
                                        &base_content_index_key,
                                        &ContentIndex::generate_mutation_payload(self.now),
                                        MutationType::SetVersionstampedValue,
                                    );
                                    req.early_completion.store(true, Ordering::Relaxed);
                                    Ok(Some(delta_base_hash))
                                } else {
                                    Ok(None)
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    ns = hex::encode(&self.ns_id),
                                    error = %e,
                                    "delta encoding failed"
                                );
                                Err(e)
                            }
                        }
                    } else {
                        Ok(None)
                    }
                });
            }

            let mut delta_base_hashes: Vec<Hash> = Vec::new();

            while let Some(delta_base_hash) = fut.next().await {
                let delta_base_hash = delta_base_hash?;
                if let Some(delta_base_hash) = delta_base_hash {
                    delta_base_hashes.push(delta_base_hash.into());
                }
            }

            drop(fut);
            for h in &delta_base_hashes {
                self.seen_hashes.insert(*h);
            }
        }
        // Finally...
        for req in &write_reqs {
            if !req.early_completion.load(Ordering::Relaxed) {
                self.txn
                    .set(&req.content_key, &Page::compress_zstd(req.req.data));
            }
            // Set content index
            self.txn.atomic_op(
                &req.content_index_key,
                &ContentIndex::generate_mutation_payload(self.now),
                MutationType::SetVersionstampedValue,
            );
            self.seen_hashes.insert(req.hash);
        }

        Ok(pregenerated_res)
    }
}

impl Core {
    /// Applies a batch of page writes to `ns_id` in a single FDB transaction
    /// and commits it. Mirrors mvstore's old `/batch/write` handler
    /// (`batch_write`), minus the streaming request/response framing - takes
    /// and returns plain in-memory `Vec`s since there's no wire in Direct
    /// mode.
    pub async fn write_pages(
        &self,
        ns_id: [u8; 10],
        now: Duration,
        write_reqs: &[WriteRequest<'_>],
    ) -> AnyResult<Vec<WriteResponse>> {
        let txn = self.db.create_trx()?;
        // It's safe to set CRR for the write path. Seeing stale data doesn't affect correctness.
        txn.set_option(TransactionOption::CausalReadRisky).unwrap();

        let mut applier = WriteApplier::new(WriteApplierContext {
            txn: &txn,
            ns_id,
            key_codec: &self.key_codec,
            now,
            content_cache: self.content_cache.as_ref(),
        });
        let res = applier
            .apply_write(write_reqs)
            .await
            .with_context(|| "cannot apply write")?;

        txn.commit()
            .await
            .map_err(FdbError::from)
            .with_context(|| "error committing transaction")?;

        Ok(res)
    }
}
