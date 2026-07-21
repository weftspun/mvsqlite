use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::atomic::{AtomicUsize, Ordering},
    time::SystemTime,
};

use anyhow::Result;
use foundationdb::{
    options::{ConflictRangeType, MutationType, TransactionOption},
    FdbError,
};
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use rand::RngCore;

use crate::{
    delta::reader::DeltaReader,
    keys::LWV_SHARD_COUNT,
    server::Server,
    util::{decode_version, generate_suffix_versionstamp_atomic_op, GoneError},
    util::{get_last_write_version, get_txn_read_version_as_versionstamp, ContentIndex},
    write::{WriteApplier, WriteApplierContext, WriteRequest},
};

pub static COMMIT_MULTI_PHASE_THRESHOLD: AtomicUsize = AtomicUsize::new(1000);
pub static PLCC_READ_SET_SIZE_THRESHOLD: AtomicUsize = AtomicUsize::new(2000);
pub static INTERVAL_ENTRY_MAX_SIZE: AtomicUsize = AtomicUsize::new(500);

pub enum CommitResult {
    Committed {
        versionstamp: [u8; 10],
        changelog: HashMap<String, Vec<u32>>,
    },
    Conflict,
    BadPageReference,
    NamespaceNotDistinct,
}

pub struct CommitContext<'a> {
    pub idempotency_key: [u8; 16],
    pub allow_skip_idempotency_check: bool,
    pub namespaces: &'a [CommitNamespaceContext<'a>],
    pub lock_owner: Option<&'a str>,
}

pub struct CommitNamespaceContext<'a> {
    pub ns_key: String,
    pub ns_id: [u8; 10],
    pub client_assumed_version: [u8; 10],
    pub use_read_set: bool,
    pub read_set: HashSet<u32>,
    pub page_writes: Vec<WriteRequest<'a>>,
    pub index_writes: Vec<(u32, [u8; 32])>,
    pub metadata: Option<String>,
}

impl Server {
    pub async fn commit<'a>(&self, ctx: CommitContext<'a>) -> Result<CommitResult> {
        let num_distinct_ns_id = ctx
            .namespaces
            .iter()
            .map(|x| x.ns_id)
            .collect::<HashSet<_>>()
            .len();
        if num_distinct_ns_id != ctx.namespaces.len() {
            // conflict with itself
            return Ok(CommitResult::NamespaceNotDistinct);
        }

        // Begin the writes.
        // We do two-phase commit (not that 2PC!) for large transactions here.
        let num_total_writes = ctx
            .namespaces
            .iter()
            .map(|x| x.index_writes.len())
            .sum::<usize>();
        let multi_phase = num_total_writes >= COMMIT_MULTI_PHASE_THRESHOLD.load(Ordering::Relaxed);
        let plcc_enable = !multi_phase
            && ctx
                .namespaces
                .iter()
                .map(|x| x.read_set.len())
                .sum::<usize>()
                <= PLCC_READ_SET_SIZE_THRESHOLD.load(Ordering::Relaxed);
        let mut commit_token = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut commit_token);
        tracing::debug!(
            multi_phase = multi_phase,
            commit_token = hex::encode(&commit_token),
            num_namespaces = ctx.namespaces.len(),
            total_writes = num_total_writes,
            "entering commit"
        );

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        // Phase 1 - Fast writes & check page existence
        let mut txn = self.db.create_trx()?;
        txn.set_option(TransactionOption::CausalReadRisky).unwrap();
        let txn_rv = get_txn_read_version_as_versionstamp(&txn).await?;

        let mut phase_1_ci_get_futures = FuturesOrdered::new();
        for ns in ctx.namespaces.iter() {
            if txn_rv < ns.client_assumed_version {
                tracing::error!(
                    our_version = hex::encode(&txn_rv),
                    client_assumed_version = hex::encode(&ns.client_assumed_version),
                    "we are behind the client - causal read fault?"
                );
                return Ok(CommitResult::Conflict);
            }

            let mut fast_write_hashes: HashSet<[u8; 32]> = HashSet::new();
            if !ns.page_writes.is_empty() {
                let mut applier = WriteApplier::new(WriteApplierContext {
                    txn: &txn,
                    ns_id: ns.ns_id,
                    key_codec: &self.key_codec,
                    now,
                    content_cache: self.content_cache.as_ref(),
                });
                let res = applier.apply_write(&ns.page_writes).await;
                match res {
                    Some(write_res) => {
                        for res in &write_res {
                            fast_write_hashes.insert(res.hash[..].try_into().unwrap());
                        }
                    }
                    None => {
                        anyhow::bail!("fast write failed");
                    }
                }
            }
            for (_, page_hash) in &ns.index_writes {
                if fast_write_hashes.contains(page_hash) {
                    continue;
                }
                let content_index_key = self
                    .key_codec
                    .construct_contentindex_key(ns.ns_id, *page_hash);
                phase_1_ci_get_futures.push_back(txn.get(&content_index_key, false));
            }
        }

        while let Some(data) = phase_1_ci_get_futures.next().await {
            if data?.is_none() {
                return Ok(CommitResult::BadPageReference);
            }
        }

        if multi_phase {
            let commit_token_keys = ctx
                .namespaces
                .iter()
                .map(|x| self.key_codec.construct_ns_commit_token_key(x.ns_id))
                .collect::<Vec<_>>();
            for k in &commit_token_keys {
                txn.set(k, &commit_token);
            }
            let committed = txn.commit().await.map_err(|e| FdbError::from(e))?;
            let committed_version = committed.committed_version()?;
            txn = committed.reset();
            txn.set_read_version(committed_version);
            let mut current_tokens = FuturesOrdered::new();
            for k in &commit_token_keys {
                current_tokens.push_back(txn.get(k, false));
            }
            while let Some(t) = current_tokens.next().await {
                if t?.as_ref().map(|x| &x[..]).unwrap_or_default() != commit_token {
                    anyhow::bail!("commit interrupted before phase 2");
                }
            }
            tracing::debug!(commit_token = hex::encode(&commit_token), "commit phase 2");
        }

        // Phase 2 - content index insertion

        for ns in ctx.namespaces {
            let metadata = self
                .ns_metadata_cache
                .get(&txn, &self.key_codec, ns.ns_id)
                .await?;

            // Does the client own the lock, if any?
            if let Some(lock) = &metadata.lock {
                match ctx.lock_owner {
                    Some(lock_owner) => {
                        // Validate lock ownership and state
                        if lock.owner.as_str() != lock_owner {
                            return Err(GoneError("you no longer own the lock").into());
                        }
                        if lock.rolling_back {
                            return Err(GoneError("rolling back").into());
                        }
                    }
                    None => {
                        return Ok(CommitResult::Conflict);
                    }
                }
            } else {
                if ctx.lock_owner.is_some() {
                    return Err(GoneError("you do not own the lock").into());
                }
            }

            let mut written_pages: BTreeSet<u32> = BTreeSet::new();

            let plcc_enable_ns = plcc_enable && ns.use_read_set;

            // Idempotency check & non-PLCC (database-level) conflict check.
            //
            // These used to be one read/write against a single shared per-namespace key,
            // which made that key a write hot-spot under concurrent commits regardless of
            // whether PLCC was in use. They are now two independent, non-contended paths:
            //
            // - Idempotency is a point lookup keyed by the (random, per-commit) idempotency
            //   key itself, so it's never hot.
            // - The LWV comparison, when needed, reads the max version across
            //   `LWV_SHARD_COUNT` shard keys instead of one, spreading write load across
            //   shards while still seeing every commit regardless of which shard it landed on.
            {
                // The client does not allow us to skip idempotency check. This happens when
                // the client is retrying a commit.
                if !ctx.allow_skip_idempotency_check {
                    let idempotency_key = self
                        .key_codec
                        .construct_idempotency_key(ns.ns_id, ctx.idempotency_key);
                    if let Some(t) = txn.get(&idempotency_key, false).await? {
                        if let Ok(ci) = ContentIndex::decode(&t) {
                            // This is an idempotent retry - return conservative values
                            return Ok(CommitResult::Committed {
                                versionstamp: ci.versionstamp,
                                changelog: HashMap::new(),
                            });
                        }
                    }
                }

                // This is not a PLCC commit. Database-level conflict check works by comparing
                // against the max version across all LWV shards.
                if !plcc_enable_ns {
                    let actual_last_write_version =
                        get_last_write_version(&txn, &self.key_codec, ns.ns_id, false).await?;
                    if ns.client_assumed_version < actual_last_write_version {
                        return Ok(CommitResult::Conflict);
                    }
                }

                // Always record this commit's version, regardless of PLCC state, so that:
                // (a) a future retry with this idempotency key can be detected, and
                // (b) a future non-PLCC commit's LWV comparison sees this write.
                //
                // The idempotency record uses the same [time_secs(8) | versionstamp(10)]
                // encoding as ContentIndex, so it can be GC'd by age the same way.
                let idempotency_key = self
                    .key_codec
                    .construct_idempotency_key(ns.ns_id, ctx.idempotency_key);
                let idempotency_atomic_op_payload = ContentIndex::generate_mutation_payload(now);
                txn.atomic_op(
                    &idempotency_key,
                    &idempotency_atomic_op_payload,
                    MutationType::SetVersionstampedValue,
                );

                // The LWV shard just needs the bare version — a 10-byte versionstamp
                // placeholder plus a 4-byte LE offset (=0) telling FDB where to fill it in.
                let mut lwv_value = [0u8; 10 + 4];
                lwv_value[10..14].copy_from_slice(&0u32.to_le_bytes());
                let lwv_shard = ctx.idempotency_key[0] % LWV_SHARD_COUNT;
                let lwv_shard_key = self.key_codec.construct_lwv_shard_key(ns.ns_id, lwv_shard);
                txn.atomic_op(
                    &lwv_shard_key,
                    &lwv_value,
                    MutationType::SetVersionstampedValue,
                );
            }

            // Fine-grained conflict check
            if plcc_enable_ns {
                let reader = DeltaReader {
                    txn: &txn,
                    key_codec: &self.key_codec,
                    ns_id: ns.ns_id,
                    replica_manager: None,
                    content_cache: self.content_cache.as_ref(),
                };
                let mut fut_list = FuturesOrdered::new();
                for &page in &ns.read_set {
                    let read_page_hash_fut = reader.read_page_hash(page, None, false);
                    fut_list.push_back(async move { (page, read_page_hash_fut.await) });
                }

                while let Some((page, data)) = fut_list.next().await {
                    let data = data?;
                    if let Some((version, _)) = data {
                        let version = decode_version(&version)?;
                        if version > ns.client_assumed_version {
                            tracing::warn!(
                                page,
                                page_version = hex::encode(&version),
                                client_assumed_version = hex::encode(&ns.client_assumed_version),
                                "page-level conflict check failed"
                            );
                            return Ok(CommitResult::Conflict);
                        }
                    }
                }
            }

            for (page_index, page_hash) in &ns.index_writes {
                let page_key_template =
                    self.key_codec
                        .construct_page_key(ns.ns_id, *page_index, [0u8; 10]);
                let page_key_atomic_op = generate_suffix_versionstamp_atomic_op(&page_key_template);
                let ci_key = self
                    .key_codec
                    .construct_contentindex_key(ns.ns_id, *page_hash);
                let ci_atomic_op = ContentIndex::generate_mutation_payload(now);
                if multi_phase {
                    txn.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                }
                txn.atomic_op(
                    &page_key_atomic_op,
                    page_hash,
                    MutationType::SetVersionstampedKey,
                );
                if multi_phase {
                    txn.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                }
                txn.atomic_op(&ci_key, &ci_atomic_op, MutationType::SetVersionstampedValue);
                written_pages.insert(*page_index);
            }
            if multi_phase {
                txn.add_conflict_range(
                    &self
                        .key_codec
                        .construct_page_key(ns.ns_id, std::u32::MIN, [0u8; 10]),
                    &self
                        .key_codec
                        .construct_page_key(ns.ns_id, std::u32::MAX, [0xffu8; 10]),
                    ConflictRangeType::Write,
                )?;
                txn.add_conflict_range(
                    &self
                        .key_codec
                        .construct_contentindex_key(ns.ns_id, [0u8; 32]),
                    &self
                        .key_codec
                        .construct_contentindex_key(ns.ns_id, [0xffu8; 32]),
                    ConflictRangeType::Write,
                )?;
            }

            let mut changelog: Vec<u8> = vec![];

            if written_pages.len() >= INTERVAL_ENTRY_MAX_SIZE.load(Ordering::Relaxed) {
                changelog.push(1); // infinite
            } else {
                changelog.reserve(1 + written_pages.len() * 4);
                changelog.push(0);
                for index in written_pages {
                    changelog.extend_from_slice(&index.to_be_bytes());
                }
            }

            let changelog_atomic_op_key = generate_suffix_versionstamp_atomic_op(
                &self.key_codec.construct_changelog_key(ns.ns_id, [0u8; 10]),
            );
            txn.atomic_op(
                &changelog_atomic_op_key,
                &changelog,
                MutationType::SetVersionstampedKey,
            );
        }

        let versionstamp_fut = txn.get_versionstamp();
        let commit_result = txn.commit().await.map_err(|e| FdbError::from(e))?;
        let versionstamp = versionstamp_fut.await?;

        // Read changelog
        let mut changelog: HashMap<String, Vec<u32>> = HashMap::new();
        {
            let committed_version = commit_result.committed_version()?;
            let txn = commit_result.reset();
            txn.set_read_version(committed_version);
            for ns in ctx.namespaces {
                let interval = self
                    .read_interval(
                        &txn,
                        ns.ns_id,
                        ns.client_assumed_version,
                        versionstamp[..].try_into().unwrap(),
                        false,
                    )
                    .await?;
                if let Some(interval) = interval {
                    changelog.insert(ns.ns_key.clone(), interval);
                }
            }
        }
        Ok(CommitResult::Committed {
            versionstamp: <[u8; 10]>::try_from(&versionstamp[..]).unwrap(),
            changelog,
        })
    }
}
