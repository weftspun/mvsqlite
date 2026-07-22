mod backoff;

use anyhow::{Context, Result};
use backoff::RandomizedExponentialBackoff;
use bytes::Bytes;
use foundationdb::FdbError;
use mvsqlite_core::{
    commit::{CommitContext, CommitNamespaceContext, CommitResult as CoreCommitResult},
    namespace::NamespaceLookup,
    read::ReadPageRequest,
    util::decode_version,
    write::WriteRequest as CoreWriteRequest,
    Core,
};
use rand::RngCore;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CommitError {
    #[error("commit error: {0}")]
    Failed(String),

    /// A page in the read set (or a delta chain) was pruned by GC before
    /// this commit could validate against it. Corresponds to the old HTTP
    /// transport's 410 Gone response.
    #[error("bad page reference")]
    BadPageReference,

    #[error("namespace not distinct")]
    NamespaceNotDistinct,
}

pub struct MultiVersionClient {
    core: Arc<Core>,
    config: MultiVersionClientConfig,
}

#[derive(Clone, Debug)]
pub struct MultiVersionClientConfig {
    /// Namespace key.
    pub ns_key: String,

    pub ns_key_hashproof: Option<String>,

    pub lock_owner: Option<String>,
}

pub struct StatResponse {
    pub version: String,
    pub metadata: String,
    pub read_only: bool,
    pub interval: Option<Vec<u32>>,
}

pub struct WriteRequest<'a> {
    pub data: &'a [u8],
    pub delta_base: Option<u32>,
}

pub struct WriteResponse {
    pub hash: Vec<u8>,
}

pub struct CommitNamespaceInit {
    pub ns_key: String,
    pub ns_key_hashproof: Option<String>,
    pub version: String,
    pub metadata: Option<String>,
    pub num_pages: u32,
    pub read_set: Option<HashSet<u32>>,
}

pub struct CommitRequest {
    pub page_index: u32,
    pub hash: Vec<u8>,
    pub data: Option<Bytes>,
}

pub struct CommitResult {
    pub version: String,
    pub duration: Duration,
    pub num_pages: u64,
    pub changelog: HashMap<String, Vec<u32>>,
}

pub enum CommitOutput {
    Committed(CommitResult),
    Conflict,
    Empty,
}

/// A commit intent for one namespace, ready to be applied by
/// `MultiVersionClient::apply_commit_intents`. `ns_id` is resolved once (at
/// `create_transaction_with_info` time, cached by `Core::lookup_nskey`) and
/// carried alongside `init` so `apply_commit_intents` doesn't need to
/// re-resolve `ns_key` -> `ns_id` on every commit.
pub struct NamespaceCommitIntent {
    pub ns_id: [u8; 10],
    pub init: CommitNamespaceInit,
    pub requests: Vec<CommitRequest>,
}

pub struct TimeToVersionResponse {
    pub after: Option<TimeToVersionPoint>,
    pub not_after: Option<TimeToVersionPoint>,
}

pub struct TimeToVersionPoint {
    pub version: String,
    pub time: u64,
}

/// True if retrying the FDB operation that produced this error might
/// succeed - i.e. it's a transient/conflict error, not a logic error.
pub fn is_retryable(e: &anyhow::Error) -> bool {
    // `.with_context(...)` (used throughout mvsqlite-core's read/commit
    // paths) wraps the real error, changing the top-level type - a plain
    // `e.downcast_ref::<FdbError>()` only checks that top-level type and
    // misses a retryable FdbError that's still present as the source
    // further down the chain. Search the whole chain instead.
    e.chain()
        .find_map(|cause| cause.downcast_ref::<FdbError>())
        .map(|x| x.is_retryable())
        .unwrap_or(false)
}

/// The single place every FDB operation in this crate retries through.
/// Calls `op` until it returns `Ok`, a non-retryable `Err`, or the outer
/// retry loop otherwise decides to give up (currently: never - `op` is
/// expected to be a single FDB attempt, and giving up is `op`'s decision to
/// make by returning a non-retryable error, e.g. a real conflict).
///
/// This exists so every FDB-calling method shares one retry policy instead
/// of each hand-rolling its own copy of the same loop - a duplicated loop
/// is a duplicated place to forget the retry entirely, which is exactly how
/// three of this crate's call sites (`create_transaction_with_info`,
/// `create_transaction_at_version`, `time2version`) ended up with no retry
/// protection at all until this was pulled out.
fn with_fdb_retry<T>(mut op: impl FnMut() -> Result<T>) -> Result<T> {
    let mut boff = RandomizedExponentialBackoff::default();
    loop {
        match op() {
            Ok(x) => return Ok(x),
            Err(e) if is_retryable(&e) => {
                tracing::debug!(error = %e, "retryable FDB error, retrying");
                boff.wait();
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Blocks the calling thread on `fut` using a minimal, runtime-agnostic
/// executor (no Tokio task queue/scheduler) - `foundationdb`'s futures are
/// explicitly not tied to any async runtime, so this drives them to
/// completion the same way FDB's C/Java clients do: the calling thread parks
/// until FDB's own completion callback wakes it directly, with no extra
/// wake -> reschedule -> poll hop through a separate executor's run loop.
/// This is what makes mvclient (and therefore the whole SQLite VFS path)
/// synchronous - no `IoEngine`/Tokio runtime is needed to drive it at all.
fn block_on<T>(fut: impl std::future::Future<Output = T>) -> T {
    futures::executor::block_on(fut)
}

impl MultiVersionClient {
    /// `core` is a shared FDB connection - callers should open one `Core`
    /// per process (see `mvsqlite-core::Core::open`) and pass the same
    /// `Arc<Core>` to every `MultiVersionClient` they construct, rather than
    /// opening a new FDB connection per client/namespace.
    pub fn new(config: MultiVersionClientConfig, core: Arc<Core>) -> Result<Arc<Self>> {
        Ok(Arc::new(Self { core, config }))
    }

    pub fn config(&self) -> &MultiVersionClientConfig {
        &self.config
    }

    pub fn create_transaction(self: &Arc<Self>) -> Result<Transaction> {
        self.create_transaction_with_info(None).map(|x| x.0)
    }

    pub fn create_transaction_with_info(
        self: &Arc<Self>,
        from_version: Option<&str>,
    ) -> Result<(Transaction, TransactionInfo)> {
        let lookup = with_fdb_retry(|| {
            block_on(self.core.resolve_namespace_and_stat(
                &self.config.ns_key,
                self.config.ns_key_hashproof.as_deref(),
                from_version.unwrap_or(""),
                false,
                self.config.lock_owner.as_deref().unwrap_or(""),
            ))
        })?;
        let (ns_id, stat) = match lookup {
            NamespaceLookup::Found { ns_id, stat } => (ns_id, stat),
            NamespaceLookup::NotFound => anyhow::bail!("namespace not found"),
        };

        tracing::debug!(
            version = stat.version,
            metadata = stat.metadata,
            "created transaction"
        );
        Ok((
            self.create_transaction_with_ns_id(ns_id, &stat.version, stat.read_only),
            TransactionInfo {
                metadata: stat.metadata,
                interval: stat.interval,
            },
        ))
    }

    fn create_transaction_with_ns_id(
        self: &Arc<Self>,
        ns_id: [u8; 10],
        version: &str,
        read_only: bool,
    ) -> Transaction {
        Transaction {
            c: self.clone(),
            ns_id,
            version: version.into(),
            page_buffer: HashMap::new(),
            seen_hashes: Mutex::new(HashSet::new()),
            read_only,
            read_set: None,
        }
    }

    /// Creates a transaction pinned to an already-known version, re-resolving
    /// `ns_key` -> `ns_id` (cheaply, via `Core`'s own cache) rather than
    /// requiring the caller to already have it.
    pub fn create_transaction_at_version(
        self: &Arc<Self>,
        version: &str,
        read_only: bool,
    ) -> Result<Transaction> {
        let ns_id = with_fdb_retry(|| {
            block_on(self.core.lookup_nskey(
                &self.config.ns_key,
                self.config.ns_key_hashproof.as_deref(),
            ))?
            .with_context(|| "namespace not found")
        })?;
        Ok(self.create_transaction_with_ns_id(ns_id, version, read_only))
    }

    pub fn apply_commit_intents(
        &self,
        intents: &[NamespaceCommitIntent],
    ) -> Result<Option<CommitResult>> {
        if intents.is_empty() {
            anyhow::bail!("no commit intents");
        }

        let start_time = Instant::now();
        let mut idempotency_key: [u8; 16] = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut idempotency_key);

        let total_num_pages: usize = intents.iter().map(|x| x.requests.len()).sum();

        let mut ns_contexts: Vec<CommitNamespaceContext> = Vec::with_capacity(intents.len());
        for intent in intents {
            let mut page_writes = Vec::new();
            let mut index_writes = Vec::with_capacity(intent.requests.len());
            for req in &intent.requests {
                let hash: [u8; 32] = req.hash[..]
                    .try_into()
                    .with_context(|| "invalid hash length")?;
                if let Some(data) = &req.data {
                    page_writes.push(CoreWriteRequest {
                        data: &data[..],
                        delta_base: Some(req.page_index),
                    });
                }
                index_writes.push((req.page_index, hash));
            }
            ns_contexts.push(CommitNamespaceContext {
                ns_key: intent.init.ns_key.clone(),
                ns_id: intent.ns_id,
                client_assumed_version: decode_version(&intent.init.version)?,
                use_read_set: intent.init.read_set.is_some(),
                read_set: intent.init.read_set.clone().unwrap_or_default(),
                page_writes,
                index_writes,
                metadata: intent.init.metadata.clone(),
            });
        }

        let mut is_retry = false;
        let outcome = with_fdb_retry(|| {
            let ctx = CommitContext {
                idempotency_key,
                // Skippable on the first attempt: idempotency_key is freshly
                // random and nothing has been written under it yet, so the
                // check is guaranteed to find nothing - paying for it would
                // only add a round trip to the common case (PLCC already
                // handles conflict detection on this path).
                //
                // NOT skippable on retry. This is a sequential gate, not a
                // parallel fallback: each retry's transaction first checks
                // (via a snapshot read, so it adds no new conflicts) whether
                // our own idempotency_key already committed, and only
                // proceeds to a normal commit if that check finds nothing.
                // Sequential-gate is the only version of this that's
                // actually safe - if the check and a fresh write attempt
                // could race instead of strictly ordering, a write could
                // land right after the check already returned "no match",
                // silently recreating the exact bug this fixes: a retry
                // after an ambiguous error (e.g. commit_unknown_result) that
                // actually succeeded in FDB would create a second, real,
                // untracked commit instead of discovering the first one
                // already went through.
                allow_skip_idempotency_check: !is_retry,
                namespaces: &ns_contexts,
                lock_owner: self.config.lock_owner.as_deref(),
            };
            is_retry = true;
            match block_on(self.core.commit(ctx))? {
                CoreCommitResult::BadPageReference => Err(CommitError::BadPageReference.into()),
                CoreCommitResult::NamespaceNotDistinct => {
                    Err(CommitError::NamespaceNotDistinct.into())
                }
                // Committed and Conflict are both terminal, non-retryable
                // outcomes, but not errors - resolve them through with_fdb_retry
                // like anything else that shouldn't loop again.
                outcome => Ok(outcome),
            }
        })?;
        match outcome {
            CoreCommitResult::Committed {
                versionstamp,
                changelog,
            } => {
                let committed_version = hex::encode(&versionstamp);
                tracing::debug!(version = committed_version, "committed transaction");
                Ok(Some(CommitResult {
                    version: committed_version,
                    duration: start_time.elapsed(),
                    num_pages: total_num_pages as u64,
                    changelog,
                }))
            }
            CoreCommitResult::Conflict => Ok(None),
            CoreCommitResult::BadPageReference | CoreCommitResult::NamespaceNotDistinct => {
                unreachable!("returned as Err above, not Ok")
            }
        }
    }

    pub fn time2version(self: &Arc<Self>, timestamp: u64) -> Result<TimeToVersionResponse> {
        let res = with_fdb_retry(|| block_on(self.core.time2version(timestamp)))?;
        Ok(TimeToVersionResponse {
            after: res.after.map(|x| TimeToVersionPoint {
                version: x.version,
                time: x.time,
            }),
            not_after: res.not_after.map(|x| TimeToVersionPoint {
                version: x.version,
                time: x.time,
            }),
        })
    }
}

pub struct Transaction {
    c: Arc<MultiVersionClient>,
    ns_id: [u8; 10],
    version: String,
    page_buffer: HashMap<u32, [u8; 32]>,
    seen_hashes: Mutex<HashSet<[u8; 32]>>,
    read_only: bool,
    read_set: Option<Mutex<HashSet<u32>>>,
}

pub struct TransactionInfo {
    pub metadata: String,
    pub interval: Option<Vec<u32>>,
}

impl Transaction {
    pub fn version(&self) -> &str {
        self.version.as_str()
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn page_is_written(&self, page_id: u32) -> bool {
        self.page_buffer.contains_key(&page_id)
    }

    pub fn written_pages(&self) -> Vec<u32> {
        self.page_buffer.keys().cloned().collect()
    }

    pub fn enable_read_set(&mut self) {
        if self.read_set.is_none() {
            self.read_set = Some(Mutex::new(HashSet::new()));
        }
    }

    pub fn disable_read_set(&mut self) {
        self.read_set = None;
    }

    pub fn read_set_size(&self) -> usize {
        self.read_set
            .as_ref()
            .map(|x| x.lock().unwrap().len())
            .unwrap_or(0)
    }

    pub fn is_read_set_enabled(&self) -> bool {
        self.read_set.is_some()
    }

    pub fn mark_read(&self, page_id: u32) {
        if let Some(read_set) = &self.read_set {
            read_set.lock().unwrap().insert(page_id);
        }
    }

    pub fn read_many_nomark(&self, page_id_list: &[u32]) -> Result<Vec<Vec<u8>>> {
        let core = &self.c.core;
        let results = block_on(async {
            let futs = page_id_list.iter().map(|&page_index| {
                let hash = self.page_buffer.get(&page_index).copied();
                let version = self.version.as_str();
                async move {
                    core.read_page(
                        self.ns_id,
                        ReadPageRequest {
                            page_index,
                            version,
                            hash,
                        },
                    )
                    .await
                }
            });
            futures::future::try_join_all(futs).await
        })?;

        let mut out = Vec::with_capacity(results.len());
        for page in results {
            let data = page.map(|x| x.data.to_vec()).unwrap_or_default();
            self.seen_hashes
                .lock()
                .unwrap()
                .insert(*blake3::hash(&data).as_bytes());
            out.push(data);
        }
        Ok(out)
    }

    /// Applies a batch of page writes immediately (synchronously) rather than
    /// backgrounding it - Direct-mode FDB writes are cheap enough (no HTTP
    /// round trip) that the old "background the write, synchronize before the
    /// next read/commit" pattern (which existed to hide HTTP latency) is no
    /// longer worth the extra bookkeeping it required.
    pub fn write_many(&mut self, raw_pages: &[(u32, &[u8])]) -> Result<()> {
        let all_pages = raw_pages
            .iter()
            .map(|&(page_index, data)| {
                let hash = blake3::hash(data);
                (page_index, data, hash)
            })
            .collect::<Vec<_>>();
        let pages_to_push: Vec<(&[u8], u32)> = all_pages
            .iter()
            .filter(|(_, _, hash)| !self.seen_hashes.lock().unwrap().contains(hash.as_bytes()))
            .map(|(page_index, data, _)| (*data, *page_index))
            .collect();

        for (page_index, _, hash) in &all_pages {
            self.page_buffer.insert(*page_index, *hash.as_bytes());
            self.seen_hashes.lock().unwrap().insert(*hash.as_bytes());
        }

        if pages_to_push.is_empty() {
            return Ok(());
        }

        let reqs: Vec<CoreWriteRequest> = pages_to_push
            .iter()
            .map(|(data, page_index)| CoreWriteRequest {
                data,
                delta_base: Some(*page_index),
            })
            .collect();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        with_fdb_retry(|| block_on(self.c.core.write_pages(self.ns_id, now, &reqs)))?;
        Ok(())
    }

    pub fn commit_intent(
        &self,
        metadata: Option<String>,
        fast_writes: &HashMap<u32, Bytes>,
    ) -> Result<Option<NamespaceCommitIntent>> {
        if self.page_buffer.is_empty() && metadata.is_none() && fast_writes.is_empty() {
            return Ok(None);
        }

        let total_num_pages = self.page_buffer.len() + fast_writes.len();

        let mut out = NamespaceCommitIntent {
            ns_id: self.ns_id,
            init: CommitNamespaceInit {
                version: self.version.clone(),
                metadata,
                num_pages: total_num_pages as u32,
                ns_key: self.c.config.ns_key.clone(),
                ns_key_hashproof: self.c.config.ns_key_hashproof.clone(),
                read_set: self.read_set.as_ref().map(|x| x.lock().unwrap().clone()),
            },
            requests: Vec::with_capacity(total_num_pages),
        };

        for (&page_index, data) in fast_writes {
            let req = CommitRequest {
                page_index,
                hash: blake3::hash(data).as_bytes().to_vec(),
                data: Some(data.clone()),
            };
            out.requests.push(req);
        }

        for (&page_index, hash) in self.page_buffer.iter() {
            if fast_writes.contains_key(&page_index) {
                continue;
            }

            let req = CommitRequest {
                page_index,
                hash: hash.to_vec(),
                data: None,
            };
            out.requests.push(req);
        }

        Ok(Some(out))
    }

    pub fn commit(
        self,
        metadata: Option<&str>,
        fast_writes: &HashMap<u32, Bytes>,
    ) -> Result<CommitOutput> {
        let intent = match self.commit_intent(metadata.map(|x| x.to_string()), fast_writes)? {
            Some(x) => x,
            None => return Ok(CommitOutput::Empty),
        };
        Ok(match self.c.apply_commit_intents(&[intent])? {
            Some(x) => CommitOutput::Committed(x),
            None => CommitOutput::Conflict,
        })
    }
}
