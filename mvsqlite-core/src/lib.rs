pub mod boot;
pub mod commit;
pub mod delta;
pub mod fixed;
pub mod gc;
pub mod keys;
pub mod lock;
pub mod metadata;
pub mod namespace;
pub mod nslock;
pub mod page;
pub mod read;
pub mod replica;
pub mod stat;
pub mod time2version;
pub mod util;
pub mod write;

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use bytes::Bytes;
use foundationdb::Database;
use moka::future::Cache;

use crate::{keys::KeyCodec, metadata::NamespaceMetadataCache, replica::ReplicaManager};

#[derive(Clone)]
pub struct CoreConfig {
    pub cluster: String,
    pub raw_data_prefix: String,
    pub metadata_prefix: String,
    pub read_only: bool,
    pub dr_tag: String,
    pub content_cache_size: usize,
    pub auto_create_ns: bool,
}

/// FDB-facing state and business logic shared between mvstore's HTTP server
/// and any client (e.g. mvclient's Direct transport) that talks to FDB
/// without going through mvstore at all. Contains no HTTP/axum/hyper types.
pub struct Core {
    pub db: Database,

    pub key_codec: Arc<KeyCodec>,

    pub(crate) nskey_cache: Cache<String, [u8; 10]>,
    pub(crate) read_version_cache: Cache<[u8; 10], i64>,
    pub read_version_and_nsid_to_lwv_cache: Cache<(i64, [u8; 10]), [u8; 10]>,
    pub replica_manager: Option<ReplicaManager>,
    pub ns_metadata_cache: NamespaceMetadataCache,

    pub content_cache: Option<Cache<[u8; 32], Bytes>>,

    pub auto_create_ns: bool,
}

impl Core {
    pub async fn open(config: CoreConfig) -> Result<Self> {
        let db = Database::new(Some(config.cluster.as_str()))
            .with_context(|| "cannot open fdb cluster")?;
        let raw_data_prefix = config.raw_data_prefix.as_bytes().to_vec();

        // Read DR replica UID
        let replica_manager = if config.read_only {
            Some(ReplicaManager::new(&db, &config.dr_tag).await?)
        } else {
            None
        };
        Ok(Self {
            db,
            key_codec: Arc::new(KeyCodec {
                raw_data_prefix,
                metadata_prefix: config.metadata_prefix,
            }),
            nskey_cache: Cache::builder()
                .time_to_live(Duration::from_secs(120))
                .time_to_idle(Duration::from_secs(5))
                .max_capacity(10000)
                .build(),
            // FDB read versions are valid for 5 seconds.
            // We conservatively cache them for only 2 seconds here. If for some reason
            // these versions still lived too long, FDB will error and the client will retry.
            read_version_cache: Cache::builder()
                .time_to_live(Duration::from_secs(2))
                .max_capacity(1000)
                .build(),
            read_version_and_nsid_to_lwv_cache: Cache::builder()
                .time_to_idle(Duration::from_secs(2))
                .build(),
            replica_manager,
            ns_metadata_cache: NamespaceMetadataCache::new(),
            content_cache: if config.content_cache_size > 0 {
                Some(
                    Cache::builder()
                        .max_capacity(config.content_cache_size as u64)
                        .build(),
                )
            } else {
                None
            },
            auto_create_ns: config.auto_create_ns,
        })
    }
}
