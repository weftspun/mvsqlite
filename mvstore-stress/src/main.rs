mod inmem;
mod tester;

use anyhow::Result;
use backtrace::Backtrace;
use clap::Parser;
use mvclient::{MultiVersionClient, MultiVersionClientConfig};
use mvsqlite_core::{Core, CoreConfig};
use std::sync::Arc;
use tester::{Tester, TesterConfig};
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

#[derive(Debug, Parser)]
#[clap(name = "mvstore-stress", about = "stress test mvsqlite-core directly against FDB")]
struct Opt {
    /// Path to FoundationDB cluster file.
    #[clap(
        long,
        default_value = "/etc/foundationdb/fdb.cluster",
        env = "MVSTORE_STRESS_FDB_CLUSTER"
    )]
    fdb_cluster: String,

    /// Data prefix. This value is NOT tuple-encoded, for maximum efficiency.
    #[clap(long, env = "MVSTORE_STRESS_RAW_DATA_PREFIX")]
    raw_data_prefix: String,

    /// Metadata prefix. This value is tuple-encoded as a string.
    #[clap(long, env = "MVSTORE_STRESS_METADATA_PREFIX")]
    metadata_prefix: String,

    /// Auto create namespace on request.
    #[clap(long)]
    auto_create_namespace: bool,

    /// Output log in JSON format.
    #[clap(long)]
    json: bool,

    /// Namespace key.
    #[clap(long, env = "NS_KEY")]
    ns_key: String,

    /// Number of concurrent tasks.
    #[clap(long)]
    concurrency: u64,

    /// Number of iterations.
    #[clap(long)]
    iterations: u64,

    /// Number of pages.
    #[clap(long)]
    pages: u32,

    /// Disable read-your-writes tests.
    #[clap(long)]
    disable_ryw: bool,

    /// Disable read sets.
    #[clap(long)]
    disable_read_set: bool,

    /// Tolerate "bad page reference" commit errors (a page in the read set
    /// or delta chain was pruned by GC before commit could validate against
    /// it) instead of treating them as test failures.
    #[clap(long)]
    permit_bad_page_reference: bool,

    /// Enable FDB client buggify (fault injection). DO NOT USE IN PRODUCTION!
    #[clap(long)]
    fdb_buggify: bool,

    /// Override mvsqlite_core::gc::GC_SCAN_BATCH_SIZE.
    #[clap(long)]
    knob_gc_scan_batch_size: Option<usize>,

    /// Override mvsqlite_core::gc::GC_FRESH_PAGE_TTL_SECS.
    #[clap(long)]
    knob_gc_fresh_page_ttl_secs: Option<u64>,

    /// Override mvsqlite_core::gc::IDEMPOTENCY_RECORD_TTL_SECS.
    #[clap(long)]
    knob_idempotency_record_ttl_secs: Option<u64>,

    /// Override mvsqlite_core::commit::COMMIT_MULTI_PHASE_THRESHOLD.
    #[clap(long)]
    knob_commit_multi_phase_threshold: Option<usize>,

    /// Override mvsqlite_core::commit::PLCC_READ_SET_SIZE_THRESHOLD.
    #[clap(long)]
    knob_plcc_read_set_size_threshold: Option<usize>,

    /// Override mvsqlite_core::nslock::NSLOCK_ROLLBACK_SCAN_BATCH_SIZE.
    #[clap(long)]
    knob_nslock_rollback_scan_batch_size: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();

    if opt.json {
        SubscriberBuilder::default()
            .with_env_filter(EnvFilter::from_default_env())
            .json()
            .init();
    } else {
        SubscriberBuilder::default()
            .with_env_filter(EnvFilter::from_default_env())
            .pretty()
            .init();
    }

    std::panic::set_hook(Box::new(|info| {
        let bt = Backtrace::new();
        tracing::error!(backtrace = ?bt, info = %info, "panic");
        std::process::abort();
    }));

    mvsqlite_core::boot::boot_with_buggify(opt.fdb_buggify);
    if opt.fdb_buggify {
        tracing::error!("fdb_buggify is enabled");
    }

    if let Some(x) = opt.knob_gc_scan_batch_size {
        mvsqlite_core::gc::GC_SCAN_BATCH_SIZE.store(x, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(value = x, "configured gc scan batch size");
    }
    if let Some(x) = opt.knob_gc_fresh_page_ttl_secs {
        mvsqlite_core::gc::GC_FRESH_PAGE_TTL_SECS.store(x, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(value = x, "configured gc fresh page ttl");
    }
    if let Some(x) = opt.knob_idempotency_record_ttl_secs {
        mvsqlite_core::gc::IDEMPOTENCY_RECORD_TTL_SECS.store(x, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(value = x, "configured idempotency record ttl");
    }
    if let Some(x) = opt.knob_commit_multi_phase_threshold {
        mvsqlite_core::commit::COMMIT_MULTI_PHASE_THRESHOLD
            .store(x, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(value = x, "configured commit multi-phase threshold");
    }
    if let Some(x) = opt.knob_plcc_read_set_size_threshold {
        mvsqlite_core::commit::PLCC_READ_SET_SIZE_THRESHOLD
            .store(x, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(value = x, "configured plcc read set size threshold");
    }
    if let Some(x) = opt.knob_nslock_rollback_scan_batch_size {
        mvsqlite_core::nslock::NSLOCK_ROLLBACK_SCAN_BATCH_SIZE
            .store(x, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(value = x, "configured nslock rollback scan batch size");
    }

    let core = Arc::new(
        Core::open(CoreConfig {
            cluster: opt.fdb_cluster.clone(),
            raw_data_prefix: opt.raw_data_prefix.clone(),
            metadata_prefix: opt.metadata_prefix.clone(),
            read_only: false,
            dr_tag: String::from("default"),
            content_cache_size: 0,
            auto_create_ns: opt.auto_create_namespace,
        })
        .await?,
    );

    let client = MultiVersionClient::new(
        MultiVersionClientConfig {
            ns_key: opt.ns_key.clone(),
            ns_key_hashproof: None,
            lock_owner: None,
        },
        core.clone(),
    )?;
    let t = Tester::new(
        client.clone(),
        core,
        TesterConfig {
            num_pages: opt.pages,
            disable_ryw: opt.disable_ryw,
            disable_read_set: opt.disable_read_set,
            permit_bad_page_reference: opt.permit_bad_page_reference,
        },
    );
    t.run(opt.concurrency as _, opt.iterations as _).await;
    println!("Test succeeded.");

    // Unlike the old HTTP-based client, this process boots FDB's C client
    // and network thread directly. std::process::exit() here would
    // terminate abruptly while that thread (and the Tokio runtime's worker
    // threads) may still be running, which segfaults under load. Returning
    // normally lets the Tokio runtime and FDB's network thread shut down in
    // an orderly way instead.
    Ok(())
}
