mod bench;

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use mvclient::{MultiVersionClient, MultiVersionClientConfig};

#[derive(Debug, Clone, clap::ValueEnum)]
enum Mode {
    /// Read the same entity from every shard concurrently and merge - the pattern
    /// used for cross-shard reads (leaderboards, "who's near a boundary").
    ScatterGather,
    /// Repeatedly move a synthetic row between two random shards via
    /// commit_across_two_namespaces, verifying it lands in exactly one shard
    /// afterward.
    Transfer,
    /// The actual linear-scaling test: a workloada-shaped (50% read / 50% update,
    /// uniform random key) workload where every worker is pinned to exactly one
    /// shard - no cross-shard access at all. --concurrency is TOTAL client
    /// concurrency, split evenly across shards, so it stays constant as
    /// --num-shards varies - run this at num-shards=1,2,4,8,... to see whether
    /// aggregate throughput actually scales with shard count.
    Scaling,
}

#[derive(Debug, Parser)]
#[clap(name = "mvsqlite-shard-bench", about = "benchmark cross-shard patterns")]
struct Opt {
    /// Comma-separated list of mvstore data-plane URLs. Shard i is pinned to
    /// data_planes[i % data_planes.len()] - pass one URL per mvstore process to
    /// test real horizontal scaling (more serving capacity as shard count grows),
    /// not just more FDB namespaces funneled through a single process.
    #[clap(long, value_delimiter = ',')]
    data_planes: Vec<String>,

    /// Any one mvstore instance's admin API - namespace registration lives in
    /// FDB, so every instance sharing the same metadata/raw-data prefix sees it.
    #[clap(long)]
    admin_api: String,

    /// Namespace prefix; shards are created as "<prefix>_0000".."<prefix>_NNNN".
    #[clap(long)]
    ns_prefix: String,

    #[clap(long)]
    num_shards: u32,

    #[clap(long)]
    concurrency: usize,

    #[clap(long)]
    iterations: usize,

    /// Number of distinct synthetic entities (page indices) to spread across shards.
    #[clap(long, default_value = "1000")]
    num_entities: u32,

    #[clap(long, value_enum)]
    mode: Mode,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opt = Opt::parse();

    if opt.data_planes.is_empty() {
        anyhow::bail!("--data-planes must have at least one URL");
    }

    let rc = reqwest::Client::new();
    let mut clients: Vec<Arc<MultiVersionClient>> = Vec::with_capacity(opt.num_shards as usize);
    for i in 0..opt.num_shards {
        let ns_key = format!("{}_{:04}", opt.ns_prefix, i);
        rc.post(format!("{}/api/create_namespace", opt.admin_api))
            .json(&serde_json::json!({ "key": ns_key }))
            .send()
            .await?;

        let data_plane = &opt.data_planes[(i as usize) % opt.data_planes.len()];
        let client = MultiVersionClient::new(
            MultiVersionClientConfig {
                data_plane: vec![data_plane.parse()?],
                ns_key,
                ns_key_hashproof: None,
                lock_owner: None,
            },
            reqwest::Client::new(),
        )?;
        clients.push(client);
    }

    match opt.mode {
        Mode::ScatterGather => {
            bench::run_scatter_gather(clients, opt.num_entities, opt.concurrency, opt.iterations)
                .await?
        }
        Mode::Transfer => {
            bench::run_transfer(clients, opt.num_entities, opt.concurrency, opt.iterations).await?
        }
        Mode::Scaling => {
            let concurrency_per_shard = (opt.concurrency / clients.len()).max(1);
            bench::run_scaling(
                clients,
                opt.num_entities,
                concurrency_per_shard,
                opt.iterations,
            )
            .await?
        }
    }

    Ok(())
}
