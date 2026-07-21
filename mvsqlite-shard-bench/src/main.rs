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
}

#[derive(Debug, Parser)]
#[clap(name = "mvsqlite-shard-bench", about = "benchmark cross-shard patterns")]
struct Opt {
    #[clap(long)]
    data_plane: String,

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

    let rc = reqwest::Client::new();
    let mut clients: Vec<Arc<MultiVersionClient>> = Vec::with_capacity(opt.num_shards as usize);
    for i in 0..opt.num_shards {
        let ns_key = format!("{}_{:04}", opt.ns_prefix, i);
        rc.post(format!("{}/api/create_namespace", opt.admin_api))
            .json(&serde_json::json!({ "key": ns_key }))
            .send()
            .await?;

        let client = MultiVersionClient::new(
            MultiVersionClientConfig {
                data_plane: vec![opt.data_plane.parse()?],
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
    }

    Ok(())
}
