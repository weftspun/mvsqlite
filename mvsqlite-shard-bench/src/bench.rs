use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use futures::future::join_all;
use mvclient::{CommitOutput, MultiVersionClient};
use rand::{thread_rng, Rng};

/// Payload encoding shared by both benchmarks: 1-byte tag + 4-byte shard index (LE).
/// tag=1 -> entity is owned here. tag=0 -> entity was tombstoned (moved away) here.
/// A never-written page reads back as an empty (zero-length) payload, distinct from
/// both of the above, so presence-vs-absence is unambiguous.
fn encode_owned(shard_idx: u32) -> Vec<u8> {
    let mut v = vec![1u8];
    v.extend_from_slice(&shard_idx.to_le_bytes());
    v
}

fn encode_tombstone() -> Vec<u8> {
    vec![0u8, 0, 0, 0, 0]
}

fn decode(payload: &[u8]) -> Option<(bool, u32)> {
    if payload.len() != 5 {
        return None;
    }
    let owned = payload[0] == 1;
    let idx = u32::from_le_bytes(payload[1..5].try_into().unwrap());
    Some((owned, idx))
}

struct LatencyStats {
    count: AtomicU64,
    total_us: AtomicU64,
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
        }
    }

    fn record(&self, d: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us
            .fetch_add(d.as_micros() as u64, Ordering::Relaxed);
    }

    fn report(&self, label: &str, elapsed: Duration) {
        let count = self.count.load(Ordering::Relaxed);
        let total_us = self.total_us.load(Ordering::Relaxed);
        let avg_us = if count > 0 { total_us / count } else { 0 };
        let ops = if elapsed.as_secs_f64() > 0.0 {
            count as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        tracing::info!(
            label,
            count,
            avg_latency_us = avg_us,
            ops_per_sec = ops,
            elapsed_s = elapsed.as_secs_f64(),
            "BENCH result"
        );
    }
}

async fn setup_entities(
    clients: &[Arc<MultiVersionClient>],
    num_entities: u32,
) -> Result<HashMap<u32, u32>> {
    let mut owner: HashMap<u32, u32> = HashMap::with_capacity(num_entities as usize);
    for entity_id in 0..num_entities {
        let home = thread_rng().gen_range(0..clients.len() as u32);
        let mut txn = clients[home as usize].create_transaction(None).await?;
        txn.write_many(&[(entity_id, encode_owned(home).as_slice())])
            .await?;
        match txn.commit(None, &Default::default()).await? {
            CommitOutput::Committed(_) => {}
            other => anyhow::bail!("setup write for entity {entity_id} did not commit: {:?}",
                match other { CommitOutput::Conflict => "Conflict", CommitOutput::Empty => "Empty", _ => "?" }),
        }
        owner.insert(entity_id, home);
    }
    Ok(owner)
}

pub async fn run_scatter_gather(
    clients: Vec<Arc<MultiVersionClient>>,
    num_entities: u32,
    concurrency: usize,
    iterations: usize,
) -> Result<()> {
    let owner = setup_entities(&clients, num_entities).await?;
    tracing::info!(num_entities, num_shards = clients.len(), "scatter-gather setup done");

    let stats = Arc::new(LatencyStats::new());
    let clients = Arc::new(clients);
    let owner = Arc::new(owner);
    let mismatches = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let workers = (0..concurrency).map(|_| {
        let clients = clients.clone();
        let owner = owner.clone();
        let stats = stats.clone();
        let mismatches = mismatches.clone();
        tokio::spawn(async move {
            for _ in 0..iterations {
                let entity_id = thread_rng().gen_range(0..owner.len() as u32);
                let expected_home = owner[&entity_id];

                let t0 = Instant::now();
                // Fan out to every shard concurrently and merge - the actual
                // scatter-gather pattern being benchmarked.
                let reads = join_all(
                    clients
                        .iter()
                        .map(|c| async move { c.create_transaction(None).await }),
                )
                .await;

                let mut found: Vec<(usize, Vec<u8>)> = Vec::new();
                let read_futs = reads.into_iter().enumerate().filter_map(|(i, r)| {
                    r.ok().map(|txn| async move {
                        let data = txn.read_many_nomark(&[entity_id]).await;
                        (i, data)
                    })
                });
                for (i, data) in join_all(read_futs).await {
                    if let Ok(pages) = data {
                        if let Some(page) = pages.into_iter().next() {
                            if !page.is_empty() {
                                found.push((i, page));
                            }
                        }
                    }
                }
                stats.record(t0.elapsed());

                let owned_here: Vec<usize> = found
                    .iter()
                    .filter_map(|(i, p)| decode(p).filter(|(owned, _)| *owned).map(|_| *i))
                    .collect();
                if owned_here.len() != 1 || owned_here[0] != expected_home as usize {
                    mismatches.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    });
    for w in workers {
        w.await?;
    }
    let elapsed = start.elapsed();
    stats.report("scatter_gather", elapsed);

    let mismatch_count = mismatches.load(Ordering::Relaxed);
    if mismatch_count > 0 {
        tracing::error!(mismatch_count, "scatter-gather found ownership mismatches");
    } else {
        tracing::info!("scatter-gather: all reads matched expected ownership");
    }

    Ok(())
}

pub async fn run_transfer(
    clients: Vec<Arc<MultiVersionClient>>,
    num_entities: u32,
    concurrency: usize,
    iterations: usize,
) -> Result<()> {
    let owner = setup_entities(&clients, num_entities).await?;
    tracing::info!(num_entities, num_shards = clients.len(), "transfer setup done");

    let clients = Arc::new(clients);
    let owner: Arc<Mutex<HashMap<u32, u32>>> = Arc::new(Mutex::new(owner));
    let stats = Arc::new(LatencyStats::new());
    let conflicts = Arc::new(AtomicU64::new(0));

    // Each worker owns a disjoint slice of entity IDs, so two workers never race on
    // the same entity's ownership bookkeeping - the benchmark still drives real
    // concurrent cross-shard commit load, just without a benchmark-harness-level
    // race that would be a bug in this tool, not in mvsqlite.
    let start = Instant::now();
    let workers = (0..concurrency).map(|worker_id| {
        let clients = clients.clone();
        let owner = owner.clone();
        let stats = stats.clone();
        let conflicts = conflicts.clone();
        let num_entities = num_entities;
        let concurrency = concurrency as u32;
        tokio::spawn(async move {
            for _ in 0..iterations {
                let entity_id = {
                    let offset = thread_rng().gen_range(0..(num_entities / concurrency).max(1));
                    (offset * concurrency + worker_id as u32) % num_entities
                };
                let current = *owner.lock().unwrap().get(&entity_id).unwrap();
                let mut target = thread_rng().gen_range(0..clients.len() as u32);
                if clients.len() > 1 {
                    while target == current {
                        target = thread_rng().gen_range(0..clients.len() as u32);
                    }
                }

                let t0 = match (
                    clients[current as usize].create_transaction(None).await,
                    clients[target as usize].create_transaction(None).await,
                ) {
                    (Ok(mut source_txn), Ok(mut destination_txn)) => {
                        if source_txn
                            .write_many(&[(entity_id, encode_tombstone().as_slice())])
                            .await
                            .is_err()
                            || destination_txn
                                .write_many(&[(entity_id, encode_owned(target).as_slice())])
                                .await
                                .is_err()
                        {
                            continue;
                        }
                        let t0 = Instant::now();
                        let result = clients[current as usize]
                            .commit_across_two_namespaces(None, source_txn, destination_txn)
                            .await;
                        match result {
                            Ok(CommitOutput::Committed(_)) => {
                                owner.lock().unwrap().insert(entity_id, target);
                            }
                            Ok(CommitOutput::Conflict) => {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                            }
                            _ => {}
                        }
                        t0
                    }
                    _ => continue,
                };
                stats.record(t0.elapsed());
            }
        })
    });
    for w in workers {
        w.await?;
    }
    let elapsed = start.elapsed();
    stats.report("commit_across_two_namespaces", elapsed);
    tracing::info!(
        conflicts = conflicts.load(Ordering::Relaxed),
        "transfer benchmark done"
    );

    Ok(())
}
