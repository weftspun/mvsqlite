use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;

use anyhow::Result;
use mvclient::{CommitError, CommitOutput, MultiVersionClient, Transaction};
use mvsqlite_core::Core;
use rand::{thread_rng, Rng, RngCore};

use crate::inmem::Inmem;

pub struct Tester {
    mem: RwLock<Inmem>,
    client: Arc<MultiVersionClient>,
    core: Arc<Core>,
    busy_versions: Mutex<BTreeMap<String, u64>>,
    config: TesterConfig,
}

pub struct TesterConfig {
    pub disable_ryw: bool,
    pub num_pages: u32,
    pub disable_read_set: bool,
    pub permit_bad_page_reference: bool,
}

impl Tester {
    pub fn new(client: Arc<MultiVersionClient>, core: Arc<Core>, config: TesterConfig) -> Arc<Self> {
        Arc::new(Self {
            mem: RwLock::new(Inmem::new()),
            client,
            core,
            busy_versions: Mutex::new(BTreeMap::new()),
            config,
        })
    }

    pub async fn run(self: &Arc<Self>, concurrency: usize, iterations: usize) {
        let truncate_worker = tokio::spawn(self.clone().truncate_worker());
        let delete_unreferenced_content_worker =
            tokio::spawn(self.clone().delete_unreferenced_content_worker());
        let sweep_idempotency_records_worker =
            tokio::spawn(self.clone().sweep_idempotency_records_worker());
        let handles = (0..concurrency)
            .map(|i| {
                let me = self.clone();
                tokio::spawn(me.task(i, iterations))
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        truncate_worker.abort();
        delete_unreferenced_content_worker.abort();
        sweep_idempotency_records_worker.abort();
    }

    async fn resolve_ns_id(&self) -> Result<[u8; 10]> {
        self.core
            .lookup_nskey(&self.client.config().ns_key, None)
            .await?
            .ok_or_else(|| anyhow::anyhow!("namespace not found"))
    }

    async fn truncate_worker(self: Arc<Self>) {
        loop {
            let sleep_dur_ms = rand::thread_rng().gen_range(1..1000);
            let sleep_dur = Duration::from_millis(sleep_dur_ms);
            tokio::time::sleep(sleep_dur).await;

            let mut remove_point: String;
            {
                let mut mem = self.mem.write().await;
                let mut versions = mem.versions.keys().cloned().collect::<Vec<_>>();
                versions.pop(); // never remove the latest version, if any

                if versions.len() == 0 {
                    continue;
                }
                let split_point = rand::thread_rng().gen_range(0..versions.len());
                remove_point = versions[split_point].clone();

                if let Some((k, _)) = self.busy_versions.lock().unwrap().iter().next() {
                    if *k < remove_point {
                        remove_point = k.clone();
                    }
                }
                let mut removals: HashSet<String> = HashSet::new();
                for x in &versions {
                    if *x < remove_point {
                        mem.versions.remove(x);
                        removals.insert(x.clone());
                    }
                }
                mem.version_list = mem
                    .version_list
                    .iter()
                    .filter(|x| !removals.contains(*x))
                    .cloned()
                    .collect::<Vec<_>>();
            }

            let ns_id = match self.resolve_ns_id().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::error!(error = %e, "failed to resolve namespace for truncation");
                    continue;
                }
            };
            let before_version = match mvsqlite_core::util::decode_version(&remove_point) {
                Ok(x) => x,
                Err(e) => {
                    tracing::error!(error = %e, "failed to decode truncation version");
                    continue;
                }
            };
            tracing::info!(remove_point = remove_point, "triggering truncation");
            match self
                .core
                .clone()
                .truncate_versions(false, ns_id, before_version, |_| {})
                .await
            {
                Ok(()) => {
                    tracing::info!("truncated namespace");
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to truncate namespace");
                }
            }
        }
    }

    async fn delete_unreferenced_content_worker(self: Arc<Self>) {
        loop {
            let sleep_dur_ms = rand::thread_rng().gen_range(1..5000);
            let sleep_dur = Duration::from_millis(sleep_dur_ms);
            tokio::time::sleep(sleep_dur).await;

            let ns_id = match self.resolve_ns_id().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::error!(error = %e, "failed to resolve namespace for duc");
                    continue;
                }
            };
            tracing::info!("triggering duc");
            match self
                .core
                .clone()
                .delete_unreferenced_content(false, ns_id, |_| {})
                .await
            {
                Ok(()) => {
                    tracing::info!("deleted unreferenced content");
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to delete unreferenced content");
                }
            }
        }
    }

    async fn sweep_idempotency_records_worker(self: Arc<Self>) {
        loop {
            let sleep_dur_ms = rand::thread_rng().gen_range(1..5000);
            let sleep_dur = Duration::from_millis(sleep_dur_ms);
            tokio::time::sleep(sleep_dur).await;

            let ns_id = match self.resolve_ns_id().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::error!(error = %e, "failed to resolve namespace for idempotency sweep");
                    continue;
                }
            };
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            tracing::info!("triggering idempotency record sweep");
            match self
                .core
                .clone()
                .sweep_idempotency_records(false, ns_id, now, |_| {})
                .await
            {
                Ok(()) => {
                    tracing::info!("swept idempotency records");
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to sweep idempotency records");
                }
            }
        }
    }

    fn acquire_version(&self, version: &str) {
        *self
            .busy_versions
            .lock()
            .unwrap()
            .entry(version.to_string())
            .or_default() += 1;
    }

    fn release_version(&self, version: &str) {
        let mut versions = self.busy_versions.lock().unwrap();
        let entry = versions.get_mut(version).unwrap();
        assert!(*entry > 0, "version {} is not acquired", version);
        *entry -= 1;
        if *entry == 0 {
            versions.remove(version);
        }
    }

    async fn task(self: Arc<Self>, task_id: usize, iterations: usize) -> Result<()> {
        let mut mem = self.mem.write().await;
        let mut txn = self.client.create_transaction()?;
        let mut txn_id = mem.start_transaction(txn.version());
        self.acquire_version(txn.version());
        drop(mem);

        let mut last_writes: Vec<Option<Vec<u8>>> = vec![None; self.config.num_pages as usize];
        for it in 0..iterations {
            let mode = rand::thread_rng().gen_range(0..11);
            tracing::debug!(task = task_id, iteration = it, mode = mode, "iteration");
            match mode {
                0..=5 => {
                    let num_reads_requested = rand::thread_rng().gen_range(1..=10);
                    let reads = (0..num_reads_requested)
                        .map(|_| rand::thread_rng().gen_range::<u32, _>(0..self.config.num_pages))
                        .filter(|x| !self.config.disable_ryw || !txn.page_is_written(*x))
                        .collect::<Vec<_>>();
                    if reads.len() == 0 {
                        continue;
                    }
                    for &id in &reads {
                        txn.mark_read(id);
                    }
                    let pages = match txn.read_many_nomark(&reads) {
                        Ok(x) => x,
                        Err(e) if mvclient::is_retryable(&e) => {
                            tracing::debug!(error = %e, "retryable read error, restarting transaction");
                            let mut mem = self.mem.write().await;
                            mem.drop_transaction(txn_id);
                            self.release_version(txn.version());
                            drop(mem);
                            (txn, txn_id) = self.create_transaction_random_base().await?;
                            continue;
                        }
                        Err(e) => return Err(e),
                    };
                    let mut mem = self.mem.write().await;
                    for (&index, page) in reads.iter().zip(pages.iter()) {
                        tracing::debug!(
                            task = task_id,
                            txn_id,
                            iteration = it,
                            index = index,
                            version = txn.version(),
                            "read"
                        );
                        mem.verify_page(txn_id, index, page, txn.version());
                    }
                }
                6..=7 => {
                    let num_writes = rand::thread_rng().gen_range(1..=10);
                    let writes = (0..num_writes)
                        .map(|_| {
                            let mut rng = rand::thread_rng();
                            let index = rng.gen_range::<u32, _>(0..self.config.num_pages);

                            // Test delta encoding
                            let data = {
                                let last_write = &last_writes[index as usize];
                                if last_write.is_some() && rng.gen_bool(0.2) {
                                    let mut data = last_write.as_ref().unwrap().clone();
                                    let start = rng.gen_range(0..data.len());
                                    let end = rng.gen_range(start..data.len());
                                    rng.fill_bytes(&mut data[start..end]);
                                    data
                                } else {
                                    let mut data = vec![0u8; 2048];
                                    rng.fill_bytes(&mut data);
                                    data
                                }
                            };
                            if rng.gen_bool(0.5) {
                                last_writes[index as usize] = Some(data.clone());
                            }

                            tracing::debug!(
                                task = task_id,
                                txn_id,
                                iteration = it,
                                index = index,
                                version = txn.version(),
                                "write"
                            );

                            (index, data)
                        })
                        .collect::<Vec<_>>();
                    let writes = writes
                        .iter()
                        .map(|(index, data)| (*index, data.as_slice()))
                        .collect::<Vec<_>>();
                    txn.write_many(&writes)?;
                    let mut mem = self.mem.write().await;
                    for &(index, data) in &writes {
                        mem.write_page(txn_id, index, data);
                    }
                }
                8 => {
                    let mut mem = self.mem.write().await;
                    let version = txn.version().to_string();
                    let txn_version = txn.version().to_string();
                    match txn.commit(None, &HashMap::new()) {
                        Ok(CommitOutput::Committed(info)) => {
                            mem.commit_transaction(txn_id, &info.version, txn_version.as_str());
                        }
                        Ok(CommitOutput::Conflict) => mem.drop_transaction(txn_id),
                        Ok(CommitOutput::Empty) => mem.drop_transaction(txn_id),
                        Err(e) => {
                            let permitted = self.config.permit_bad_page_reference
                                && matches!(
                                    e.downcast_ref::<CommitError>(),
                                    Some(CommitError::BadPageReference)
                                );
                            if permitted {
                                tracing::warn!("ignored bad page reference as requested");
                                mem.drop_transaction(txn_id);
                            } else {
                                return Err(e);
                            }
                        }
                    }
                    self.release_version(&version);
                    drop(mem);
                    tokio::task::yield_now().await;
                    (txn, txn_id) = self.create_transaction_random_base().await?;
                    tracing::debug!(version = txn.version(), "created txn");
                }
                9 => {
                    let mut mem = self.mem.write().await;
                    mem.drop_transaction(txn_id);
                    self.release_version(txn.version());
                    drop(mem);
                    tokio::task::yield_now().await;
                    (txn, txn_id) = self.create_transaction_random_base().await?;
                    tracing::debug!(version = txn.version(), "created txn");
                }
                10 => {
                    let dur_millis = rand::thread_rng().gen_range(1..100);
                    tokio::time::sleep(Duration::from_millis(dur_millis)).await;
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    async fn create_transaction_random_base(&self) -> Result<(Transaction, u64)> {
        let mut mem = self.mem.write().await;

        if thread_rng().gen_bool(0.5) {
            if let Some(version) = mem.pick_random_version() {
                let mut txn = self
                    .client
                    .create_transaction_at_version(version, false)?;
                let txn_id = mem.start_transaction(txn.version());
                self.acquire_version(txn.version());
                if !self.config.disable_read_set && thread_rng().gen_bool(0.5) {
                    txn.enable_read_set();
                }
                return Ok((txn, txn_id));
            }
        }

        let mut txn = self.client.create_transaction()?;
        let txn_id = mem.start_transaction(txn.version());
        self.acquire_version(txn.version());
        if !self.config.disable_read_set && thread_rng().gen_bool(0.5) {
            txn.enable_read_set();
        }
        Ok((txn, txn_id))
    }
}
