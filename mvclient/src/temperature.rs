use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use rand::Rng;

/// Local, in-memory, best-effort tracker of which pages recently caused commit
/// conflicts for this client process.
///
/// This is intentionally never synchronized across clients or persisted anywhere —
/// it's a per-process scheduling heuristic only, in the spirit of MOCC's page
/// "temperature" tracking. Keeping it purely local means it can never become a new
/// shared contention point: unlike a server-side/FDB-backed conflict counter, it adds
/// no new round trips and does not limit how many client processes can scale out.
pub struct PageTemperatureTracker {
    inner: Mutex<HashMap<u32, (u32, Instant)>>,
}

const TEMPERATURE_TTL: Duration = Duration::from_secs(5);
const MAX_TRACKED_PAGES: usize = 10_000;
const MAX_BACKOFF: Duration = Duration::from_millis(20);
const MAX_HEAT: u32 = 20;

impl Default for PageTemperatureTracker {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl PageTemperatureTracker {
    pub fn record_conflict(&self, pages: impl Iterator<Item = u32>) {
        let mut map = self.inner.lock().unwrap();
        let now = Instant::now();
        for page in pages {
            let entry = map.entry(page).or_insert((0, now));
            entry.0 = (entry.0 + 1).min(100);
            entry.1 = now;
        }
        if map.len() > MAX_TRACKED_PAGES {
            // Cheap bound: this tracker is a best-effort heuristic, not a source of
            // truth, so losing it and letting it repopulate is safe.
            map.clear();
        }
    }

    pub fn record_success(&self, pages: impl Iterator<Item = u32>) {
        let mut map = self.inner.lock().unwrap();
        for page in pages {
            map.remove(&page);
        }
    }

    /// A small, jittered pre-commit delay scaled by how "hot" the pages this commit is
    /// about to touch have recently been. Zero when nothing's hot. The goal is to
    /// reduce the chance that many concurrent committers collide on the same
    /// recently-contended pages at the exact same instant, not to guarantee anything.
    pub fn suggested_backoff(&self, pages: impl Iterator<Item = u32>) -> Duration {
        let max_heat = {
            let map = self.inner.lock().unwrap();
            let now = Instant::now();
            let mut max_heat = 0u32;
            for page in pages {
                if let Some(&(count, last_seen)) = map.get(&page) {
                    if now.duration_since(last_seen) < TEMPERATURE_TTL {
                        max_heat = max_heat.max(count);
                    }
                }
            }
            max_heat
        };

        if max_heat == 0 {
            return Duration::ZERO;
        }

        let scale = (max_heat.min(MAX_HEAT) as f64) / (MAX_HEAT as f64);
        let base_ms = (MAX_BACKOFF.as_millis() as f64) * scale;
        let jitter = rand::thread_rng().gen_range(0.5..1.0);
        Duration::from_millis((base_ms * jitter) as u64)
    }
}
