//! Best-effort in-memory GC scheduling. Pending work is lost on process restart.
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Semaphore;
use utils::id::{TenantId, TimelineId};

type Key = (TenantId, TimelineId);

pub struct OggitGcScheduler {
    // Empty entries still represent active jobs; new events request another pass.
    pending: Mutex<HashMap<Key, HashSet<TimelineId>>>,
    concurrency: Semaphore,
    delay: Duration,
}

impl Default for OggitGcScheduler {
    fn default() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            concurrency: Semaphore::new(2),
            delay: Duration::from_secs(2),
        }
    }
}

impl OggitGcScheduler {
    /// Serialize each parent's jobs, including database configuration changes.
    /// Enqueue never waits for a database connection or a concurrency permit.
    pub fn enqueue<F, Fut>(
        self: &Arc<Self>,
        tenant: TenantId,
        parent: TimelineId,
        deleted: TimelineId,
        run: F,
    ) where
        F: Fn(HashSet<TimelineId>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send,
    {
        let key = (tenant, parent);
        {
            let mut pending = self.pending.lock().unwrap();
            if let Some(children) = pending.get_mut(&key) {
                children.insert(deleted);
                return;
            }
            pending.insert(key, HashSet::from([deleted]));
        }
        let scheduler = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(scheduler.delay).await;
                let permit = scheduler.concurrency.acquire().await.unwrap();
                let children = {
                    let mut pending = scheduler.pending.lock().unwrap();
                    std::mem::take(pending.get_mut(&key).unwrap())
                };
                for attempt in 1..=3 {
                    let outcome =
                        tokio::time::timeout(Duration::from_secs(120), run(children.clone())).await;
                    match outcome {
                        Ok(Ok(())) => break,
                        Ok(Err(error)) => eprintln!(
                            "background oggit GC failed tenant={tenant} parent={parent} attempt={attempt}: {error:#}"
                        ),
                        Err(_) => eprintln!(
                            "background oggit GC timed out tenant={tenant} parent={parent} attempt={attempt}"
                        ),
                    }
                    if attempt < 3 {
                        tokio::time::sleep(scheduler.delay).await;
                    }
                }
                drop(permit);
                let mut pending = scheduler.pending.lock().unwrap();
                if pending.get(&key).unwrap().is_empty() {
                    pending.remove(&key);
                    break;
                }
            }
        });
    }
}
