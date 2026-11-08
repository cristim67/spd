//! Starea partajată a aplicației: pool-uri, backend-ul curent, statistici,
//! single-flight și coada write-behind.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::{OnceCell, mpsc};
use utoipa::ToSchema;

use crate::cache::{Backend, ModeConfig, Resources};
use crate::db::ProductPatch;
use crate::metrics;

/// Backend-ul activ + configurația lui. Înlocuit atomic la schimbarea modului.
#[derive(Debug)]
pub struct Runtime {
    pub cfg: ModeConfig,
    pub backend: Backend,
}

#[derive(Debug, Default)]
pub struct Counters {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub db_reads: AtomicU64,
    pub db_writes: AtomicU64,
    pub coalesced: AtomicU64,
    pub cache_errors: AtomicU64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CountersSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub hit_ratio: f64,
    pub db_reads: u64,
    pub db_writes: u64,
    /// Cereri servite de o interogare DB aflată deja în curs (single-flight).
    pub coalesced: u64,
    pub cache_errors: u64,
}

impl Counters {
    pub fn snapshot(&self) -> CountersSnapshot {
        let hits = self.hits.load(Relaxed);
        let misses = self.misses.load(Relaxed);
        let total = hits + misses;
        CountersSnapshot {
            hits,
            misses,
            hit_ratio: if total == 0 {
                0.0
            } else {
                hits as f64 / total as f64
            },
            db_reads: self.db_reads.load(Relaxed),
            db_writes: self.db_writes.load(Relaxed),
            coalesced: self.coalesced.load(Relaxed),
            cache_errors: self.cache_errors.load(Relaxed),
        }
    }

    pub fn reset(&self) {
        for c in [
            &self.hits,
            &self.misses,
            &self.db_reads,
            &self.db_writes,
            &self.coalesced,
            &self.cache_errors,
        ] {
            c.store(0, Relaxed);
        }
    }
}

/// O modificare amânată (write-behind).
#[derive(Debug, Clone, Copy)]
pub struct WbUpdate {
    pub id: i64,
    pub category_id: i32,
    pub patch: ProductPatch,
}

type Flight = Arc<OnceCell<Result<Option<Bytes>, String>>>;

#[derive(Debug)]
pub struct AppState {
    pub db: PgPool,
    pub resources: Resources,
    runtime: RwLock<Arc<Runtime>>,
    pub counters: Counters,
    /// Benzile write-behind; produsul `id` merge mereu pe banda `id % N`.
    wb_lanes: Vec<mpsc::Sender<WbUpdate>>,
    inflight: Mutex<HashMap<String, Flight>>,
    pub instance: String,
    pub ui_instances: Vec<String>,
}

impl AppState {
    pub fn new(
        db: PgPool,
        resources: Resources,
        runtime: Runtime,
        wb_lanes: Vec<mpsc::Sender<WbUpdate>>,
        instance: String,
        ui_instances: Vec<String>,
    ) -> Self {
        Self {
            db,
            resources,
            runtime: RwLock::new(Arc::new(runtime)),
            counters: Counters::default(),
            wb_lanes,
            inflight: Mutex::new(HashMap::new()),
            instance,
            ui_instances,
        }
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime.read())
    }

    pub fn set_runtime(&self, rt: Runtime) {
        *self.runtime.write() = Arc::new(rt);
        self.counters.reset();
    }

    pub fn wb_pending(&self) -> usize {
        self.wb_lanes
            .iter()
            .map(|t| t.max_capacity() - t.capacity())
            .sum()
    }

    /// Pune o modificare în banda produsului (ordinea per produs e păstrată).
    pub async fn enqueue_write(&self, u: WbUpdate) -> anyhow::Result<()> {
        let lane = &self.wb_lanes[u.id.unsigned_abs() as usize % self.wb_lanes.len()];
        lane.send(u)
            .await
            .map_err(|_| anyhow::anyhow!("write-behind queue closed"))
    }

    fn cache_error(&self, op: &str, e: &anyhow::Error) {
        self.counters.cache_errors.fetch_add(1, Relaxed);
        metrics::CACHE_ERRORS.with_label_values(&[op]).inc();
        tracing::warn!(op, error = %e, "cache backend error, degrading to database");
    }

    /// Citire cache-aside cu protecție la stampede.
    ///
    /// Pași: caută în cache; la miss încarcă din DB (`load`) cu single-flight
    /// (cererile concurente pentru aceeași cheie așteaptă aceeași interogare);
    /// apoi populează cache-ul. Erorile cache-ului nu opresc cererea (degradare
    /// la DB). Returnează `(valoare, hit)`.
    pub async fn read_through<F, Fut>(
        &self,
        key: &str,
        tags: &[&str],
        load: F,
    ) -> anyhow::Result<(Option<Bytes>, bool)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<Option<Bytes>>>,
    {
        let rt = self.runtime();
        if !rt.backend.is_none() {
            match rt.backend.get(key).await {
                Ok(Some(v)) => {
                    self.counters.hits.fetch_add(1, Relaxed);
                    metrics::CACHE_HIT.inc();
                    return Ok((Some(v), true));
                }
                Ok(None) => {}
                Err(e) => self.cache_error("get", &e),
            }
            self.counters.misses.fetch_add(1, Relaxed);
            metrics::CACHE_MISS.inc();
        }

        let fetch = async {
            self.counters.db_reads.fetch_add(1, Relaxed);
            metrics::DB_READ.inc();
            let v = load().await?;
            if let Some(v) = &v {
                if let Err(e) = rt.backend.set(key, v, rt.cfg.ttl(), tags).await {
                    self.cache_error("set", &e);
                }
            }
            Ok::<_, anyhow::Error>(v)
        };

        if !rt.cfg.coalesce {
            return Ok((fetch.await?, false));
        }

        let (cell, leader) = {
            let mut map = self.inflight.lock();
            if let Some(c) = map.get(key) {
                (Arc::clone(c), false)
            } else {
                let c: Flight = Arc::default();
                map.insert(key.to_owned(), Arc::clone(&c));
                (c, true)
            }
        };
        if !leader {
            self.counters.coalesced.fetch_add(1, Relaxed);
        }
        let res = cell
            .get_or_init(move || async move { fetch.await.map_err(|e| e.to_string()) })
            .await
            .clone();
        // Oricine termină curăță intrarea (leader-ul poate fi anulat de client).
        let mut map = self.inflight.lock();
        if map.get(key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
            map.remove(key);
        }
        res.map(|v| (v, false)).map_err(anyhow::Error::msg)
    }

    /// Invalidare best-effort (erorile sunt doar contorizate).
    pub async fn invalidate(&self, key: Option<&str>, tag: Option<&str>) {
        let rt = self.runtime();
        if let Some(k) = key {
            if let Err(e) = rt.backend.delete(k).await {
                self.cache_error("delete", &e);
            }
        }
        if let Some(t) = tag {
            if let Err(e) = rt.backend.invalidate_tag(t).await {
                self.cache_error("invalidate_tag", &e);
            }
        }
    }

    pub async fn cache_set(&self, key: &str, value: &Bytes, tags: &[&str]) {
        let rt = self.runtime();
        if let Err(e) = rt.backend.set(key, value, rt.cfg.ttl(), tags).await {
            self.cache_error("set", &e);
        }
    }
}

/// Bucla write-behind: consumă coada în loturi, combină modificările per
/// produs și le scrie cu un singur `UPDATE`. La oprire golește coada (nu
/// pierde scrieri confirmate clientului).
pub async fn write_behind_worker(
    state: Arc<AppState>,
    mut rx: mpsc::Receiver<WbUpdate>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut buf = Vec::with_capacity(1024);
    loop {
        let n = tokio::select! {
            n = rx.recv_many(&mut buf, 1024) => n,
            _ = shutdown.changed() => {
                // Nu mai acceptăm scrieri noi, dar le golim pe cele din coadă.
                rx.close();
                rx.recv_many(&mut buf, 1024).await
            }
        };
        if n == 0 {
            tracing::info!("write-behind queue drained, worker exiting");
            return;
        }
        let mut merged: HashMap<i64, (i32, ProductPatch)> = HashMap::with_capacity(buf.len());
        for u in buf.drain(..) {
            merged
                .entry(u.id)
                .and_modify(|(_, p)| p.merge(&u.patch))
                .or_insert((u.category_id, u.patch));
        }
        let batch: Vec<(i64, ProductPatch)> = merged.iter().map(|(id, (_, p))| (*id, *p)).collect();

        let notify = state.runtime().backend.l1().is_some();
        let mut attempt = 0u32;
        loop {
            match crate::db::update_batch(&state.db, &batch, notify).await {
                Ok(_) => break,
                Err(e) if attempt < 8 => {
                    attempt += 1;
                    tracing::warn!(error = %e, attempt, "write-behind flush failed, retrying");
                    tokio::time::sleep(Duration::from_millis(50 << attempt)).await;
                }
                Err(e) => {
                    tracing::error!(error = %e, lost = batch.len(), "write-behind flush gave up");
                    break;
                }
            }
        }
        state.counters.db_writes.fetch_add(1, Relaxed);
        metrics::DB_WRITE_BATCH.inc();

        // Clasamentele categoriilor afectate devin vechi abia acum (după flush).
        let mut cats: Vec<i32> = merged.values().map(|(c, _)| *c).collect();
        cats.sort_unstable();
        cats.dedup();
        // Invalidările rulează concurent, într-un task separat: worker-ul trece imediat
        // la lotul următor (altfel sute de round-trip-uri secvențiale blochează flush-ul).
        let st = Arc::clone(&state);
        tokio::spawn(async move {
            let mut set = tokio::task::JoinSet::new();
            for c in cats {
                let st = Arc::clone(&st);
                set.spawn(async move { st.invalidate(None, Some(&format!("cat:{c}"))).await });
            }
            while set.join_next().await.is_some() {}
        });
        metrics::WB_QUEUE.set(state.wb_pending() as i64);
    }
}
