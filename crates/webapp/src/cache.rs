//! Backend-urile de cache interschimbabile la runtime.
//!
//! Dispatch prin `enum` (nu `dyn Trait`): fără alocări pe heap per apel și
//! fără `async_trait`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use bytes::Bytes;
use cache_node::ClusterClient;
use memtable::{Policy, StatsSnapshot, Table, TableConfig};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;

/// Tipul de backend de cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    /// Fără cache — fiecare cerere ajunge în PostgreSQL (baseline secvențial).
    None,
    /// Tabelă `memtable` în procesul aplicației (L1, per instanță).
    Local,
    /// Cluster de noduri `cache-node`, consistent hashing.
    Cluster,
    /// Redis (baseline industrial).
    Redis,
    /// Tabelă `UNLOGGED` într-un PostgreSQL cu date pe tmpfs (tabelă in-memory relațională).
    Pgmem,
    /// L1 local + L2 cluster (near-cache).
    Tiered,
}

/// Strategia de propagare a scrierilor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum WriteStrategy {
    /// Scrie în DB, apoi șterge intrarea din cache (cache-aside clasic).
    Invalidate,
    /// Scrie în DB, apoi actualizează cache-ul cu valoarea nouă.
    WriteThrough,
    /// Actualizează cache-ul imediat; DB-ul e scris asincron, în loturi.
    WriteBehind,
}

/// Configurația de runtime, modificabilă prin `POST /admin/mode`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct ModeConfig {
    pub backend: BackendKind,
    pub strategy: WriteStrategy,
    /// Capacitatea (intrări) pentru L1 local și pentru fiecare nod de cluster.
    pub capacity: usize,
    #[schema(value_type = String, example = "lru")]
    pub policy: Policy,
    pub ttl_secs: u64,
    /// Câte noduri de cluster se folosesc (1..=N configurate).
    pub nodes: usize,
    /// Single-flight: o singură interogare DB per cheie lipsă, la un moment dat.
    pub coalesce: bool,
}

impl Default for ModeConfig {
    fn default() -> Self {
        Self {
            backend: BackendKind::Local,
            strategy: WriteStrategy::Invalidate,
            capacity: 200_000,
            policy: Policy::Lru,
            ttl_secs: 300,
            nodes: usize::MAX,
            coalesce: true,
        }
    }
}

impl ModeConfig {
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.ttl_secs.max(1))
    }
}

/// TTL-ul L1 într-un cache pe două niveluri: scurt, pentru a limita datele vechi.
const L1_TTL: Duration = Duration::from_secs(5);

/// `ConnectionManager` nu implementează `Debug`; îl împachetăm.
#[derive(Clone)]
pub struct RedisConn(pub ConnectionManager);

impl std::fmt::Debug for RedisConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedisConn")
    }
}

#[derive(Debug)]
pub enum Backend {
    None,
    Local(Arc<Table>),
    Cluster(ClusterClient),
    Redis(RedisConn),
    PgMem(PgPool),
    Tiered { l1: Arc<Table>, l2: ClusterClient },
}

fn local_table(cfg: &ModeConfig) -> Arc<Table> {
    Arc::new(Table::new(TableConfig {
        capacity: cfg.capacity,
        shards: 64,
        policy: cfg.policy,
        default_ttl: None,
    }))
}

/// Resursele externe disponibile pentru construirea unui backend.
#[derive(Debug, Clone)]
pub struct Resources {
    pub redis: Option<RedisConn>,
    pub pgmem: Option<PgPool>,
    pub nodes: Vec<Arc<cache_node::NodeClient>>,
}

impl Resources {
    fn cluster(&self, cfg: &ModeConfig) -> Result<ClusterClient> {
        if self.nodes.is_empty() {
            bail!("no cache nodes configured (CACHE_NODES)");
        }
        let n = cfg.nodes.clamp(1, self.nodes.len());
        Ok(ClusterClient::new(self.nodes[..n].to_vec()))
    }

    /// Construiește backend-ul și golește stocarea partajată (experimente curate).
    pub async fn build(&self, cfg: &ModeConfig) -> Result<Backend> {
        Ok(match cfg.backend {
            BackendKind::None => Backend::None,
            BackendKind::Local => Backend::Local(local_table(cfg)),
            BackendKind::Cluster | BackendKind::Tiered => {
                let cl = self.cluster(cfg)?;
                cl.configure(cfg.capacity, cfg.policy, 64).await?;
                if cfg.backend == BackendKind::Cluster {
                    Backend::Cluster(cl)
                } else {
                    Backend::Tiered {
                        l1: local_table(cfg),
                        l2: cl,
                    }
                }
            }
            BackendKind::Redis => {
                let Some(r) = self.redis.clone() else {
                    bail!("REDIS_URL not configured")
                };
                let (): () = redis::cmd("FLUSHDB").query_async(&mut r.0.clone()).await?;
                Backend::Redis(r)
            }
            BackendKind::Pgmem => {
                let Some(p) = self.pgmem.clone() else {
                    bail!("PGMEM_URL not configured")
                };
                sqlx::query("TRUNCATE cache_kv").execute(&p).await?;
                Backend::PgMem(p)
            }
        })
    }
}

/// Schema tabelei-cache in-memory din PostgreSQL. Advisory lock-ul serializează
/// inițializarea când mai multe instanțe pornesc simultan (DDL-ul `IF NOT EXISTS`
/// nu e sigur la concurență în PostgreSQL).
pub async fn init_pgmem(p: &PgPool) -> Result<()> {
    let mut tx = p.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(7243)")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "CREATE UNLOGGED TABLE IF NOT EXISTS cache_kv (
            key text PRIMARY KEY,
            value bytea NOT NULL,
            expires_at timestamptz NOT NULL,
            tags text[] NOT NULL DEFAULT '{}')",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS cache_kv_tags ON cache_kv USING gin (tags)")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

fn tag_key(tag: &str) -> String {
    format!("tag:{tag}")
}

impl Backend {
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Nivelul local (L1), dacă există — invalidat prin LISTEN/NOTIFY.
    pub fn l1(&self) -> Option<&Table> {
        match self {
            Self::Local(t) | Self::Tiered { l1: t, .. } => Some(t),
            _ => None,
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        Ok(match self {
            Self::None => None,
            Self::Local(t) => t.get(key),
            Self::Cluster(c) => c.get(key).await?,
            Self::Redis(r) => {
                let v: Option<Vec<u8>> = r.0.clone().get(key).await?;
                v.map(Bytes::from)
            }
            Self::PgMem(p) => {
                let v: Option<Vec<u8>> = sqlx::query_scalar(
                    "SELECT value FROM cache_kv WHERE key = $1 AND expires_at > now()",
                )
                .bind(key)
                .fetch_optional(p)
                .await?;
                v.map(Bytes::from)
            }
            Self::Tiered { l1, l2 } => {
                if let Some(v) = l1.get(key) {
                    return Ok(Some(v));
                }
                let v = l2.get(key).await?;
                if let Some(v) = &v {
                    l1.set(key, v.clone(), Some(L1_TTL), &[]);
                }
                v
            }
        })
    }

    pub async fn set(&self, key: &str, value: &Bytes, ttl: Duration, tags: &[&str]) -> Result<()> {
        match self {
            Self::None => {}
            Self::Local(t) => t.set(key, value.clone(), Some(ttl), tags),
            Self::Cluster(c) => c.set(key, value, ttl, tags).await?,
            Self::Redis(r) => {
                let secs = ttl.as_secs().max(1);
                let mut pipe = redis::pipe();
                pipe.set_ex(key, value.as_ref(), secs).ignore();
                for t in tags {
                    pipe.sadd(tag_key(t), key)
                        .ignore()
                        .expire(tag_key(t), secs as i64)
                        .ignore();
                }
                let (): () = pipe.query_async(&mut r.0.clone()).await?;
            }
            Self::PgMem(p) => {
                sqlx::query(
                    "INSERT INTO cache_kv (key, value, expires_at, tags)
                     VALUES ($1, $2, now() + make_interval(secs => $3), $4)
                     ON CONFLICT (key) DO UPDATE
                     SET value = excluded.value, expires_at = excluded.expires_at, tags = excluded.tags",
                )
                .bind(key)
                .bind(value.as_ref())
                .bind(ttl.as_secs_f64())
                .bind(tags)
                .execute(p)
                .await?;
            }
            Self::Tiered { l1, l2 } => {
                l2.set(key, value, ttl, tags).await?;
                l1.set(key, value.clone(), Some(L1_TTL.min(ttl)), tags);
            }
        }
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        match self {
            Self::None => {}
            Self::Local(t) => {
                t.delete(key);
            }
            Self::Cluster(c) => {
                c.delete(key).await?;
            }
            Self::Redis(r) => {
                let (): () = r.0.clone().del(key).await?;
            }
            Self::PgMem(p) => {
                sqlx::query("DELETE FROM cache_kv WHERE key = $1")
                    .bind(key)
                    .execute(p)
                    .await?;
            }
            Self::Tiered { l1, l2 } => {
                l2.delete(key).await?;
                l1.delete(key);
            }
        }
        Ok(())
    }

    pub async fn invalidate_tag(&self, tag: &str) -> Result<()> {
        match self {
            Self::None => {}
            Self::Local(t) => {
                t.invalidate_tag(tag);
            }
            Self::Cluster(c) => {
                c.invalidate_tag(tag).await?;
            }
            Self::Redis(r) => {
                let mut r = r.0.clone();
                let keys: Vec<String> = r.smembers(tag_key(tag)).await?;
                let mut pipe = redis::pipe();
                if !keys.is_empty() {
                    pipe.del(keys).ignore();
                }
                pipe.del(tag_key(tag)).ignore();
                let (): () = pipe.query_async(&mut r).await?;
            }
            Self::PgMem(p) => {
                sqlx::query("DELETE FROM cache_kv WHERE tags @> ARRAY[$1]")
                    .bind(tag)
                    .execute(p)
                    .await?;
            }
            Self::Tiered { l1, l2 } => {
                l2.invalidate_tag(tag).await?;
                l1.invalidate_tag(tag);
            }
        }
        Ok(())
    }

    /// Statistici interne ale backend-ului (unde există).
    pub async fn stats(&self) -> serde_json::Value {
        let local = |t: &Table| serde_json::to_value(t.stats()).unwrap_or_default();
        let nodes = |stats: Vec<(String, StatsSnapshot)>| {
            serde_json::Value::Array(
                stats
                    .into_iter()
                    .map(|(addr, s)| serde_json::json!({ "node": addr, "stats": s }))
                    .collect(),
            )
        };
        match self {
            Self::Local(t) => serde_json::json!({ "l1": local(t) }),
            Self::Cluster(c) => match c.stats().await {
                Ok(s) => serde_json::json!({ "nodes": nodes(s) }),
                Err(e) => serde_json::json!({ "error": e.to_string() }),
            },
            Self::Tiered { l1, l2 } => match l2.stats().await {
                Ok(s) => serde_json::json!({ "l1": local(l1), "nodes": nodes(s) }),
                Err(e) => serde_json::json!({ "l1": local(l1), "error": e.to_string() }),
            },
            _ => serde_json::Value::Null,
        }
    }
}
