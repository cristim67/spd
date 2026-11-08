//! `memtable` — motor de tabele in-memory pentru cache.
//!
//! O [`Table`] este împărțită în *shard*-uri independente (fiecare cu propriul
//! mutex) pentru a reduce contenția între fire. Fiecare shard ține intrările
//! într-un *slab* (`Vec`) legat printr-o listă dublu înlănțuită intrusivă, ceea
//! ce face ca toate operațiile (get / set / delete / evict) să fie O(1).
//!
//! Funcționalități:
//! * politici de evacuare: [`Policy::Lru`], [`Policy::Fifo`], [`Policy::Clock`]
//!   (second-chance);
//! * expirare TTL (leneșă la citire + [`Table::purge_expired`] pentru janitor);
//! * invalidare pe *tag*-uri (ex. toate intrările unei categorii);
//! * statistici atomice (hit/miss/evictions...).

mod shard;

use std::fmt;
use std::hash::{BuildHasher, RandomState};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use shard::{Lookup, Shard};

/// Politica de evacuare folosită când un shard atinge capacitatea.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Policy {
    /// Least Recently Used — exact, mută intrarea în capul listei la fiecare acces.
    #[default]
    Lru,
    /// First In First Out — ignoră accesele.
    Fifo,
    /// CLOCK / second-chance — aproximare LRU cu un singur bit de referință.
    Clock,
}

impl FromStr for Policy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "lru" => Ok(Self::Lru),
            "fifo" => Ok(Self::Fifo),
            "clock" => Ok(Self::Clock),
            other => Err(format!("unknown policy `{other}` (lru|fifo|clock)")),
        }
    }
}

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Lru => "lru",
            Self::Fifo => "fifo",
            Self::Clock => "clock",
        })
    }
}

/// Configurația unei tabele.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    /// Numărul maxim total de intrări, împărțit egal între shard-uri
    /// (capacitatea efectivă e rotunjită la `ceil(capacity / shards) * shards`).
    pub capacity: usize,
    /// Numărul de shard-uri (rotunjit la o putere a lui 2).
    pub shards: usize,
    pub policy: Policy,
    /// TTL implicit, folosit când `set` primește `None`.
    pub default_ttl: Option<Duration>,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            capacity: 100_000,
            shards: 16,
            policy: Policy::Lru,
            default_ttl: None,
        }
    }
}

/// Instantaneu al statisticilor unei tabele.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub expired: u64,
    pub sets: u64,
    pub evictions: u64,
    pub deletes: u64,
    pub len: u64,
}

impl StatsSnapshot {
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    hits: AtomicU64,
    misses: AtomicU64,
    expired: AtomicU64,
    sets: AtomicU64,
    evictions: AtomicU64,
    deletes: AtomicU64,
}

/// Tabelă in-memory, thread-safe, cu sharding.
#[derive(Debug)]
pub struct Table {
    shards: Box<[Mutex<Shard>]>,
    hasher: RandomState,
    config: TableConfig,
    stats: Stats,
}

impl Table {
    pub fn new(config: TableConfig) -> Self {
        let n = config.shards.max(1).next_power_of_two();
        let per_shard = config.capacity.div_ceil(n).max(1);
        let shards = (0..n)
            .map(|_| Mutex::new(Shard::new(per_shard, config.policy)))
            .collect();
        Self {
            shards,
            hasher: RandomState::new(),
            config,
            stats: Stats::default(),
        }
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }

    fn shard(&self, key: &str) -> &Mutex<Shard> {
        // Lungimea e putere a lui 2, deci masca e echivalentă cu modulo.
        let idx = (self.hasher.hash_one(key) as usize) & (self.shards.len() - 1);
        &self.shards[idx]
    }

    /// Returnează valoarea dacă există și nu a expirat.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let res = self.shard(key).lock().get(key, Instant::now());
        match res {
            Lookup::Hit(v) => {
                self.stats.hits.fetch_add(1, Relaxed);
                Some(v)
            }
            Lookup::Expired => {
                self.stats.expired.fetch_add(1, Relaxed);
                self.stats.misses.fetch_add(1, Relaxed);
                None
            }
            Lookup::Miss => {
                self.stats.misses.fetch_add(1, Relaxed);
                None
            }
        }
    }

    /// Inserează sau înlocuiește o intrare. `ttl = None` folosește TTL-ul implicit.
    pub fn set(&self, key: &str, value: Bytes, ttl: Option<Duration>, tags: &[&str]) {
        let now = Instant::now();
        let expires_at = ttl.or(self.config.default_ttl).map(|t| now + t);
        let evicted = self.shard(key).lock().set(key, value, expires_at, tags);
        self.stats.sets.fetch_add(1, Relaxed);
        if evicted > 0 {
            self.stats.evictions.fetch_add(evicted as u64, Relaxed);
        }
    }

    pub fn delete(&self, key: &str) -> bool {
        let removed = self.shard(key).lock().delete(key);
        if removed {
            self.stats.deletes.fetch_add(1, Relaxed);
        }
        removed
    }

    /// Șterge toate intrările etichetate cu `tag`. Returnează numărul lor.
    pub fn invalidate_tag(&self, tag: &str) -> usize {
        let n: usize = self
            .shards
            .iter()
            .map(|s| s.lock().invalidate_tag(tag))
            .sum();
        self.stats.deletes.fetch_add(n as u64, Relaxed);
        n
    }

    /// Elimină intrările expirate (apelat periodic de un janitor).
    pub fn purge_expired(&self) -> usize {
        let now = Instant::now();
        let n: usize = self
            .shards
            .iter()
            .map(|s| s.lock().purge_expired(now))
            .sum();
        self.stats.expired.fetch_add(n as u64, Relaxed);
        n
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        for s in &self.shards {
            s.lock().clear();
        }
    }

    pub fn stats(&self) -> StatsSnapshot {
        StatsSnapshot {
            hits: self.stats.hits.load(Relaxed),
            misses: self.stats.misses.load(Relaxed),
            expired: self.stats.expired.load(Relaxed),
            sets: self.stats.sets.load(Relaxed),
            evictions: self.stats.evictions.load(Relaxed),
            deletes: self.stats.deletes.load(Relaxed),
            len: self.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn table(capacity: usize, policy: Policy) -> Table {
        Table::new(TableConfig {
            capacity,
            shards: 1,
            policy,
            default_ttl: None,
        })
    }

    #[test]
    fn set_get_delete() {
        let t = table(10, Policy::Lru);
        t.set("a", Bytes::from_static(b"1"), None, &[]);
        assert_eq!(t.get("a").as_deref(), Some(&b"1"[..]));
        assert!(t.delete("a"));
        assert!(t.get("a").is_none());
        let s = t.stats();
        assert_eq!((s.hits, s.misses, s.sets, s.deletes), (1, 1, 1, 1));
    }

    #[test]
    fn capacity_is_respected_across_shards() {
        let t = Table::new(TableConfig {
            capacity: 64,
            shards: 4,
            ..Default::default()
        });
        for i in 0..10_000 {
            t.set(&i.to_string(), Bytes::new(), None, &[]);
        }
        assert!(t.len() <= 64);
        assert_eq!(t.stats().evictions, 10_000 - t.len() as u64);
    }

    #[test]
    fn ttl_expires() {
        let t = table(10, Policy::Lru);
        t.set("a", Bytes::new(), Some(Duration::from_millis(20)), &[]);
        assert!(t.get("a").is_some());
        std::thread::sleep(Duration::from_millis(40));
        assert!(t.get("a").is_none());
        assert_eq!(t.stats().expired, 1);
    }

    #[test]
    fn tag_invalidation() {
        let t = Table::new(TableConfig {
            capacity: 100,
            shards: 8,
            ..Default::default()
        });
        for i in 0..20 {
            let tag = if i % 2 == 0 { "even" } else { "odd" };
            t.set(&format!("k{i}"), Bytes::new(), None, &[tag, "all"]);
        }
        assert_eq!(t.invalidate_tag("even"), 10);
        assert_eq!(t.len(), 10);
        assert_eq!(t.invalidate_tag("all"), 10);
        assert!(t.is_empty());
    }

    #[test]
    fn concurrent_access_is_consistent() {
        let t = Arc::new(Table::new(TableConfig {
            capacity: 1024,
            shards: 16,
            ..Default::default()
        }));
        let handles: Vec<_> = (0..8)
            .map(|th| {
                let t = Arc::clone(&t);
                std::thread::spawn(move || {
                    for i in 0..10_000 {
                        let k = format!("{}", (i * 7 + th) % 2000);
                        if t.get(&k).is_none() {
                            t.set(&k, Bytes::from(k.clone()), None, &[]);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let s = t.stats();
        assert_eq!(s.hits + s.misses, 80_000);
        assert!(t.len() <= 1024);
    }

    #[test]
    fn policy_parse_roundtrip() {
        for p in [Policy::Lru, Policy::Fifo, Policy::Clock] {
            assert_eq!(p.to_string().parse::<Policy>().unwrap(), p);
        }
        assert!("nope".parse::<Policy>().is_err());
    }
}
