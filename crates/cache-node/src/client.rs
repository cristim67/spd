//! Client asincron pentru un nod (cu pool de conexiuni) și client de cluster.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use memtable::{Policy, StatsSnapshot};
use parking_lot::Mutex;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use tokio::task::JoinSet;

use crate::ring::Ring;

type Conn = BufStream<TcpStream>;

#[derive(Debug)]
pub(crate) enum Reply {
    Simple,
    Bulk(Option<Bytes>),
    Int(i64),
}

/// Client pentru un singur nod. Conexiunile sunt refolosite dintr-un pool.
#[derive(Debug)]
pub struct NodeClient {
    addr: String,
    pool: Mutex<Vec<Conn>>,
    max_idle: usize,
}

fn check_token(s: &str) -> Result<()> {
    if s.is_empty() || s.bytes().any(|b| b.is_ascii_whitespace() || b == b',') {
        bail!("invalid key/tag `{s}`: must be non-empty without whitespace or commas");
    }
    Ok(())
}

impl NodeClient {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            pool: Mutex::new(Vec::new()),
            max_idle: 256,
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    async fn connect(&self) -> Result<Conn> {
        let s = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(&self.addr))
            .await
            .with_context(|| format!("connect timeout {}", self.addr))??;
        s.set_nodelay(true)?;
        Ok(BufStream::new(s))
    }

    async fn exchange(conn: &mut Conn, header: &str, payload: Option<&[u8]>) -> Result<Reply> {
        conn.write_all(header.as_bytes()).await?;
        conn.write_all(b"\n").await?;
        if let Some(p) = payload {
            conn.write_all(p).await?;
        }
        conn.flush().await?;

        let mut line = String::new();
        if conn.read_line(&mut line).await? == 0 {
            bail!("connection closed");
        }
        let line = line.trim_end();
        let (kind, rest) = line.split_at(1.min(line.len()));
        match kind {
            "+" => Ok(Reply::Simple),
            ":" => Ok(Reply::Int(rest.parse()?)),
            "$" if rest == "-1" => Ok(Reply::Bulk(None)),
            "$" => {
                let mut buf = vec![0; rest.parse()?];
                conn.read_exact(&mut buf).await?;
                Ok(Reply::Bulk(Some(buf.into())))
            }
            "-" => Err(anyhow!("server error: {rest}")),
            _ => Err(anyhow!("protocol error: `{line}`")),
        }
    }

    pub(crate) async fn call(&self, header: &str, payload: Option<&[u8]>) -> Result<Reply> {
        let pooled = self.pool.lock().pop();
        let from_pool = pooled.is_some();
        let mut conn = match pooled {
            Some(c) => c,
            None => self.connect().await?,
        };
        let res = match Self::exchange(&mut conn, header, payload).await {
            // O conexiune din pool poate fi moartă (nod repornit): reîncercăm o dată.
            Err(_) if from_pool => {
                conn = self.connect().await?;
                Self::exchange(&mut conn, header, payload).await
            }
            r => r,
        };
        if res.is_ok() {
            let mut pool = self.pool.lock();
            if pool.len() < self.max_idle {
                pool.push(conn);
            }
        }
        res
    }

    pub async fn ping(&self) -> Result<()> {
        self.call("PING", None).await.map(|_| ())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        check_token(key)?;
        match self.call(&format!("GET {key}"), None).await? {
            Reply::Bulk(v) => Ok(v),
            r => Err(anyhow!("unexpected reply {r:?}")),
        }
    }

    pub async fn set(&self, key: &str, value: &[u8], ttl: Duration, tags: &[&str]) -> Result<()> {
        check_token(key)?;
        for t in tags {
            check_token(t)?;
        }
        let header = format!(
            "SET {key} {} {} {}",
            ttl.as_millis(),
            value.len(),
            tags.join(",")
        );
        self.call(header.trim_end(), Some(value)).await.map(|_| ())
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        check_token(key)?;
        match self.call(&format!("DEL {key}"), None).await? {
            Reply::Int(n) => Ok(n > 0),
            r => Err(anyhow!("unexpected reply {r:?}")),
        }
    }

    pub async fn invalidate_tag(&self, tag: &str) -> Result<u64> {
        check_token(tag)?;
        match self.call(&format!("TAG {tag}"), None).await? {
            Reply::Int(n) => Ok(n.max(0) as u64),
            r => Err(anyhow!("unexpected reply {r:?}")),
        }
    }

    pub async fn stats(&self) -> Result<StatsSnapshot> {
        match self.call("STATS", None).await? {
            Reply::Bulk(Some(b)) => Ok(serde_json::from_slice(&b)?),
            r => Err(anyhow!("unexpected reply {r:?}")),
        }
    }

    pub async fn flush(&self) -> Result<()> {
        self.call("FLUSH", None).await.map(|_| ())
    }

    pub async fn configure(&self, capacity: usize, policy: Policy, shards: usize) -> Result<()> {
        self.call(&format!("CONFIG {capacity} {policy} {shards}"), None)
            .await
            .map(|_| ())
    }
}

/// Client de cluster: rutează fiecare cheie la un nod prin consistent hashing.
#[derive(Debug, Clone)]
pub struct ClusterClient {
    ring: Ring,
    nodes: Vec<Arc<NodeClient>>,
}

impl ClusterClient {
    pub fn new(nodes: Vec<Arc<NodeClient>>) -> Self {
        let names: Vec<&str> = nodes.iter().map(|n| n.addr()).collect();
        Self {
            ring: Ring::new(&names, 160),
            nodes,
        }
    }

    pub fn nodes(&self) -> &[Arc<NodeClient>] {
        &self.nodes
    }

    fn node(&self, key: &str) -> &NodeClient {
        &self.nodes[self.ring.node_for(key)]
    }

    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        self.node(key).get(key).await
    }

    pub async fn set(&self, key: &str, value: &[u8], ttl: Duration, tags: &[&str]) -> Result<()> {
        self.node(key).set(key, value, ttl, tags).await
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        self.node(key).delete(key).await
    }

    /// Intrările unui tag pot fi pe orice nod, deci invalidarea e difuzată
    /// (broadcast) în paralel către toate nodurile.
    pub async fn invalidate_tag(&self, tag: &str) -> Result<u64> {
        let mut set = JoinSet::new();
        for n in &self.nodes {
            let (n, tag) = (Arc::clone(n), tag.to_owned());
            set.spawn(async move { n.invalidate_tag(&tag).await });
        }
        let mut total = 0;
        while let Some(r) = set.join_next().await {
            total += r??;
        }
        Ok(total)
    }

    pub async fn stats(&self) -> Result<Vec<(String, StatsSnapshot)>> {
        let mut out = Vec::with_capacity(self.nodes.len());
        for n in &self.nodes {
            out.push((n.addr().to_owned(), n.stats().await?));
        }
        Ok(out)
    }

    pub async fn flush(&self) -> Result<()> {
        for n in &self.nodes {
            n.flush().await?;
        }
        Ok(())
    }

    pub async fn configure(&self, capacity: usize, policy: Policy, shards: usize) -> Result<()> {
        for n in &self.nodes {
            n.configure(capacity, policy, shards).await?;
        }
        Ok(())
    }
}
