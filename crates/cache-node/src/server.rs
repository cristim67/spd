//! Serverul TCP al unui nod de cache. Fiecare conexiune e servită de un task
//! tokio; tabela [`memtable::Table`] e partajată (sharding intern).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use memtable::{Policy, Table, TableConfig};
use parking_lot::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

/// Payload maxim acceptat (protecție împotriva cererilor malițioase).
const MAX_VALUE: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub struct NodeState {
    table: RwLock<Arc<Table>>,
}

impl NodeState {
    pub fn new(config: TableConfig) -> Arc<Self> {
        Arc::new(Self {
            table: RwLock::new(Arc::new(Table::new(config))),
        })
    }

    pub fn table(&self) -> Arc<Table> {
        Arc::clone(&self.table.read())
    }

    fn replace(&self, config: TableConfig) {
        *self.table.write() = Arc::new(Table::new(config));
    }
}

/// Acceptă conexiuni la infinit; pornește și janitor-ul TTL.
pub async fn serve(listener: TcpListener, state: Arc<NodeState>) -> Result<()> {
    let janitor = Arc::clone(&state);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            janitor.table().purge_expired();
        }
    });
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle(stream, &state).await {
                tracing::debug!(%peer, error = %e, "connection closed with error");
            }
        });
    }
}

async fn handle(stream: TcpStream, state: &NodeState) -> Result<()> {
    stream.set_nodelay(true)?;
    let (r, w) = stream.into_split();
    let mut r = BufReader::new(r);
    let mut w = BufWriter::new(w);
    let mut line = String::new();
    loop {
        line.clear();
        if r.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        if let Err(e) = dispatch(line.trim_end(), &mut r, &mut w, state).await {
            // Erorile de protocol sunt raportate clientului; erorile I/O închid conexiunea.
            if e.downcast_ref::<std::io::Error>().is_some() {
                return Err(e);
            }
            w.write_all(format!("-ERR {e}\n").as_bytes()).await?;
        }
        // Flush doar când nu mai sunt comenzi în buffer => pipelining eficient.
        if r.buffer().is_empty() {
            w.flush().await?;
        }
    }
}

async fn write_bulk<W: AsyncWrite + Unpin>(w: &mut W, data: Option<&[u8]>) -> Result<()> {
    match data {
        Some(d) => {
            w.write_all(format!("${}\n", d.len()).as_bytes()).await?;
            w.write_all(d).await?;
        }
        None => w.write_all(b"$-1\n").await?,
    }
    Ok(())
}

fn arg<'a>(it: &mut impl Iterator<Item = &'a str>, name: &str) -> Result<&'a str> {
    it.next()
        .ok_or_else(|| anyhow!("missing argument <{name}>"))
}

async fn dispatch<R, W>(line: &str, r: &mut R, w: &mut W, state: &NodeState) -> Result<()>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut it = line.split(' ');
    let cmd = it.next().unwrap_or_default();
    match cmd {
        "PING" => w.write_all(b"+PONG\n").await?,
        "GET" => {
            let v = state.table().get(arg(&mut it, "key")?);
            write_bulk(w, v.as_deref()).await?;
        }
        "SET" => {
            let key = arg(&mut it, "key")?;
            let ttl_ms: u64 = arg(&mut it, "ttl_ms")?.parse()?;
            let len: usize = arg(&mut it, "len")?.parse()?;
            if len > MAX_VALUE {
                return Err(anyhow!("value too large"));
            }
            let tags: Vec<&str> = it
                .next()
                .map(|t| t.split(',').collect())
                .unwrap_or_default();
            let mut buf = vec![0; len];
            r.read_exact(&mut buf).await?;
            let ttl = (ttl_ms > 0).then(|| Duration::from_millis(ttl_ms));
            state.table().set(key, Bytes::from(buf), ttl, &tags);
            w.write_all(b"+OK\n").await?;
        }
        "DEL" => {
            let n = u8::from(state.table().delete(arg(&mut it, "key")?));
            w.write_all(format!(":{n}\n").as_bytes()).await?;
        }
        "TAG" => {
            let n = state.table().invalidate_tag(arg(&mut it, "tag")?);
            w.write_all(format!(":{n}\n").as_bytes()).await?;
        }
        "STATS" => {
            let json = serde_json::to_vec(&state.table().stats())?;
            write_bulk(w, Some(&json)).await?;
        }
        "FLUSH" => {
            state.table().clear();
            w.write_all(b"+OK\n").await?;
        }
        "CONFIG" => {
            let capacity: usize = arg(&mut it, "capacity")?.parse()?;
            let policy: Policy = arg(&mut it, "policy")?
                .parse()
                .map_err(|e: String| anyhow!(e))?;
            let shards: usize = arg(&mut it, "shards")?.parse()?;
            state.replace(TableConfig {
                capacity,
                shards,
                policy,
                default_ttl: None,
            });
            tracing::info!(capacity, %policy, shards, "table reconfigured");
            w.write_all(b"+OK\n").await?;
        }
        other => return Err(anyhow!("unknown command `{other}`")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterClient, NodeClient};

    async fn spawn_node() -> String {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(serve(l, NodeState::new(TableConfig::default())));
        addr
    }

    #[tokio::test]
    async fn client_server_roundtrip() {
        let c = NodeClient::new(spawn_node().await);
        c.ping().await.unwrap();
        assert_eq!(c.get("k").await.unwrap(), None);
        c.set("k", b"hello\nworld", Duration::ZERO, &["t"])
            .await
            .unwrap();
        assert_eq!(
            c.get("k").await.unwrap().as_deref(),
            Some(&b"hello\nworld"[..])
        );
        assert_eq!(c.invalidate_tag("t").await.unwrap(), 1);
        assert!(!c.delete("k").await.unwrap());
        let s = c.stats().await.unwrap();
        assert_eq!((s.hits, s.misses, s.sets), (1, 1, 1));
        assert!(c.get("bad key").await.is_err());
    }

    #[tokio::test]
    async fn cluster_routes_and_broadcasts() {
        let mut nodes = Vec::new();
        for _ in 0..3 {
            nodes.push(Arc::new(NodeClient::new(spawn_node().await)));
        }
        let cl = ClusterClient::new(nodes);
        for i in 0..300 {
            cl.set(&format!("p:{i}"), b"x", Duration::ZERO, &["all"])
                .await
                .unwrap();
        }
        let stats = cl.stats().await.unwrap();
        assert!(
            stats.iter().all(|(_, s)| s.len > 50),
            "keys should spread over nodes"
        );
        assert_eq!(cl.invalidate_tag("all").await.unwrap(), 300);
    }

    #[tokio::test]
    async fn protocol_errors_keep_connection_open() {
        let c = NodeClient::new(spawn_node().await);
        assert!(c.configure(10, Policy::Lru, 1).await.is_ok());
        let err = c.call("BOGUS", None).await.unwrap_err();
        assert!(err.to_string().contains("unknown command"));
        c.ping().await.unwrap();
    }
}
