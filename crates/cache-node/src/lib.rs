//! Nod de cache distribuit și clientul de cluster.
//!
//! Protocol text, binary-safe (inspirat din RESP/memcached), câte o comandă
//! pe linie, payload-ul urmând imediat după antet:
//!
//! ```text
//! PING                                  -> +PONG
//! GET <key>                             -> $<len>\n<bytes> | $-1
//! SET <key> <ttl_ms> <len> [t1,t2]\n<bytes> -> +OK
//! DEL <key>                             -> :1 | :0
//! TAG <tag>                             -> :<n>         (invalidare pe tag)
//! STATS                                 -> $<len>\n<json>
//! FLUSH                                 -> +OK
//! CONFIG <capacity> <policy> <shards>   -> +OK          (recreează tabela)
//! ```
//! Erorile sunt raportate ca `-ERR <mesaj>`. Răspunsurile sunt trimise
//! (flush) doar când bufferul de intrare e gol, deci clienții pot face
//! *pipelining*.

pub mod client;
pub mod ring;
pub mod server;

pub use client::{ClusterClient, NodeClient};
pub use ring::Ring;
