use clap::Parser;
use memtable::{Policy, TableConfig};
use tokio::net::TcpListener;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Nod de cache distribuit bazat pe `memtable`.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    #[arg(long, env = "LISTEN", default_value = "0.0.0.0:7000")]
    listen: String,
    #[arg(long, env = "CAPACITY", default_value_t = 100_000)]
    capacity: usize,
    #[arg(long, env = "SHARDS", default_value_t = 16)]
    shards: usize,
    #[arg(long, env = "POLICY", default_value = "lru")]
    policy: Policy,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let a = Args::parse();
    let listener = TcpListener::bind(&a.listen).await?;
    tracing::info!(listen = %a.listen, capacity = a.capacity, policy = %a.policy, "cache node up");
    let state = cache_node::server::NodeState::new(TableConfig {
        capacity: a.capacity,
        shards: a.shards,
        policy: a.policy,
        default_ttl: None,
    });
    cache_node::server::serve(listener, state).await
}
