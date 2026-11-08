//! Generator de încărcare și benchmark-uri offline.
//!
//! * `http`     — încărcare HTTP închisă (closed-loop) cu distribuție Zipf;
//! * `simulate` — simulare trace-driven a ratei de hit pentru politicile de evacuare;
//! * `engine`   — scalabilitatea motorului `memtable` (fire × shard-uri).

mod csv;
mod engine;
mod http;
mod sim;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    Http(http::Args),
    Simulate(sim::Args),
    Engine(engine::Args),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().cmd {
        Cmd::Http(a) => http::run(a).await,
        Cmd::Simulate(a) => sim::run(&a),
        Cmd::Engine(a) => engine::run(&a),
    }
}
