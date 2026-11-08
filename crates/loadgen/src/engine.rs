//! Scalabilitatea motorului `memtable`: debit (Mops/s) în funcție de numărul de
//! fire și de shard-uri. Configurația 1 fir × 1 shard este baseline-ul secvențial;
//! cu un singur shard, firele se serializează pe același mutex (contenție).

use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use bytes::Bytes;
use memtable::{Table, TableConfig};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long, value_delimiter = ',', default_value = "1,2,4,8")]
    threads: Vec<usize>,
    #[arg(long, value_delimiter = ',', default_value = "1,4,16,64")]
    shards: Vec<usize>,
    #[arg(long, default_value_t = 2_000_000)]
    ops_per_thread: usize,
    #[arg(long, default_value_t = 100_000)]
    keys: usize,
    #[arg(long, default_value_t = 0.9)]
    read_ratio: f64,
    #[arg(long, default_value_t = 0.99)]
    zipf: f64,
    #[arg(long, default_value = "lru")]
    policy: memtable::Policy,
    #[arg(long, default_value_t = 3)]
    repeats: usize,
    #[arg(long, default_value = "results/engine_scaling.csv")]
    out: PathBuf,
}

/// Rulează o configurație și returnează milioane de operații pe secundă.
fn measure(a: &Args, keys: &Arc<Vec<String>>, threads: usize, shards: usize) -> f64 {
    let table = Arc::new(Table::new(TableConfig {
        capacity: a.keys,
        shards,
        policy: a.policy,
        ..TableConfig::default()
    }));
    let value = Bytes::from(vec![b'x'; 256]);
    for k in keys.iter() {
        table.set(k, value.clone(), None, &[]);
    }
    let barrier = Arc::new(Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (table, keys, barrier, value) = (
                Arc::clone(&table),
                Arc::clone(keys),
                Arc::clone(&barrier),
                value.clone(),
            );
            let (n, rr, s) = (a.ops_per_thread, a.read_ratio, a.zipf);
            std::thread::spawn(move || {
                // Pre-generăm indicii ca eșantionarea Zipf să nu fie măsurată.
                let mut rng = SmallRng::seed_from_u64(t as u64);
                let z = Zipf::new(keys.len() as u64, s).expect("valid zipf");
                let ops: Vec<(usize, bool)> = (0..n)
                    .map(|_| (z.sample(&mut rng) as usize - 1, rng.r#gen::<f64>() < rr))
                    .collect();
                barrier.wait();
                for (i, read) in ops {
                    if read {
                        std::hint::black_box(table.get(&keys[i]));
                    } else {
                        table.set(&keys[i], value.clone(), None, &[]);
                    }
                }
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    for h in handles {
        h.join().expect("worker panicked");
    }
    (threads * a.ops_per_thread) as f64 / start.elapsed().as_secs_f64() / 1e6
}

pub fn run(a: &Args) -> anyhow::Result<()> {
    let keys = Arc::new(
        (0..a.keys)
            .map(|i| format!("product:{i}"))
            .collect::<Vec<_>>(),
    );
    let mut rows = Vec::new();
    for &shards in &a.shards {
        let mut base = None;
        for &threads in &a.threads {
            let mut runs: Vec<f64> = (0..a.repeats)
                .map(|_| measure(a, &keys, threads, shards))
                .collect();
            runs.sort_by(f64::total_cmp);
            let median = runs[runs.len() / 2];
            let b = *base.get_or_insert(median);
            eprintln!(
                "shards={shards:3} threads={threads:2} {median:7.2} Mops/s speedup={:.2}",
                median / b
            );
            rows.push(vec![
                a.policy.to_string(),
                a.zipf.to_string(),
                shards.to_string(),
                threads.to_string(),
                format!("{median:.3}"),
                format!("{:.3}", median / b),
            ]);
        }
    }
    crate::csv::append(
        &a.out,
        &["policy", "zipf", "shards", "threads", "mops", "speedup"],
        &rows,
    )?;
    eprintln!("wrote {}", a.out.display());
    Ok(())
}
