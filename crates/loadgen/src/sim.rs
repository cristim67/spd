//! Simulare trace-driven: rata de hit în funcție de capacitate, pe politici.
//!
//! Model IRM (Independent Reference Model) cu popularitate Zipf; opțional cu
//! „scanări" secvențiale peste chei reci (poluarea cache-ului, punctul slab al LRU).
//! Linia `ideal` este limita superioară teoretică pentru IRM: cache-ul static cu
//! cele mai populare C chei, `H(C) = sum_{i<=C} i^-s / sum_{i<=N} i^-s`.

use std::path::PathBuf;

use bytes::Bytes;
use memtable::{Policy, Table, TableConfig};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long, default_value_t = 100_000)]
    keys: u64,
    #[arg(long, default_value_t = 2_000_000)]
    requests: usize,
    /// Exponenții Zipf simulați.
    #[arg(long, value_delimiter = ',', default_value = "0.8,0.99")]
    zipf: Vec<f64>,
    /// Capacități, ca procent din numărul de chei.
    #[arg(long, value_delimiter = ',', default_value = "1,2,5,10,20,30,50")]
    capacities: Vec<f64>,
    #[arg(long, default_value = "results/sim_hit_ratio.csv")]
    out: PathBuf,
}

/// Generează cheile cererilor. `scan_frac` din cereri sunt o scanare secvențială
/// peste un spațiu rece, disjunct (chei care nu se repetă curând).
fn trace(keys: u64, n: usize, s: f64, scan_frac: f64, seed: u64) -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let z = Zipf::new(keys, s).expect("valid zipf");
    let mut cold = keys;
    (0..n)
        .map(|_| {
            if rng.r#gen::<f64>() < scan_frac {
                cold += 1;
                keys + (cold % (keys * 10))
            } else {
                z.sample(&mut rng) as u64
            }
        })
        .collect()
}

fn hit_ratio(trace: &[u64], capacity: usize, policy: Policy) -> f64 {
    let t = Table::new(TableConfig {
        capacity,
        shards: 1,
        policy,
        default_ttl: None,
    });
    let keys: Vec<String> = trace.iter().map(u64::to_string).collect();
    // Prima jumătate încălzește cache-ul; măsurăm doar a doua.
    let (warm, measure) = keys.split_at(keys.len() / 2);
    for k in warm {
        if t.get(k).is_none() {
            t.set(k, Bytes::new(), None, &[]);
        }
    }
    let mut hits = 0usize;
    for k in measure {
        if t.get(k).is_some() {
            hits += 1;
        } else {
            t.set(k, Bytes::new(), None, &[]);
        }
    }
    hits as f64 / measure.len() as f64
}

/// Limita teoretică IRM pentru un cache static cu cele mai populare `c` chei.
pub fn ideal_hit_ratio(keys: u64, c: u64, s: f64) -> f64 {
    let h = |n: u64| (1..=n).map(|i| (i as f64).powf(-s)).sum::<f64>();
    h(c.min(keys)) / h(keys)
}

pub fn run(a: &Args) -> anyhow::Result<()> {
    let mut rows = Vec::new();
    for &s in &a.zipf {
        for (wl, scan) in [("zipf", 0.0), ("zipf+scan", 0.2)] {
            let tr = trace(a.keys, a.requests, s, scan, 42);
            for &pct in &a.capacities {
                let cap = ((a.keys as f64) * pct / 100.0).round() as usize;
                for p in [Policy::Lru, Policy::Fifo, Policy::Clock] {
                    let hr = hit_ratio(&tr, cap, p);
                    eprintln!("s={s} {wl:9} cap={pct:>4}% {p:5} hit={hr:.4}");
                    rows.push(vec![
                        wl.into(),
                        s.to_string(),
                        p.to_string(),
                        pct.to_string(),
                        format!("{hr:.5}"),
                    ]);
                }
                if scan == 0.0 {
                    let ideal = ideal_hit_ratio(a.keys, cap as u64, s);
                    rows.push(vec![
                        wl.into(),
                        s.to_string(),
                        "ideal".into(),
                        pct.to_string(),
                        format!("{ideal:.5}"),
                    ]);
                }
            }
        }
    }
    crate::csv::append(
        &a.out,
        &["workload", "zipf", "policy", "capacity_pct", "hit_ratio"],
        &rows,
    )?;
    eprintln!("wrote {}", a.out.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ideal_bounds() {
        assert!((ideal_hit_ratio(1000, 1000, 0.99) - 1.0).abs() < 1e-12);
        let a = ideal_hit_ratio(1000, 100, 0.99);
        assert!(a > 0.5 && a < 1.0);
    }

    #[test]
    fn lru_beats_nothing_and_respects_ideal() {
        let tr = trace(10_000, 200_000, 0.99, 0.0, 1);
        let lru = hit_ratio(&tr, 1000, Policy::Lru);
        let ideal = ideal_hit_ratio(10_000, 1000, 0.99);
        assert!(lru > 0.3, "lru {lru}");
        assert!(lru <= ideal + 0.02, "lru {lru} vs ideal {ideal}");
    }
}
