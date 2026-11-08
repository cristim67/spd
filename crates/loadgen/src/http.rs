//! Încărcare HTTP closed-loop: `concurrency` clienți, fiecare trimite cererea
//! următoare imediat după răspuns. Latențele sunt înregistrate în histograme HDR
//! (per worker, combinate la final). Debitul și latența sunt legate prin legea
//! lui Little: `L = λ · W` (L = concurența).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use serde_json::Value;

#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// URL-ul de bază pentru trafic (ex. load balancer-ul nginx).
    #[arg(long, default_value = "http://nginx")]
    url: String,
    /// URL-urile instanțelor, pentru /admin (mod, reset, statistici).
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "http://app1:8080,http://app2:8080"
    )]
    admin: Vec<String>,
    /// Configurația `ModeConfig` (JSON) aplicată tuturor instanțelor înainte de rulare.
    #[arg(long)]
    mode: Option<String>,
    #[arg(long, default_value_t = 20)]
    duration: u64,
    #[arg(long, default_value_t = 5)]
    warmup: u64,
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    /// Debit țintă total (cereri/s). Absent = closed-loop (debit maxim). Cu `--rate`,
    /// latența se măsoară de la momentul *programat* al cererii (fără coordinated omission).
    #[arg(long)]
    rate: Option<f64>,
    /// Fracțiunea de citiri (restul sunt PUT pe produse).
    #[arg(long, default_value_t = 0.95)]
    read_ratio: f64,
    /// Din citiri, fracțiunea care cere clasamentul unei categorii (interogare grea).
    #[arg(long, default_value_t = 0.1)]
    category_ratio: f64,
    #[arg(long, default_value_t = 0.99)]
    zipf: f64,
    #[arg(long, default_value_t = 100_000)]
    keys: u64,
    #[arg(long, default_value_t = 200)]
    categories: u64,
    /// Numele experimentului (prima coloană din CSV).
    #[arg(long, default_value = "adhoc")]
    experiment: String,
    /// Etichetă liberă (a doua coloană din CSV), ex. valoarea parametrului variat.
    #[arg(long, default_value = "")]
    label: String,
    #[arg(long, default_value = "results/http.csv")]
    out: PathBuf,
}

const HEADER: &[&str] = &[
    "experiment",
    "label",
    "backend",
    "strategy",
    "policy",
    "capacity",
    "nodes",
    "coalesce",
    "concurrency",
    "read_ratio",
    "zipf",
    "instances",
    "duration_s",
    "requests",
    "errors",
    "throughput_rps",
    "mean_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "p999_ms",
    "max_ms",
    "hit_ratio",
    "db_reads",
    "db_writes",
    "coalesced",
];

#[derive(Debug, Default)]
struct WorkerResult {
    hist: Option<Histogram<u64>>,
    requests: u64,
    errors: u64,
}

async fn worker(
    c: reqwest::Client,
    a: Args,
    seed: u64,
    until: Instant,
    record: bool,
) -> WorkerResult {
    let mut rng = SmallRng::seed_from_u64(seed);
    let zk = Zipf::new(a.keys, a.zipf).expect("valid zipf");
    let zc = Zipf::new(a.categories, a.zipf).expect("valid zipf");
    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("valid bounds");
    let (mut requests, mut errors) = (0, 0);
    // Open-loop: fiecare worker are un orar fix, decalat ca sosirile să fie uniforme.
    let interval = a
        .rate
        .map(|r| Duration::from_secs_f64(a.concurrency as f64 / r));
    let mut next = Instant::now() + interval.map_or(Duration::ZERO, |i| i.mul_f64(rng.r#gen()));
    while Instant::now() < until {
        let req = if rng.r#gen::<f64>() < a.read_ratio {
            if rng.r#gen::<f64>() < a.category_ratio {
                let cat = zc.sample(&mut rng) as u64;
                c.get(format!("{}/api/categories/{cat}/top", a.url))
            } else {
                let id = zk.sample(&mut rng) as u64;
                c.get(format!("{}/api/products/{id}", a.url))
            }
        } else {
            let id = zk.sample(&mut rng) as u64;
            let price: i64 = rng.gen_range(100..100_000);
            c.put(format!("{}/api/products/{id}", a.url))
                .json(&serde_json::json!({ "price_cents": price }))
        };
        let start = match interval {
            Some(i) => {
                tokio::time::sleep_until(next.into()).await;
                let scheduled = next;
                next += i;
                scheduled
            }
            None => Instant::now(),
        };
        let ok = match req.send().await {
            Ok(r) => {
                let ok = r.status().is_success();
                let _ = r.bytes().await;
                ok
            }
            Err(_) => false,
        };
        if record {
            requests += 1;
            if ok {
                let _ = hist.record(start.elapsed().as_micros().max(1) as u64);
            } else {
                errors += 1;
            }
        }
    }
    WorkerResult {
        hist: Some(hist),
        requests,
        errors,
    }
}

async fn phase(c: &reqwest::Client, a: &Args, secs: u64, record: bool) -> WorkerResult {
    let until = Instant::now() + Duration::from_secs(secs);
    let tasks: Vec<_> = (0..a.concurrency)
        .map(|i| {
            tokio::spawn(worker(
                c.clone(),
                a.clone(),
                i as u64 * 7919 + u64::from(record),
                until,
                record,
            ))
        })
        .collect();
    let mut total = WorkerResult::default();
    for t in tasks {
        let r = t.await.expect("worker panicked");
        total.requests += r.requests;
        total.errors += r.errors;
        if let Some(h) = r.hist {
            match &mut total.hist {
                Some(acc) => acc.add(h).expect("compatible histograms"),
                None => total.hist = Some(h),
            }
        }
    }
    total
}

async fn admin_post(
    c: &reqwest::Client,
    base: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<()> {
    let mut req = c.post(format!("{base}{path}"));
    if let Some(b) = body {
        req = req.json(b);
    }
    let r = req
        .send()
        .await
        .with_context(|| format!("POST {base}{path}"))?;
    if !r.status().is_success() {
        bail!(
            "POST {base}{path}: {} {}",
            r.status(),
            r.text().await.unwrap_or_default()
        );
    }
    Ok(())
}

pub async fn run(a: Args) -> Result<()> {
    let c = reqwest::Client::builder()
        .pool_max_idle_per_host(a.concurrency)
        .timeout(Duration::from_secs(30))
        .build()?;

    if let Some(m) = &a.mode {
        let v: Value = serde_json::from_str(m).context("--mode must be JSON")?;
        for base in &a.admin {
            admin_post(&c, base, "/admin/mode", Some(&v)).await?;
        }
    }
    if a.warmup > 0 {
        eprintln!("[{}] warmup {}s", a.experiment, a.warmup);
        phase(&c, &a, a.warmup, false).await;
    }
    for base in &a.admin {
        admin_post(&c, base, "/admin/reset", None).await?;
    }
    eprintln!(
        "[{}] measuring {}s, concurrency {}",
        a.experiment, a.duration, a.concurrency
    );
    let started = Instant::now();
    let res = phase(&c, &a, a.duration, true).await;
    let elapsed = started.elapsed().as_secs_f64();

    let (mut hits, mut misses, mut db_reads, mut db_writes, mut coalesced) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut mode = Value::Null;
    for base in &a.admin {
        let stats: Value = c
            .get(format!("{base}/admin/stats"))
            .send()
            .await?
            .json()
            .await?;
        let n = |k: &str| stats["counters"][k].as_u64().unwrap_or(0);
        hits += n("hits");
        misses += n("misses");
        db_reads += n("db_reads");
        db_writes += n("db_writes");
        coalesced += n("coalesced");
        mode = stats["mode"].clone();
    }
    let hist = res.hist.context("no histogram")?;
    let ms = |q: f64| format!("{:.3}", hist.value_at_quantile(q) as f64 / 1000.0);
    let hit_ratio = if hits + misses == 0 {
        0.0
    } else {
        hits as f64 / (hits + misses) as f64
    };
    let text = |v: &Value| v.as_str().map_or_else(|| v.to_string(), str::to_owned);
    let row = vec![
        a.experiment.clone(),
        a.label.clone(),
        text(&mode["backend"]),
        text(&mode["strategy"]),
        text(&mode["policy"]),
        text(&mode["capacity"]),
        text(&mode["nodes"]),
        text(&mode["coalesce"]),
        a.concurrency.to_string(),
        a.read_ratio.to_string(),
        a.zipf.to_string(),
        a.admin.len().to_string(),
        format!("{elapsed:.2}"),
        res.requests.to_string(),
        res.errors.to_string(),
        format!("{:.1}", (res.requests - res.errors) as f64 / elapsed),
        format!("{:.3}", hist.mean() / 1000.0),
        ms(0.5),
        ms(0.95),
        ms(0.99),
        ms(0.999),
        format!("{:.3}", hist.max() as f64 / 1000.0),
        format!("{hit_ratio:.4}"),
        db_reads.to_string(),
        db_writes.to_string(),
        coalesced.to_string(),
    ];
    eprintln!(
        "[{}] {} rps={} p50={}ms p99={}ms hit={hit_ratio:.3} errors={}",
        a.experiment, row[2], row[15], row[17], row[19], res.errors
    );
    crate::csv::append(&a.out, HEADER, &[row])
}
