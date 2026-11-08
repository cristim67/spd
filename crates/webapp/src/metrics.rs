//! Metrici Prometheus + middleware de măsurare a latenței per rută.

use std::sync::LazyLock;
use std::time::Instant;

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

fn register<C: prometheus::core::Collector + Clone + 'static>(c: C) -> C {
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("metric registered once");
    c
}

pub static HTTP_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(
        HistogramVec::new(
            HistogramOpts::new("http_request_duration_seconds", "HTTP latency per route").buckets(
                vec![
                    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0,
                ],
            ),
            &["route", "method", "status"],
        )
        .expect("valid metric"),
    )
});

pub static CACHE_LOOKUPS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(
        IntCounterVec::new(
            Opts::new("cache_lookups_total", "Cache lookups"),
            &["result"],
        )
        .expect("valid metric"),
    )
});

pub static DB_QUERIES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(
        IntCounterVec::new(
            Opts::new("db_queries_total", "Queries sent to PostgreSQL"),
            &["kind"],
        )
        .expect("valid metric"),
    )
});

pub static CACHE_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(
        IntCounterVec::new(
            Opts::new("cache_errors_total", "Cache backend errors"),
            &["op"],
        )
        .expect("valid metric"),
    )
});

pub static WB_QUEUE: LazyLock<IntGauge> = LazyLock::new(|| {
    register(
        IntGauge::new("write_behind_queue_depth", "Pending write-behind updates").expect("valid"),
    )
});

// Contoare pre-rezolvate: evită căutarea label-urilor (hash + lock) la fiecare cerere.
pub static CACHE_HIT: LazyLock<IntCounter> =
    LazyLock::new(|| CACHE_LOOKUPS.with_label_values(&["hit"]));
pub static CACHE_MISS: LazyLock<IntCounter> =
    LazyLock::new(|| CACHE_LOOKUPS.with_label_values(&["miss"]));
pub static DB_READ: LazyLock<IntCounter> =
    LazyLock::new(|| DB_QUERIES.with_label_values(&["read"]));
pub static DB_WRITE: LazyLock<IntCounter> =
    LazyLock::new(|| DB_QUERIES.with_label_values(&["write"]));
pub static DB_WRITE_BATCH: LazyLock<IntCounter> =
    LazyLock::new(|| DB_QUERIES.with_label_values(&["write_batch"]));

pub async fn track(req: Request, next: Next) -> Response {
    // `MatchedPath` e un `Arc<str>`: clonarea nu alocă.
    let route = req.extensions().get::<MatchedPath>().cloned();
    let method = req.method().clone();
    let start = Instant::now();
    let resp = next.run(req).await;
    let route = route.as_ref().map_or("unmatched", MatchedPath::as_str);
    HTTP_LATENCY
        .with_label_values(&[route, method.as_str(), resp.status().as_str()])
        .observe(start.elapsed().as_secs_f64());
    resp
}

pub async fn handler() -> impl IntoResponse {
    let mut buf = Vec::new();
    let _ = TextEncoder::new().encode(&REGISTRY.gather(), &mut buf);
    ([("content-type", "text/plain; version=0.0.4")], buf)
}
