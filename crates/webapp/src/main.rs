//! Aplicația web: API REST peste PostgreSQL cu cache configurabil la runtime.

mod admin;
mod api;
mod cache;
mod db;
mod metrics;
mod state;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::response::Html;
use axum::routing::get;
use clap::Parser;
use sqlx::postgres::{PgListener, PgPoolOptions};
use tokio::sync::mpsc;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::cache::{BackendKind, ModeConfig, Resources, WriteStrategy};
use crate::state::{AppState, Runtime};

/// mimalloc: mai rapid decât allocatorul glibc la multe alocări mici, concurente.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    #[arg(long, env = "LISTEN", default_value = "0.0.0.0:8080")]
    listen: String,
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "DB_POOL", default_value_t = 32)]
    db_pool: u32,
    #[arg(long, env = "REDIS_URL")]
    redis_url: Option<String>,
    #[arg(long, env = "PGMEM_URL")]
    pgmem_url: Option<String>,
    /// Adresele nodurilor de cache, separate prin virgulă.
    #[arg(long, env = "CACHE_NODES", value_delimiter = ',')]
    cache_nodes: Vec<String>,
    #[arg(long, env = "CACHE_BACKEND", default_value = "local")]
    cache_backend: String,
    /// Numărul de benzi (workeri) write-behind paralele.
    #[arg(long, env = "WB_LANES", default_value_t = 4)]
    wb_lanes: usize,
    #[arg(long, env = "INSTANCE_ID", default_value = "app")]
    instance: String,
    /// Prefixele URL ale instanțelor, pentru UI (ex. `/node/app1,/node/app2`).
    #[arg(long, env = "UI_INSTANCES", value_delimiter = ',')]
    ui_instances: Vec<String>,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Memory cache pentru aplicații web",
        description = "API demonstrativ: produse în PostgreSQL, cache configurabil la runtime \
                       (local, cluster, Redis, tabelă in-memory PostgreSQL, near-cache).",
        version = "0.1.0"
    ),
    paths(
        api::get_product, api::category_top, api::update_product,
        admin::stats, admin::info, admin::set_mode, admin::reset
    ),
    components(schemas(
        db::Product, db::TopProduct, db::ProductPatch,
        ModeConfig, BackendKind, WriteStrategy,
        admin::StatsResponse, admin::InfoResponse, state::CountersSnapshot
    )),
    tags((name = "products", description = "API de business"), (name = "admin", description = "Control și statistici"))
)]
struct ApiDoc;

async fn connect_retry(url: &str, size: u32) -> anyhow::Result<sqlx::PgPool> {
    let mut last = None;
    for _ in 0..30 {
        match PgPoolOptions::new()
            .max_connections(size)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
        {
            Ok(p) => return Ok(p),
            Err(e) => {
                tracing::warn!(error = %e, "database not ready, retrying");
                last = Some(e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    Err(last.map_or_else(|| anyhow::anyhow!("connect failed"), Into::into))
}

/// Ascultă notificările PostgreSQL (trigger pe `products`) și invalidează L1.
/// Astfel instanțele rămân coerente și la modificări făcute direct în DB.
async fn invalidation_listener(state: Arc<AppState>) {
    loop {
        let res: anyhow::Result<()> = async {
            let mut l = PgListener::connect_with(&state.db).await?;
            l.listen("cache_invalidation").await?;
            tracing::info!("listening for cache invalidations");
            loop {
                let n = l.recv().await?;
                let rt = state.runtime();
                let Some(l1) = rt.backend.l1() else { continue };
                if let Some((id, cat)) = n.payload().split_once(':') {
                    l1.delete(&format!("product:{id}"));
                    l1.invalidate_tag(&format!("cat:{cat}"));
                }
            }
        }
        .await;
        if let Err(e) = res {
            tracing::warn!(error = %e, "invalidation listener failed, reconnecting");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// Conectează PostgreSQL-ul in-memory, creează tabela-cache și pornește janitor-ul TTL
/// (Redis și `memtable` expiră intrările intern).
async fn connect_pgmem(url: &str, pool: u32) -> anyhow::Result<sqlx::PgPool> {
    let p = connect_retry(url, pool).await.context("pgmem database")?;
    cache::init_pgmem(&p).await?;
    let janitor = p.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let q = sqlx::query("DELETE FROM cache_kv WHERE expires_at <= now()");
            if let Err(e) = q.execute(&janitor).await {
                tracing::warn!(error = %e, "pgmem janitor failed");
            }
        }
    });
    Ok(p)
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = ctrl_c => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();
    let a = Args::parse();

    let db = connect_retry(&a.database_url, a.db_pool)
        .await
        .context("main database")?;
    let pgmem = match &a.pgmem_url {
        Some(u) => Some(connect_pgmem(u, a.db_pool).await?),
        None => None,
    };
    let redis = match &a.redis_url {
        Some(u) => Some(cache::RedisConn(
            redis::aio::ConnectionManager::new(redis::Client::open(u.as_str())?).await?,
        )),
        None => None,
    };
    let nodes = a
        .cache_nodes
        .iter()
        .map(|n| Arc::new(cache_node::NodeClient::new(n.clone())))
        .collect();
    let resources = Resources {
        redis,
        pgmem,
        nodes,
    };

    let cfg = ModeConfig {
        backend: serde_json::from_value(serde_json::Value::String(a.cache_backend.clone()))
            .context("CACHE_BACKEND")?,
        ..ModeConfig::default()
    };
    let backend = resources.build(&cfg).await?;

    // Benzi write-behind paralele, partiționate după id (ordine păstrată per produs).
    let (wb_lanes, wb_rxs): (Vec<_>, Vec<_>) = (0..a.wb_lanes.max(1))
        .map(|_| mpsc::channel(25_000))
        .unzip();
    let state = Arc::new(AppState::new(
        db,
        resources,
        Runtime { cfg, backend },
        wb_lanes,
        a.instance.clone(),
        a.ui_instances.clone(),
    ));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let workers: Vec<_> = wb_rxs
        .into_iter()
        .map(|rx| {
            tokio::spawn(state::write_behind_worker(
                Arc::clone(&state),
                rx,
                stop_rx.clone(),
            ))
        })
        .collect();
    tokio::spawn(invalidation_listener(Arc::clone(&state)));

    let app = Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("static/index.html")) }),
        )
        .route("/health", get(|| async { "ok" }))
        .route("/metrics", get(metrics::handler))
        .route(
            "/api/products/{id}",
            get(api::get_product).put(api::update_product),
        )
        .route("/api/categories/{id}/top", get(api::category_top))
        .route("/admin/stats", get(admin::stats))
        .route("/admin/info", get(admin::info))
        .route("/admin/mode", axum::routing::post(admin::set_mode))
        .route("/admin/reset", axum::routing::post(admin::reset))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(axum::middleware::from_fn(metrics::track))
        .with_state(Arc::clone(&state));

    let listener = tokio::net::TcpListener::bind(&a.listen).await?;
    tracing::info!(listen = %a.listen, instance = %a.instance, "webapp up");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Oprire: închidem coada write-behind și așteptăm golirea ei (fără pierderi).
    tracing::info!(
        pending = state.wb_pending(),
        "shutting down, draining write-behind"
    );
    let _ = stop_tx.send(true);
    let drain = async {
        for w in workers {
            let _ = w.await;
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(20), drain).await;
    Ok(())
}
