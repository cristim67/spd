//! Endpoint-uri de administrare (folosite de UI și de generatorul de încărcare).

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::ApiError;
use crate::cache::ModeConfig;
use crate::state::{AppState, CountersSnapshot, Runtime};

#[derive(Debug, Serialize, ToSchema)]
pub struct StatsResponse {
    pub instance: String,
    pub mode: ModeConfig,
    pub counters: CountersSnapshot,
    pub write_behind_pending: usize,
    /// Statistici interne ale backend-ului (L1 / noduri de cluster).
    #[schema(value_type = Object)]
    pub backend: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InfoResponse {
    pub instance: String,
    /// Prefixe URL ale tuturor instanțelor (pentru UI, prin nginx).
    pub instances: Vec<String>,
    pub cache_nodes: Vec<String>,
    pub redis: bool,
    pub pgmem: bool,
}

/// Starea curentă și contoarele instanței.
#[utoipa::path(get, path = "/admin/stats", tag = "admin", responses((status = 200, body = StatsResponse)))]
pub async fn stats(State(state): State<Arc<AppState>>) -> Json<StatsResponse> {
    let rt = state.runtime();
    Json(StatsResponse {
        instance: state.instance.clone(),
        mode: rt.cfg.clone(),
        counters: state.counters.snapshot(),
        write_behind_pending: state.wb_pending(),
        backend: rt.backend.stats().await,
    })
}

/// Informații despre topologie.
#[utoipa::path(get, path = "/admin/info", tag = "admin", responses((status = 200, body = InfoResponse)))]
pub async fn info(State(state): State<Arc<AppState>>) -> Json<InfoResponse> {
    let r = &state.resources;
    Json(InfoResponse {
        instance: state.instance.clone(),
        instances: state.ui_instances.clone(),
        cache_nodes: r.nodes.iter().map(|n| n.addr().to_owned()).collect(),
        redis: r.redis.is_some(),
        pgmem: r.pgmem.is_some(),
    })
}

/// Schimbă backend-ul / strategia la runtime (golește cache-ul, resetează contoarele).
#[utoipa::path(
    post, path = "/admin/mode", tag = "admin", request_body = ModeConfig,
    responses((status = 200, body = ModeConfig), (status = 400, description = "Backend indisponibil"))
)]
pub async fn set_mode(
    State(state): State<Arc<AppState>>,
    Json(cfg): Json<ModeConfig>,
) -> Result<Json<ModeConfig>, ApiError> {
    if cfg.capacity == 0 {
        return Err(ApiError::BadRequest("capacity must be > 0".into()));
    }
    let backend = state
        .resources
        .build(&cfg)
        .await
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    tracing::info!(?cfg, "mode changed");
    state.set_runtime(Runtime {
        cfg: cfg.clone(),
        backend,
    });
    Ok(Json(cfg))
}

/// Resetează contoarele (fără a goli cache-ul).
#[utoipa::path(post, path = "/admin/reset", tag = "admin", responses((status = 204)))]
pub async fn reset(State(state): State<Arc<AppState>>) -> axum::http::StatusCode {
    state.counters.reset();
    axum::http::StatusCode::NO_CONTENT
}
