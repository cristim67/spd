//! API-ul public (documentat `OpenAPI` / Swagger).

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::Deserialize;
use utoipa::IntoParams;

use crate::cache::WriteStrategy;
use crate::db::{self, Product, ProductPatch};
use crate::state::{AppState, WbUpdate};

/// Eroare HTTP uniformă: `{"error": "..."}`.
#[derive(Debug)]
pub enum ApiError {
    NotFound,
    BadRequest(String),
    Internal(anyhow::Error),
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        Self::Internal(e.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "not found".to_owned()),
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            Self::Internal(e) => {
                tracing::error!(error = %e, "request failed");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        };
        (code, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

/// Răspuns JSON deja serializat + antetul `X-Cache: HIT|MISS`.
fn json_bytes(body: Bytes, hit: bool) -> Response {
    let mut r = body.into_response();
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    h.insert(
        "x-cache",
        HeaderValue::from_static(if hit { "HIT" } else { "MISS" }),
    );
    r
}

pub fn product_key(id: i64) -> String {
    format!("product:{id}")
}

pub fn category_tag(c: i32) -> String {
    format!("cat:{c}")
}

async fn load_product(state: &AppState, id: i64) -> anyhow::Result<Option<Bytes>> {
    Ok(db::product(&state.db, id)
        .await?
        .map(|p| serde_json::to_vec(&p))
        .transpose()?
        .map(Bytes::from))
}

/// Detaliile unui produs (cu rating agregat din recenzii).
#[utoipa::path(
    get, path = "/api/products/{id}", tag = "products",
    params(("id" = i64, Path, description = "ID produs")),
    responses(
        (status = 200, body = Product, description = "Produs; antetul X-Cache indică HIT/MISS"),
        (status = 404, description = "Inexistent")
    )
)]
pub async fn get_product(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Response, ApiError> {
    let key = product_key(id);
    let (v, hit) = state
        .read_through(&key, &[], || load_product(&state, id))
        .await?;
    v.map(|b| json_bytes(b, hit)).ok_or(ApiError::NotFound)
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct TopQuery {
    /// Numărul de produse (1..=100, implicit 10).
    pub limit: Option<i64>,
}

/// Top produse dintr-o categorie după rating.
#[utoipa::path(
    get, path = "/api/categories/{id}/top", tag = "products",
    params(("id" = i32, Path, description = "ID categorie"), TopQuery),
    responses((status = 200, body = [db::TopProduct]))
)]
pub async fn category_top(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
    Query(q): Query<TopQuery>,
) -> Result<Response, ApiError> {
    let limit = q.limit.unwrap_or(10);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::BadRequest("limit must be in 1..=100".into()));
    }
    let key = format!("cat-top:{id}:{limit}");
    let tag = category_tag(id);
    let (v, hit) = state
        .read_through(&key, &[&tag], || async {
            let rows = db::category_top(&state.db, id, limit).await?;
            Ok(Some(Bytes::from(serde_json::to_vec(&rows)?)))
        })
        .await?;
    v.map(|b| json_bytes(b, hit)).ok_or(ApiError::NotFound)
}

/// Modifică prețul și/sau stocul. Propagarea în cache depinde de strategia activă.
#[utoipa::path(
    put, path = "/api/products/{id}", tag = "products",
    params(("id" = i64, Path, description = "ID produs")),
    request_body = ProductPatch,
    responses(
        (status = 200, body = Product, description = "Produsul după modificare"),
        (status = 202, body = Product, description = "Acceptat (write-behind, scris asincron în DB)"),
        (status = 404, description = "Inexistent")
    )
)]
pub async fn update_product(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(patch): Json<ProductPatch>,
) -> Result<Response, ApiError> {
    if patch.price_cents.is_some_and(|p| p < 0) || patch.stock.is_some_and(|s| s < 0) {
        return Err(ApiError::BadRequest(
            "price_cents and stock must be >= 0".into(),
        ));
    }
    let rt = state.runtime();
    let key = product_key(id);
    let strategy = if rt.backend.is_none() {
        WriteStrategy::Invalidate
    } else {
        rt.cfg.strategy
    };

    match strategy {
        WriteStrategy::Invalidate | WriteStrategy::WriteThrough => {
            state.counters.db_writes.fetch_add(1, Relaxed);
            crate::metrics::DB_WRITE.inc();
            let updated = db::update_product(&state.db, id, patch, rt.backend.l1().is_some())
                .await?
                .ok_or(ApiError::NotFound)?;
            let cat = updated.category_id;
            let fresh = Bytes::from(serde_json::to_vec(&updated)?);
            if strategy == WriteStrategy::WriteThrough {
                state.cache_set(&key, &fresh, &[]).await;
                state.invalidate(None, Some(&category_tag(cat))).await;
            } else {
                state.invalidate(Some(&key), Some(&category_tag(cat))).await;
            }
            Ok(json_bytes(fresh, false))
        }
        WriteStrategy::WriteBehind => {
            let (cur, _) = state
                .read_through(&key, &[], || load_product(&state, id))
                .await?;
            let mut p: Product = serde_json::from_slice(&cur.ok_or(ApiError::NotFound)?)?;
            patch.apply(&mut p);
            let body = Bytes::from(serde_json::to_vec(&p)?);
            state.cache_set(&key, &body, &[]).await;
            state
                .enqueue_write(WbUpdate {
                    id,
                    category_id: p.category_id,
                    patch,
                })
                .await?;
            crate::metrics::WB_QUEUE.set(state.wb_pending() as i64);
            let mut r = json_bytes(body, true);
            *r.status_mut() = StatusCode::ACCEPTED;
            Ok(r)
        }
    }
}
