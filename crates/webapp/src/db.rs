//! Accesul la baza de date relațională (sursa de adevăr).

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;

/// Produs, așa cum e expus de API (și stocat în cache ca JSON).
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct Product {
    pub id: i64,
    pub category_id: i32,
    pub name: String,
    pub description: String,
    pub price_cents: i64,
    pub stock: i32,
    /// Incrementat la fiecare modificare (util pentru detectarea datelor vechi).
    pub version: i64,
    /// Media notelor din recenzii.
    pub rating: f64,
    pub reviews: i64,
}

/// Rând din clasamentul unei categorii.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub struct TopProduct {
    pub id: i64,
    pub name: String,
    pub price_cents: i64,
    pub rating: f64,
    pub reviews: i64,
}

/// Modificare parțială a unui produs.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema)]
pub struct ProductPatch {
    pub price_cents: Option<i64>,
    pub stock: Option<i32>,
}

impl ProductPatch {
    pub fn apply(&self, p: &mut Product) {
        if let Some(v) = self.price_cents {
            p.price_cents = v;
        }
        if let Some(v) = self.stock {
            p.stock = v;
        }
        p.version += 1;
    }

    /// Combină două modificări (ultima scriere câștigă, pe câmp).
    pub fn merge(&mut self, newer: &Self) {
        self.price_cents = newer.price_cents.or(self.price_cents);
        self.stock = newer.stock.or(self.stock);
    }
}

/// Coloanele lui [`Product`]; agregatele sunt materializate în `products`.
const PRODUCT_COLS: &str = "id, category_id, name, description, price_cents, stock, version, \
                            rating_avg AS rating, review_count::int8 AS reviews";

pub async fn product(db: &PgPool, id: i64) -> sqlx::Result<Option<Product>> {
    sqlx::query_as(&format!(
        "SELECT {PRODUCT_COLS} FROM products WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(db)
    .await
}

/// Clasamentul unei categorii: index scan pe `products_category_top_idx` + `LIMIT`.
pub async fn category_top(db: &PgPool, category: i32, limit: i64) -> sqlx::Result<Vec<TopProduct>> {
    sqlx::query_as(
        "SELECT id, name, price_cents, rating_avg AS rating, review_count::int8 AS reviews
         FROM products
         WHERE category_id = $1
         ORDER BY rating_avg DESC, review_count DESC, id
         LIMIT $2",
    )
    .bind(category)
    .bind(limit)
    .fetch_all(db)
    .await
}

/// Valoarea pentru `app.notify` (vezi trigger-ul din `init.sql`).
fn notify_flag(notify: bool) -> &'static str {
    if notify { "on" } else { "off" }
}

/// Aplică modificarea și returnează produsul actualizat (o singură interogare,
/// prin `RETURNING`), sau `None` dacă nu există.
/// `notify = false` suprimă `NOTIFY`-ul trigger-ului (inutil fără cache L1).
pub async fn update_product(
    db: &PgPool,
    id: i64,
    patch: ProductPatch,
    notify: bool,
) -> sqlx::Result<Option<Product>> {
    // `set_config(..., true)` e local tranzacției implicite a acestei instrucțiuni.
    sqlx::query_as(&format!(
        "UPDATE products
         SET price_cents = COALESCE($2, price_cents), stock = COALESCE($3, stock),
             version = version + 1, updated_at = now()
         FROM (SELECT set_config('app.notify', $4, true)) AS cfg
         WHERE id = $1
         RETURNING {PRODUCT_COLS}"
    ))
    .bind(id)
    .bind(patch.price_cents)
    .bind(patch.stock)
    .bind(notify_flag(notify))
    .fetch_optional(db)
    .await
}

/// Scriere în lot (write-behind): un singur `UPDATE ... FROM unnest(...)`.
pub async fn update_batch(
    db: &PgPool,
    batch: &[(i64, ProductPatch)],
    notify: bool,
) -> sqlx::Result<u64> {
    let ids: Vec<i64> = batch.iter().map(|(id, _)| *id).collect();
    let prices: Vec<Option<i64>> = batch.iter().map(|(_, p)| p.price_cents).collect();
    let stocks: Vec<Option<i32>> = batch.iter().map(|(_, p)| p.stock).collect();
    let res = sqlx::query(
        "UPDATE products p
         SET price_cents = COALESCE(u.price, p.price_cents), stock = COALESCE(u.stock, p.stock),
             version = p.version + 1, updated_at = now()
         FROM unnest($1::bigint[], $2::bigint[], $3::int[]) AS u(id, price, stock),
              (SELECT set_config('app.notify', $4, true)) AS cfg
         WHERE p.id = u.id",
    )
    .bind(&ids)
    .bind(&prices)
    .bind(&stocks)
    .bind(notify_flag(notify))
    .execute(db)
    .await?;
    Ok(res.rows_affected())
}
