-- Schema și date de test pentru baza relațională (sursa de adevăr).
CREATE TABLE categories (
    id   int  PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE products (
    id          bigint      PRIMARY KEY,
    category_id int         NOT NULL REFERENCES categories (id),
    name        text        NOT NULL,
    description text        NOT NULL,
    price_cents bigint      NOT NULL CHECK (price_cents >= 0),
    stock       int         NOT NULL CHECK (stock >= 0),
    version     bigint      NOT NULL DEFAULT 1,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    -- Agregate materializate din `reviews` (întreținute de trigger, vezi mai jos):
    -- citirile nu mai fac JOIN + GROUP BY.
    rating_avg   float8     NOT NULL DEFAULT 0,
    review_count int        NOT NULL DEFAULT 0
) WITH (fillfactor = 90);  -- spațiu liber în pagini => UPDATE-uri HOT (fără scrieri în indecși)

CREATE TABLE reviews (
    id         bigserial   PRIMARY KEY,
    product_id bigint      NOT NULL REFERENCES products (id),
    rating     smallint    NOT NULL CHECK (rating BETWEEN 1 AND 5),
    body       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- 200 categorii, 100.000 produse, 500.000 recenzii.
INSERT INTO categories
SELECT g, 'Categoria ' || g FROM generate_series(1, 200) g;

INSERT INTO products (id, category_id, name, description, price_cents, stock)
SELECT g, 1 + (g % 200), 'Produs ' || g,
       md5(g::text) || ' ' || md5((g * 7)::text) || ' ' || md5((g * 13)::text),
       (random() * 100000)::bigint, (random() * 1000)::int
FROM generate_series(1, 100000) g;

INSERT INTO reviews (product_id, rating, body)
SELECT 1 + (g % 100000), 1 + (random() * 4)::int, md5(g::text)
FROM generate_series(1, 500000) g;

UPDATE products p
SET rating_avg = s.avg, review_count = s.cnt
FROM (SELECT product_id, avg(rating)::float8 AS avg, count(*)::int AS cnt
      FROM reviews GROUP BY product_id) s
WHERE p.id = s.product_id;

CREATE INDEX reviews_product_idx ON reviews (product_id);
-- Clasamentul unei categorii devine un index scan cu LIMIT (≈0,05 ms, față de
-- 7,6 ms cu JOIN + GROUP BY). Prețul nu e indexat, deci schimbarea lui rămâne HOT.
CREATE INDEX products_category_top_idx
    ON products (category_id, rating_avg DESC, review_count DESC, id);

-- Întreținerea incrementală a agregatelor la modificarea recenziilor.
CREATE FUNCTION maintain_rating() RETURNS trigger AS $$
BEGIN
    IF TG_OP IN ('DELETE', 'UPDATE') THEN
        UPDATE products
        SET rating_avg = CASE WHEN review_count <= 1 THEN 0
                              ELSE (rating_avg * review_count - OLD.rating) / (review_count - 1) END,
            review_count = review_count - 1
        WHERE id = OLD.product_id;
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        UPDATE products
        SET rating_avg = (rating_avg * review_count + NEW.rating) / (review_count + 1),
            review_count = review_count + 1
        WHERE id = NEW.product_id;
    END IF;
    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER reviews_rating
AFTER INSERT OR UPDATE OR DELETE ON reviews
FOR EACH ROW EXECUTE FUNCTION maintain_rating();

-- Invalidare distribuită: fiecare modificare a unui produs e anunțată pe
-- canalul `cache_invalidation`; instanțele aplicației ascultă (LISTEN) și își
-- golesc cache-ul local (L1). Funcționează și pentru scrieri făcute direct în DB.
--
-- NOTIFY ține un lock global până după flush-ul WAL la commit, serializând toate
-- scrierile. Aplicația setează `app.notify = off` (local tranzacției) când
-- backend-ul de cache nu are L1 și notificarea nu e necesară.
CREATE FUNCTION notify_product_change() RETURNS trigger AS $$
BEGIN
    IF current_setting('app.notify', true) IS DISTINCT FROM 'off' THEN
        PERFORM pg_notify('cache_invalidation', NEW.id || ':' || NEW.category_id);
    END IF;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER products_notify
AFTER UPDATE ON products
FOR EACH ROW EXECUTE FUNCTION notify_product_change();

-- VACUUM actualizează visibility map-ul, necesar pentru index-only scans.
VACUUM ANALYZE;
