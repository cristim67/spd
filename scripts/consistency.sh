#!/usr/bin/env bash
# Test de integrare: coerența cache-ului între instanțe, pentru fiecare
# backend × strategie. Scrie prețul prin app1, îl citește prin app2.
set -euo pipefail
BASE=${BASE:-http://localhost:8080}
fail=0
price_of() { curl -sf "$1" | sed -E 's/.*"price_cents":([0-9]+).*/\1/'; }
for backend in none local cluster tiered redis pgmem; do
  for strategy in invalidate write-through write-behind; do
    for app in app1 app2; do
      curl -sf -XPOST "$BASE/node/$app/admin/mode" -H 'content-type: application/json' \
        -d "{\"backend\":\"$backend\",\"strategy\":\"$strategy\"}" >/dev/null
    done
    id=$((RANDOM % 1000 + 1)); want=$((RANDOM * 3))
    price_of "$BASE/node/app2/api/products/$id" >/dev/null   # populează cache-ul app2
    curl -sf -XPUT "$BASE/node/app1/api/products/$id" -H 'content-type: application/json' \
      -d "{\"price_cents\":$want}" >/dev/null
    sleep 0.3   # flush write-behind + propagare NOTIFY
    got=$(price_of "$BASE/node/app2/api/products/$id")
    if [ "$got" = "$want" ]; then s=OK; else s="STALE (got $got)"; fail=1; fi
    printf '%-8s %-14s %s\n' "$backend" "$strategy" "$s"
  done
done
exit $fail
