#!/usr/bin/env bash
# Rulează toate experimentele și scrie CSV-urile în results/.
# Necesită stack-ul pornit: docker compose up -d --build
#
#   scripts/bench.sh            # toate experimentele
#   scripts/bench.sh e1 e4      # doar unele
#   DUR=30 WARM=10 scripts/bench.sh
set -euo pipefail
cd "$(dirname "$0")/.."

DUR=${DUR:-15}
WARM=${WARM:-5}
OUT=results/http.csv
LG=(docker compose --profile bench run --rm -T loadgen)

http() { "${LG[@]}" http --duration "$DUR" --warmup "$WARM" --out "$OUT" "$@"; }
mode() { printf '{"backend":"%s","strategy":"%s","capacity":%s,"nodes":%s,"coalesce":%s,"policy":"lru","ttl_secs":600}' \
  "$1" "${2:-invalidate}" "${3:-200000}" "${4:-3}" "${5:-true}"; }

# E1 — comparația backend-urilor (citiri 95%, Zipf 0.99, 64 clienți).
e1() { for b in none local cluster tiered redis pgmem; do
  http --experiment e1 --label "$b" --mode "$(mode "$b")"; done; }

# E2 — efectul asimetriei Zipf, cache limitat la 10% din chei (doar citiri).
e2() { for s in 0.6 0.8 0.99 1.2; do
  http --experiment e2 --label "$s" --zipf "$s" --read-ratio 1 --mode "$(mode none)"
  http --experiment e2 --label "$s" --zipf "$s" --read-ratio 1 --mode "$(mode local invalidate 10000)"
  http --experiment e2 --label "$s" --zipf "$s" --read-ratio 1 --mode "$(mode cluster invalidate 3334)"
done; }

# E3 — strategii de scriere în funcție de proporția de scrieri (backend cluster).
e3() { for rr in 0.5 0.8 0.95; do for st in invalidate write-through write-behind; do
  http --experiment e3 --label "$rr" --read-ratio "$rr" --mode "$(mode cluster "$st")"; done; done; }

# E4 — scalabilitate cu numărul de clienți concurenți (1 = client secvențial).
e4() { for c in 1 4 16 64 128 256; do for b in none local cluster; do
  http --experiment e4 --label "$c" --concurrency "$c" --mode "$(mode "$b")"; done; done; }

# E5 — scale-out: 1 vs 2 instanțe, ambele prin nginx (/node/app1 = doar app1).
e5() { for b in none cluster; do
  http --experiment e5 --label "1" --concurrency 128 --url http://nginx/node/app1 --admin http://app1:8080 --mode "$(mode "$b")"
  http --experiment e5 --label "2" --concurrency 128 --mode "$(mode "$b")"
done; }

# E6 — memorie agregată: 1..3 noduri de cluster, 10.000 intrări pe nod (doar citiri).
e6() { for n in 1 2 3; do
  http --experiment e6 --label "$n" --read-ratio 1 --mode "$(mode cluster invalidate 10000 "$n")"; done; }

# E7 — cache stampede: pornire la rece, cheie foarte populară, cu/fără single-flight.
e7() { for c in true false; do
  "${LG[@]}" http --duration 10 --warmup 0 --out "$OUT" --experiment e7 --label "$c" \
    --zipf 1.4 --concurrency 128 --mode "$(mode local invalidate 200000 3 "$c")"; done; }

# E8 — eficiență: debit fix (open-loop, 8.000 cereri/s) și CPU consumat per container.
e8() { : > results/e8_cpu.csv.tmp; for b in none local cluster tiered redis pgmem; do
  http --experiment e8 --label "$b" --rate 8000 --concurrency 256 --mode "$(mode "$b")" &
  sleep $((WARM + DUR / 2 + 3))
  docker stats --no-stream --format '{{.Name}},{{.CPUPerc}}' | sed -E "s/^spd-//; s/-1,/,/; s/-run-[a-z0-9]+,/,/; s/%//; s/^/$b,/" >> results/e8_cpu.csv.tmp
  wait; done
  { echo "backend,container,cpu"; cat results/e8_cpu.csv.tmp; } > results/e8_cpu.csv; rm results/e8_cpu.csv.tmp; }

# Benchmark-uri offline (nu necesită rețea): politici de evacuare, scalare motor.
sim()    { "${LG[@]}" simulate --out results/sim_hit_ratio.csv; }
engine() { rm -f results/engine_scaling.csv
  "${LG[@]}" engine --threads 1,2,4,5,8 --shards 1,4,16,64 --out results/engine_scaling.csv
  "${LG[@]}" engine --threads 1,2,4,5,8 --shards 64 --policy clock --out results/engine_scaling.csv
  "${LG[@]}" engine --threads 1,2,4,5,8 --shards 64 --zipf 0 --out results/engine_scaling.csv; }

ALL=(e1 e2 e3 e4 e5 e6 e7 e8 sim engine)
STEPS=("${@:-${ALL[@]}}")
mkdir -p results
if [ $# -eq 0 ]; then rm -f results/*.csv; fi
for s in "${STEPS[@]}"; do echo "=== $s ==="; "$s"; done
echo "Rezultate în results/: $(ls results)"
