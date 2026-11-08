#!/usr/bin/env bash
# Rulare scurtă + consum CPU per container (pentru iterații de optimizare).
#   scripts/profile.sh <eticheta> [backend] [read_ratio]
set -euo pipefail
cd "$(dirname "$0")/.."
label=$1; backend=${2:-local}; rr=${3:-0.95}
docker compose --profile bench run --rm -T loadgen http --duration 15 --warmup 3 \
  --experiment opt --label "$label" --read-ratio "$rr" --out results/opt/opt.csv \
  --mode "{\"backend\":\"$backend\"}" 2>&1 | grep -E "rps=" &
sleep 11
docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' | sed 's/spd-//; s/-1 / /' | sort -k2 -rn | head -8
wait
