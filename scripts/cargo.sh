#!/usr/bin/env bash
# Rulează cargo într-un container (nu necesită Rust instalat local).
# Exemplu: scripts/cargo.sh test --workspace
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=spd-rust-dev
docker image inspect "$IMG" >/dev/null 2>&1 || docker build -q -t "$IMG" -f deploy/Dockerfile.dev deploy >/dev/null
TTY=$([ -t 1 ] && echo "-t" || true)
exec docker run --rm $TTY \
  -v "$PWD":/src -w /src \
  -v spd-cargo-registry:/usr/local/cargo/registry \
  -v spd-cargo-target:/src/target \
  "$IMG" cargo "$@"
