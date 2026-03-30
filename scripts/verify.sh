#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[verify] cargo test"
cargo test

echo "[verify] cargo clippy --all-targets --all-features -- -D warnings"
cargo clippy --all-targets --all-features -- -D warnings

echo "[verify] duvet report --ci true --require-citations true --require-tests true"
duvet report --ci true --require-citations true --require-tests true
