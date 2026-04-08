#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

banned_runtime_dependencies=(
    tokio
    async-std
    smol
    glommio
    embassy-executor
    async-executor
    futures-executor
    futures-timer
)

banned_platform_dependencies=(
    embassy
    rtic
    freertos
    zephyr
    esp-idf
    esp_idf
    arduino
)

tree_output="$(mktemp)"
trap 'rm -f "$tree_output"' EXIT

echo "[verify] cargo test"
cargo test

echo "[verify] cargo clippy --all-targets --all-features -- -D warnings -A clippy::disallowed_methods -A clippy::disallowed_types -A clippy::disallowed_macros"
cargo clippy --all-targets --all-features -- \
    -D warnings \
    -A clippy::disallowed_methods \
    -A clippy::disallowed_types \
    -A clippy::disallowed_macros

echo "[verify] cargo clippy --lib --all-features -- -D warnings -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::todo -D clippy::unimplemented -D clippy::unreachable -D clippy::disallowed_methods -D clippy::disallowed_types -D clippy::disallowed_macros"
cargo clippy --lib --all-features -- \
    -D warnings \
    -D clippy::unwrap_used \
    -D clippy::expect_used \
    -D clippy::panic \
    -D clippy::todo \
    -D clippy::unimplemented \
    -D clippy::unreachable \
    -D clippy::disallowed_methods \
    -D clippy::disallowed_types \
    -D clippy::disallowed_macros

echo "[verify] cargo tree -e normal --prefix none"
cargo tree -e normal --prefix none > "$tree_output"

for dependency in "${banned_runtime_dependencies[@]}"; do
    if rg -q "^${dependency}( v|$)" "$tree_output"; then
        echo "[verify] unexpected runtime dependency: ${dependency}" >&2
        exit 1
    fi
done

for dependency in "${banned_platform_dependencies[@]}"; do
    if rg -q "^${dependency}( v|$)" "$tree_output"; then
        echo "[verify] unexpected framework or RTOS dependency: ${dependency}" >&2
        exit 1
    fi
done

echo "[verify] duvet report --ci true --require-citations true --require-tests true"
duvet report --ci true --require-citations true --require-tests true
