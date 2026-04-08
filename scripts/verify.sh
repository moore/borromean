#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

no_std_target="${BORROMEAN_NO_STD_TARGET:-riscv32imac-unknown-none-elf}"

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
requirements_backup="$(mktemp -d)"
requirements_root=".duvet/requirements"
policy_requirements_root=".duvet/policy/requirements"
policy_requirements_active=0

restore_duvet_requirements() {
    if [[ "$policy_requirements_active" -eq 1 && -d "$requirements_backup/original" ]]; then
        rm -rf "$requirements_root"
        mv "$requirements_backup/original" "$requirements_root"
        policy_requirements_active=0
    fi
}

trap 'restore_duvet_requirements; rm -f "$tree_output"; rm -rf "$requirements_backup"' EXIT

echo "[verify] cargo test"
cargo test

echo "[verify] cargo build --lib --target ${no_std_target}"
cargo build --lib --target "$no_std_target"

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

echo "[verify] cargo run --quiet --bin traceability_audit"
cargo run --quiet --bin traceability_audit

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

echo "[verify] duvet report --config-path .duvet/config.toml --ci true --require-citations false --require-tests true"
duvet report --config-path .duvet/config.toml --ci true --require-citations false --require-tests true

echo "[verify] duvet report --config-path .duvet/policy/config.toml --ci true --require-citations true --require-tests false"
mv "$requirements_root" "$requirements_backup/original"
cp -R "$policy_requirements_root" "$requirements_root"
policy_requirements_active=1
duvet report --config-path .duvet/policy/config.toml --ci true --require-citations true --require-tests false
restore_duvet_requirements
