#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

run_verify() {
    echo "==> ./scripts/verify.sh"
    ./scripts/verify.sh
}

run_test() {
    echo "==> cargo test"
    cargo test
}

run_clippy() {
    echo "==> cargo clippy --all-targets --all-features -- -D warnings"
    cargo clippy --all-targets --all-features -- -D warnings
}

run_cargo_format() {
    echo "==> cargo fmt --all"
    cargo fmt --all
}

run_markdown_format() {
    echo "==> rumdl fmt ."
    rumdl fmt . --respect-gitignore
}

run_format() {
    run_cargo_format
    run_markdown_format
}

run_duvet() {
    echo "==> cargo run --quiet --bin traceability_audit -- check-requirements"
    cargo run --quiet --bin traceability_audit -- check-requirements
    echo "==> duvet report --config-path .duvet/config.toml --require-tests true"
    duvet report --config-path .duvet/config.toml --require-tests true
}

run_mutants() {
    echo "==> cargo run --quiet --bin traceability_audit -- check-requirements"
    cargo run --quiet --bin traceability_audit -- check-requirements
    echo "==> cargo mutants"
    cargo mutants
}

usage() {
    cat <<'USAGE'
Usage: ./tasks.sh [task...]

Tasks:
  all      Run the full repository verification lane
  verify   Run the full repository verification lane
  fmt      Run Rust and markdown formatting
  test     Run cargo test
  clippy   Run cargo clippy
  md       Run markdown formatting
  duvet    Validate traceability annotations and generate the Duvet report
  mutants  Manually run cargo-mutants after validating annotations
USAGE
}

run_task() {
    case "$1" in
        all|verify)
            run_verify
            ;;
        fmt)
            run_format
            ;;
        test)
            run_test
            ;;
        clippy)
            run_clippy
            ;;
        md)
            run_markdown_format
            ;;
        duvet)
            run_duvet
            ;;
        mutants)
            run_mutants
            ;;
        -h|--help|help)
            usage
            ;;
        *)
            echo "unknown task: $1" >&2
            usage >&2
            return 2
            ;;
    esac
}

cd "$ROOT_DIR"

if [ "$#" -eq 0 ]; then
    run_task all
    exit 0
fi

for task in "$@"; do
    run_task "$task"
done
