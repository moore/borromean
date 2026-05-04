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
  test     Run cargo test
  clippy   Run cargo clippy
  duvet    Validate traceability annotations and generate the Duvet report
  mutants  Run requirement-filtered cargo-mutants after validating annotations
USAGE
}

run_task() {
    case "$1" in
        all|verify)
            run_verify
            ;;
        test)
            run_test
            ;;
        clippy)
            run_clippy
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
