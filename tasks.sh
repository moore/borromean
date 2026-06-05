#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

MEMORY_PROFILE_CONFIGS=(
    perf/profile_memory_insert.toml
    perf/profile_memory_update_hot.toml
    perf/profile_memory_read_hits.toml
    perf/profile_memory_read_misses.toml
    perf/profile_memory_mixed_update.toml
)

PERF_MATRIX_CONFIGS=(
    perf/file_backing.toml
    perf/file_backing_4k.toml
    perf/file_backing_update_hot.toml
    perf/file_backing_update_hot_4k.toml
    perf/file_backing_read_hits.toml
    perf/file_backing_read_hits_4k.toml
    perf/file_backing_read_misses.toml
    perf/file_backing_read_misses_4k.toml
    perf/file_backing_mixed_update.toml
    perf/file_backing_mixed_update_4k.toml
)

run_verify() {
    echo "==> ./scripts/verify.sh"
    ./scripts/verify.sh
}

run_test() {
    echo "==> cargo test"
    cargo test
}

run_clippy() {
    echo "==> cargo clippy --all-targets --all-features -- -D warnings -A clippy::disallowed_methods -A clippy::disallowed_types -A clippy::disallowed_macros"
    cargo clippy --all-targets --all-features -- \
        -D warnings \
        -A clippy::disallowed_methods \
        -A clippy::disallowed_types \
        -A clippy::disallowed_macros
    echo "==> cargo clippy --lib --all-features -- -D warnings -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::todo -D clippy::unimplemented -D clippy::unreachable -D clippy::disallowed_methods -D clippy::disallowed_types -D clippy::disallowed_macros"
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

run_trace_review() {
    echo "==> python3 scripts/trace_review.py init"
    python3 scripts/trace_review.py init
}

run_trace_review_summary() {
    echo "==> python3 scripts/trace_review.py summarize"
    python3 scripts/trace_review.py summarize
}

run_mutants() {
    echo "==> cargo run --quiet --bin traceability_audit -- check-requirements"
    cargo run --quiet --bin traceability_audit -- check-requirements
    echo "==> cargo mutants"
    cargo mutants
}

run_perf() {
    local config_path="${BORROMEAN_PERF_CONFIG:-perf/file_backing.toml}"
    echo "==> cargo run --release --features perf-tools --bin file_backing_perf -- --config ${config_path}"
    cargo run --release --features perf-tools --bin file_backing_perf -- --config "$config_path"
}

run_perf_matrix() {
    local config_path
    for config_path in "${PERF_MATRIX_CONFIGS[@]}"; do
        echo "==> cargo run --release --features perf-tools --bin file_backing_perf -- --config ${config_path}"
        cargo run --release --features perf-tools --bin file_backing_perf -- --config "$config_path"
    done
    run_perf_matrix_summary
}

require_perf() {
    if ! command -v perf >/dev/null 2>&1; then
        echo "perf is required for profiling but was not found on PATH" >&2
        return 127
    fi
}

build_perf_profile_binary() {
    echo "==> RUSTFLAGS=\"${RUSTFLAGS:-} -C force-frame-pointers=yes\" cargo build --release --features perf-tools --bin file_backing_perf"
    RUSTFLAGS="${RUSTFLAGS:-} -C force-frame-pointers=yes" \
        cargo build --release --features perf-tools --bin file_backing_perf
}

run_perf_profile_artifacts() {
    local label="$1"
    local config_path="$2"
    local profile_dir="${BORROMEAN_PERF_PROFILE_DIR:-target/perf/profiles}"
    local frequency="${BORROMEAN_PERF_PROFILE_FREQ:-997}"
    local base_name
    local output_prefix
    base_name="$(basename "$config_path" .toml)"
    output_prefix="${profile_dir}/${label}-${base_name}"

    mkdir -p "$profile_dir"

    echo "==> perf stat -d -o ${output_prefix}.stat.txt -- target/release/file_backing_perf --config ${config_path}"
    perf stat -d \
        -o "${output_prefix}.stat.txt" \
        -- target/release/file_backing_perf --config "$config_path"

    echo "==> perf record -F ${frequency} -g --call-graph fp -o ${output_prefix}.perf.data -- target/release/file_backing_perf --config ${config_path}"
    perf record -F "$frequency" -g --call-graph fp \
        -o "${output_prefix}.perf.data" \
        -- target/release/file_backing_perf --config "$config_path"

    echo "==> perf report --stdio -i ${output_prefix}.perf.data > ${output_prefix}.perf.txt"
    perf report --stdio -i "${output_prefix}.perf.data" > "${output_prefix}.perf.txt"
    echo "profile artifacts: ${output_prefix}.stat.txt ${output_prefix}.perf.data ${output_prefix}.perf.txt"
}

run_perf_profile_for_config() {
    local label="$1"
    local config_path="$2"

    require_perf
    build_perf_profile_binary
    run_perf_profile_artifacts "$label" "$config_path"
}

run_perf_profile() {
    local config_path="${BORROMEAN_PERF_PROFILE_CONFIG:-perf/file_backing_smoke.toml}"
    run_perf_profile_for_config "file" "$config_path"
}

run_perf_profile_memory() {
    local config_path="${BORROMEAN_PERF_MEMORY_PROFILE_CONFIG:-perf/profile_memory_update_hot.toml}"
    run_perf_profile_for_config "memory" "$config_path"
}

run_perf_profile_memory_matrix() {
    local config_path
    local json_paths=()

    require_perf
    build_perf_profile_binary
    for config_path in "${MEMORY_PROFILE_CONFIGS[@]}"; do
        run_perf_profile_artifacts "memory" "$config_path"
        json_paths+=("target/perf/$(basename "$config_path" .toml).json")
    done
    summarize_memory_profile_jsons "${json_paths[@]}"
}

run_perf_profile_memory_summary() {
    local config_path
    local json_paths=()

    for config_path in "${MEMORY_PROFILE_CONFIGS[@]}"; do
        json_paths+=("target/perf/$(basename "$config_path" .toml).json")
    done
    summarize_memory_profile_jsons "${json_paths[@]}"
}

summarize_memory_profile_jsons() {
    python3 scripts/summarize_memory_profile_jsons.py "$@"
}

perf_matrix_json_paths() {
    local config_path
    for config_path in "${PERF_MATRIX_CONFIGS[@]}"; do
        printf 'target/perf/%s.json\n' "$(basename "$config_path" .toml)"
    done
}

summarize_perf_matrix_jsons() {
    python3 scripts/summarize_perf_matrix_jsons.py "$@"
}

run_perf_matrix_summary() {
    local json_paths=()
    local json_path

    while IFS= read -r json_path; do
        json_paths+=("$json_path")
    done < <(perf_matrix_json_paths)

    summarize_perf_matrix_jsons \
        --output target/perf/perf_matrix_summary.md \
        "${json_paths[@]}"
}

run_perf_calibrate() {
    local configs=()

    if [ -n "${BORROMEAN_PERF_CALIBRATION_CONFIGS:-}" ]; then
        read -r -a configs <<<"$BORROMEAN_PERF_CALIBRATION_CONFIGS"
    else
        configs=("${PERF_MATRIX_CONFIGS[@]}")
    fi

    echo "==> cargo build --release --features perf-tools --bin file_backing_perf"
    cargo build --release --features perf-tools --bin file_backing_perf
    echo "==> python3 scripts/calibrate_perf_matrix.py ${configs[*]}"
    python3 scripts/calibrate_perf_matrix.py \
        --binary target/release/file_backing_perf \
        --repeats "${BORROMEAN_PERF_CALIBRATION_REPEATS:-3}" \
        --read-counts "${BORROMEAN_PERF_CALIBRATION_READ_COUNTS:-3000,10000,30000,100000,300000}" \
        --write-counts "${BORROMEAN_PERF_CALIBRATION_WRITE_COUNTS:-3000,10000}" \
        --mixed-counts "${BORROMEAN_PERF_CALIBRATION_MIXED_COUNTS:-3000,10000}" \
        "${configs[@]}"
}

run_perf_calibrate_summary() {
    python3 scripts/calibrate_perf_matrix.py --summarize-only
}

run_bench() {
    echo "==> cargo bench --features file-backing --bench file_backing_mmap"
    cargo bench --features file-backing --bench file_backing_mmap
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
  trace-review
           Generate fresh per-test semantic review packets under target/trace-review
  trace-review-summary
           Validate reviewer result JSON files and summarize semantic review findings
  mutants  Manually run cargo-mutants after validating annotations
  perf     Run the FileBacking perf runner (override config with BORROMEAN_PERF_CONFIG)
  perf-test
           Alias for perf
  perf-matrix
           Run 1 MiB and 4 KiB insert, update, read-hit, read-miss, and mixed comparison configs, then summarize them
  perf-matrix-summary
           Summarize generated perf matrix JSON reports as Markdown
  perf-calibrate
           Run repeated perf configs at increasing operation counts and recommend stable run sizes
  perf-calibrate-summary
           Summarize existing calibration JSON reports
  perf-profile
           Profile a release perf run with frame pointers (override config with BORROMEAN_PERF_PROFILE_CONFIG)
  perf-profile-memory
           Profile a memory-backed Borromean perf run (override config with BORROMEAN_PERF_MEMORY_PROFILE_CONFIG)
  perf-profile-memory-matrix
           Profile insert, update, read-hit, read-miss, and mixed memory-backed configs
  perf-profile-memory-summary
           Summarize generated memory profile JSON reports
  bench    Run Criterion benchmarks for the FileBacking mmap backend
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
        trace-review)
            run_trace_review
            ;;
        trace-review-summary)
            run_trace_review_summary
            ;;
        mutants)
            run_mutants
            ;;
        perf|perf-test|perf-tests)
            run_perf
            ;;
        perf-matrix)
            run_perf_matrix
            ;;
        perf-matrix-summary)
            run_perf_matrix_summary
            ;;
        perf-calibrate)
            run_perf_calibrate
            ;;
        perf-calibrate-summary)
            run_perf_calibrate_summary
            ;;
        perf-profile)
            run_perf_profile
            ;;
        perf-profile-memory)
            run_perf_profile_memory
            ;;
        perf-profile-memory-matrix)
            run_perf_profile_memory_matrix
            ;;
        perf-profile-memory-summary)
            run_perf_profile_memory_summary
            ;;
        bench)
            run_bench
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
