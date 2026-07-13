#!/usr/bin/env bash
set -euo pipefail

archive_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${archive_root}/../../.." && pwd)"
cd "$repo_root"

if [[ -n "${QUINT_BIN:-}" ]]; then
    quint_bin="$QUINT_BIN"
elif [[ -x "${archive_root}/node_modules/.bin/quint" ]]; then
    quint_bin="${archive_root}/node_modules/.bin/quint"
else
    quint_bin="quint"
fi
expected_version="0.32.0"
actual_version="$($quint_bin --version)"
if [[ "$actual_version" != "$expected_version" ]]; then
    echo "expected Quint ${expected_version}, found ${actual_version}" >&2
    exit 1
fi

verify_timeout="${MODEL_VERIFY_TIMEOUT:-600}"
tlc_config="${archive_root}/tlc-config.json"

typecheck_model() {
    local model="$1"
    echo "[models] typecheck ${model}"
    "$quint_bin" typecheck "$model"
}

verify_model() {
    local model="$1"
    local step="$2"
    local invariant="$3"
    local max_steps="$4"
    local backend="${5:-apalache}"
    if [[ "$backend" == "tlc" ]]; then
        echo "[models] verify ${model}:${step}:${invariant} (${max_steps}-action model bound, ${backend})"
    else
        echo "[models] verify ${model}:${step}:${invariant} (${max_steps} steps, ${backend})"
    fi
    if [[ "$backend" == "tlc" ]]; then
        timeout --foreground "$verify_timeout" "$quint_bin" verify "$model" \
            --backend tlc \
            --step "$step" \
            --invariant "$invariant" \
            --tlc-config "$tlc_config" \
            --verbosity 0
    else
        timeout --foreground "$verify_timeout" "$quint_bin" verify "$model" \
            --apalache-version 0.56.1 \
            --step "$step" \
            --invariant "$invariant" \
            --max-steps "$max_steps" \
            --verbosity 0
    fi
}

expect_unsafe_step() {
    local model="$1"
    local step="$2"
    local invariant="$3"
    echo "[models] expect ${model}:${step} to violate ${invariant}"
    local output
    local status
    set +e
    output="$(
        timeout --foreground "$verify_timeout" "$quint_bin" run "$model" \
            --step "$step" \
            --invariant "$invariant" \
            --max-steps 1 \
            --max-samples 1 \
            --verbosity 1 2>&1
    )"
    status=$?
    set -e
    if [[ "$status" -eq 0 ]]; then
        echo "unsafe comparison ${step} unexpectedly satisfied ${invariant}" >&2
        exit 1
    fi
    if [[ "$status" -eq 124 ]]; then
        echo "unsafe comparison ${step} timed out" >&2
        exit 1
    fi
    if [[ "$output" != *"error: Invariant violated"* ]]; then
        echo "$output" >&2
        echo "unsafe comparison ${step} failed for a reason other than the expected invariant violation" >&2
        exit 1
    fi
    echo "[models] observed expected invariant violation for ${step}"
}

simple_models=(
    "fifo_wear"
    "free_basis_publication"
    "wal_rotation"
    "transaction_atomicity"
    "transaction_views"
    "writer_exclusion"
    "ordered_cleanup"
    "reclaim_reuse"
    "capacity_preflight"
    "core_composition"
)

for module in "${simple_models[@]}"; do
    model="${archive_root}/${module}.qnt"
    typecheck_model "$model"
    verify_model "$model" step safety 8
done

abstract_model="${archive_root}/region_ownership.qnt"
mechanical_model="${archive_root}/storage_mechanical.qnt"
refinement_model="${archive_root}/ownership_refinement.qnt"
legacy_recovery_model="models/transaction_free_recovery.qnt"
legacy_recovery_wrapper="${archive_root}/legacy_recovery_bounded.qnt"

typecheck_model "$abstract_model"
verify_model "$abstract_model" step safety 8 tlc

typecheck_model "$mechanical_model"
verify_model "$mechanical_model" step safety 6 tlc

typecheck_model "$refinement_model"
verify_model "$refinement_model" reservationPublicationStep safety 13 tlc
verify_model "$refinement_model" releaseReuseStep safety 8 tlc
verify_model "$refinement_model" transactionStep safety 21 tlc
verify_model "$refinement_model" crashReplayStep safety 10 tlc
verify_model "$refinement_model" step safety 6 tlc

typecheck_model "$legacy_recovery_model"
typecheck_model "$legacy_recovery_wrapper"
verify_model "$legacy_recovery_wrapper" step safety 8 tlc

expect_unsafe_step "$mechanical_model" UnsafePublishWithoutContent safety
expect_unsafe_step "$mechanical_model" UnsafeReadinessWithoutErase safety
expect_unsafe_step "$mechanical_model" UnsafeReusePublished safety
expect_unsafe_step "$mechanical_model" UnsafeMismatchedTokenPublication safety
