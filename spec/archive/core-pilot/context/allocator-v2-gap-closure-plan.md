# Allocator v2 Gap Closure Plan

> **Archive status:** Completed historical checkpoint. Its v2/ring assumptions
> and verification claims are not current design authority.

## Summary

This file is the durable checklist for closing the current allocator-v2
specification gaps. The implementation target is to make the code match the
ring specs, with one intentional clarification: the materialized free-space
metadata chain root is canonical region `1`, and log prologue cursors name
positions within that chain.

## Checklist

- [x] Fix disk-format sequence policy.
  - Free-space metadata region headers use chain-local sequences
    `0..metadata_region_count - 1`.
  - Initial main-WAL region `0` uses `sequence = metadata_region_count`.
  - Runtime free-space metadata materialization writes strictly increasing
    chain-local sequences.
  - Startup rejects non-increasing free-space metadata chain sequences.
- [x] Implement transaction enrollment.
  - Full transaction begin appends `add_transaction_collection` as the first
    private transaction-log record.
  - Active transaction slots store the enrolled collection and observed
    committed generation.
  - Commit validates the current committed generation against enrollment and
    returns a transaction-conflict error on mismatch.
  - Transaction-log replay rejects unenrolled or conflicting collection records.
- [x] Add foreground inline transaction support.
  - Add a private main-WAL inline transaction helper that pre-reserves and
    writes bounded inline transaction ranges atomically.
  - Use it for short internal atomic groups when no full transaction is active.
  - Route the same work into the full transaction log when a full transaction is
    active.
- [x] Harden transaction-log retirement.
  - Retiring a pinned transaction-log range skips regions already present in
    the free-space collection.
  - The retained-log entry is removed only after every region is durably freed
    or already present.
  - Retirement remains blocked while any reachable transaction-control record
    references the range.
- [x] Clarify free-space metadata root and clean terminology.
  - Ring specs say the materialized free-space metadata root is canonical
    region `1`.
  - Remaining "free list" wording is replaced with "free-space collection,"
    except explicit obsolete/history text.
  - Prefer `MAIN_WAL_V2_FORMAT` and `LogRegionPrologue` in docs/tests, keeping
    compatibility aliases only where removing them would create broad churn.

## Required Tests

- [x] Initial free-space metadata sequences and main-WAL sequence.
- [x] Startup rejects a free-space metadata chain with non-increasing sequence
  headers.
- [x] Full transactions emit `add_transaction_collection`.
- [x] Commit conflict when the observed committed generation no longer matches.
- [x] Transaction-log replay rejects unenrolled collection mutations.
- [x] Foreground inline transaction commit and uncommitted crash behavior from
  normal storage paths.
- [x] Transaction-log retirement retry after a partial durable `free_region`.
- [x] Stale wording scan for obsolete allocator terms.

## Final Checks

- [x] `./tasks.sh`
- [x] `cargo check --lib --features embedded-storage`
- [x] `cargo check --lib --features file-backing`
- [x] `cargo check --bins`
- [x] `git diff --check -- src spec/ring spec/object-log.md journal.md allocator-v2-gap-closure-plan.md`
- [x] Searched `src`, `spec`, and `journal.md` for `free-list`, `free list`,
  `AllocBegin`, `FreePointerFooter`, and `free_list_head_after`.

## Assumptions

- Explicit public transactions remain single-collection and single-slot for now.
- No migration from old footer/free-list media is added.
- Compatibility aliases may remain only where removing them would create broad
  unrelated churn.
