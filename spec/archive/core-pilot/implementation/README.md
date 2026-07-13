# Archived Rust Kernel Pilot

Status: superseded implementation experiment; not compiled and not public API.

This directory preserves the Rust kernel spike that accompanied the archived
core specification. Immediately before archival, its 26 focused `kernel` tests
passed. That result demonstrates internal consistency with the pilot, not
conformance with the active design.

The snapshot is unsuitable as an active foundation because it uses an explicit
per-region ownership table and reservation tokens, exposes a raw device contract
that differs from the agreed logical `FlashIo` contract, permits ownership
transfers outside transactions, and lacks the required transaction-log,
recursive-growth, finish-lock, and WAL-lifecycle protocols.

Ideas worth reconsidering during later implementation work include:

- metadata-last formatting and its crash test;
- exact I/O tracing and torn-write fault injection;
- zero-I/O capacity rejection tests;
- checked monotonic free-queue positions; and
- validate-all-before-apply catalog transitions.

The `.rs` files are a historical snapshot only. They intentionally remain
outside `src/` and are not built by Cargo.
