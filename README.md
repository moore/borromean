# Borromean

Borromean is an efficient multi-collection storage for flash devices. It works over a linear region of storage supporting both multiple types of collections as well as multiple instances of any type.

## Requirement Traceability

The storage spec in [spec/ring.md](spec/ring.md) now keeps normative requirements next to the motivating text. Each requirement uses a stable identifier such as `RING-WAL-ENC-001` so Duvet annotations can point at local spec text instead of a requirements appendix.

Example Rust annotation:

```rust
//= spec/ring.md#startup-replay-algorithm
//# RING-STARTUP-003 Select WAL tail as the unique candidate WAL region with the largest valid sequence.
```