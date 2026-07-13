# Archived Core Quint Model Pilot

Status: superseded verification experiment; not current design authority.

These models are retained with the archived core specification because they
capture useful properties and counterexamples. Their ownership table,
reservation tokens, and direct allocation protocol were rejected by the active
relational design. Passing these checks is historical evidence only and does
not establish conformance with the active core specification.

The ownership pilot uses three layers so the logical specification remains
independent of its storage protocol:

```text
region_ownership ──────┐
                       ├── ownership_refinement
storage_mechanical ────┘
```

`region_ownership.qnt` is the abstract state machine for the archived
[ownership chapter](../../../spec/archive/core-pilot/01-ownership.md). Its
public boundary is a pure ownership state,
event vocabulary, transition reducer, batch reducer, replay fold, and
invariant. The executable actions exercise that boundary with valid and invalid
bounded requests.

`storage_mechanical.qnt` is shared implementation vocabulary. It is mechanical
with respect to ownership and durability: issued work, successful sync,
runtime apply, physical content and erase state, crash outcomes, and replay are
separate. It also carries the corrected bounded transaction, collection,
free-queue, allocation-sequence, enrollment, and finish-lock state inherited
from the retained
[`transaction_free_recovery.qnt`](../../transaction_free_recovery.qnt).
Collection payload behavior is still abstract.

`ownership_refinement.qnt` is the only dependency between those layers. It
imports both models, converts durable mechanical events to abstract ownership
events, and checks forward refinement. A physical step may append one abstract
event batch or stutter. The durable record is the abstract transition point;
runtime apply therefore stutters while bringing memory up to the replayed
state.

The bridge also checks that quiescent runtime ownership equals durable replay,
that runtime may lag by exactly the synced pending batch, and that rejected
mechanical ownership requests produce the corresponding abstract error without
changing ownership. Focused step sets cover reservation/publication,
release/reuse, transactions, and crash/replay before the full composed step set
is checked.

The intentionally unsafe mechanical actions are excluded from the production
`step`. The verification script runs each directly and requires the safety
invariant to fail, demonstrating that the checks reject publication without
durable content, readiness without erase, reuse of a published region, and a
mismatched publication token.

The original [`transaction_free_recovery.qnt`](../../transaction_free_recovery.qnt)
remains unchanged as a regression oracle. `legacy_recovery_bounded.qnt` wraps
that exact init, step, and safety relation with eight actions of model fuel for
TLC.
The ownership abstraction, mechanical protocol, and refinement bridge are
checked with Quint's TLC backend. Apalache 0.56.1 remains the verifier for the
smaller focused core models; its Z3 translation currently returns `UNKNOWN` or
fails to complete for the richer ownership protocol and legacy recovery model
instead of producing a proof or counterexample. The verification script treats
that result as unsupported, not success.

Quint 0.32.0 does not forward `--max-steps` to its TLC backend, so the ownership
models enforce their bounds in the transition systems themselves. The abstract
model explores eight actions, the mechanical composition explores six, and the
bridge explores reservation/publication, release/reuse, transaction,
crash/replay, and full-composition prefixes of 13, 8, 21, 10, and 6 actions.
These depths reach paired publication, release through durable readiness, and
transaction decision through ordered cleanup and finish. `tlc-config.json`
limits each sequential TLC process to two workers and a 4 GiB heap; the runner
also applies a per-check timeout. A printed bound is therefore an actual model
bound, not an ignored checker option.

Reproduce the archived checks from the repository root with:

```sh
npm ci --prefix models/archive/core-pilot
npm run verify-models --prefix models/archive/core-pilot
```
